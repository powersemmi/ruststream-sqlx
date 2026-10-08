//! The runtime the crate's connections belong to.
//!
//! A connection a pool opens registers its socket with the I/O driver of the runtime that polls
//! the open. A subscription on `threads(n)` settles and publishes on a dedicated thread's runtime,
//! which ends with the subscription: a connection opened there would die with it while the pool
//! kept handing it out. Where a settlement or a publish takes a connection of the pool, the crate
//! takes it as if on the runtime the service started on.

use std::future::{Future, poll_fn};
use std::pin::pin;

use sqlx::pool::PoolConnection;
use sqlx::{Database, Error, Pool};
use tokio::runtime::Handle;

/// A connection of `pool`, taken inside `home`'s context: a connection the pool opens for it,
/// and the timers the pool arms, belong to `home`, whichever runtime awaits the call.
///
/// Each poll enters `home`, one reference-count increment and decrement on its handle.
pub(crate) async fn acquire<DB: Database>(
    pool: &Pool<DB>,
    home: &Handle,
) -> Result<PoolConnection<DB>, Error> {
    on(home, pool.acquire()).await
}

/// Polls `work` inside `home`'s context: the sockets and timers it creates register with `home`'s
/// drivers, and what it spawns runs there, while its waker stays the caller's.
async fn on<Work: Future>(home: &Handle, work: Work) -> Work::Output {
    let mut work = pin!(work);
    poll_fn(|cx| {
        let _entered = home.enter();
        work.as_mut().poll(cx)
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::thread;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::runtime::Builder;

    use super::*;

    /// A socket opened on a runtime that then ends, used on the home runtime.
    async fn opened_on_a_passing_runtime(home: Option<Handle>) -> Result<(), String> {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let address = listener.local_addr().expect("an address");
        let opened = thread::spawn(move || {
            let passing = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("builds");
            let stream = passing.block_on(async move {
                let connect = TcpStream::connect(address);
                match home {
                    Some(home) => on(&home, connect).await,
                    None => connect.await,
                }
            });
            drop(passing);
            stream
        })
        .join()
        .expect("the thread ends")
        .expect("connects");
        let (mut accepted, _) = listener.accept().await.expect("accepts");
        accepted.write_all(b"ok").await.expect("writes");
        let mut stream = opened;
        let mut read = [0; 2];
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            stream.read_exact(&mut read),
        )
        .await;
        match answered {
            Ok(Ok(_)) if &read == b"ok" => Ok(()),
            Ok(Ok(_)) => Err(format!("read {read:?}")),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("the read hangs".to_owned()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_socket_opened_on_home_outlives_the_runtime_that_polled_the_open() {
        let home = Handle::current();
        assert_eq!(opened_on_a_passing_runtime(Some(home)).await, Ok(()));
    }

    // The case `on` exists for: without it, the socket dies with the runtime that opened it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_socket_opened_elsewhere_dies_with_its_runtime() {
        assert!(opened_on_a_passing_runtime(None).await.is_err());
    }
}
