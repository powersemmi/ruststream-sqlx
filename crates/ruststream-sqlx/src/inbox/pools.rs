//! The pool a handler queries through: the service's pool on the runtime the broker connected on,
//! and a small pool of its own on each dedicated thread of a `threads(n)` subscription.
//!
//! A connection registers its socket with the I/O driver of the runtime that opens it. A dedicated
//! thread's runtime ends with its subscription, so a connection the service's pool opened there
//! would stay in that pool after the runtime is gone and hang the next query that got it. A
//! thread's own pool opens its connections on the thread, lends them only there, and is dropped
//! with the thread.

use std::any::Any;
use std::cell::RefCell;
use std::fmt;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::ptr;

use sqlx::{Database, Pool};
use tokio::runtime::Handle;

/// A subscription's handle on the service's pool, leaked once when it opens, with what a
/// dedicated thread builds its own pool from.
///
/// It dereferences to the service's pool: claims, settlements, transactions and the broker's own
/// publishes take their connections there.
pub(crate) struct ServicePool<DB: Database> {
    pool: Pool<DB>,
    /// The runtime the broker connected on: a handler there queries through the service's pool.
    home: Handle,
    /// The connections a dedicated thread's pool opens at most.
    per_thread: NonZeroU32,
}

/// The pool of a subscription's handlers on this thread: `None` on the runtime the broker
/// connected on, where they take the service's pool.
struct HandlerPool {
    /// The address of the subscription's [`ServicePool`], which each opening of a subscription
    /// leaks anew, so an entry never outlives what it names.
    owner: usize,
    /// The thread's own `Pool<DB>`. The thread-local cannot name `DB`, so it holds the pool behind
    /// `Any`, read back with one type check per read.
    own: Option<Box<dyn Any>>,
}

thread_local! {
    /// The handler pools of the subscriptions that ran a handler on this thread. A dedicated
    /// thread serves one subscription and holds one entry; its pool drops with the thread, after
    /// the thread's runtime, and closes the connections it opened there.
    static HANDLER_POOLS: RefCell<Vec<HandlerPool>> = const { RefCell::new(Vec::new()) };
}

impl<DB: Database> ServicePool<DB> {
    /// The handle of a subscription on `pool`, whose broker connected on `home`, for the life of
    /// the process; a dedicated thread's pool opens `per_thread` connections at most.
    pub(crate) fn leak(pool: Pool<DB>, home: Handle, per_thread: NonZeroU32) -> &'static Self {
        Box::leak(Box::new(Self {
            pool,
            home,
            per_thread,
        }))
    }

    /// The pool a handler on the calling thread queries through: the service's pool on the
    /// runtime the broker connected on, and on a dedicated thread the thread's own, built on the
    /// first read there.
    ///
    /// One thread-local access and one clone of a pool handle per read, as the service's pool
    /// costs; on a dedicated thread one type check besides.
    pub(crate) fn for_handler(&'static self) -> Pool<DB> {
        let owner = ptr::from_ref(self).addr();
        HANDLER_POOLS.with_borrow_mut(|pools| {
            let index = pools
                .iter()
                .position(|entry| entry.owner == owner)
                .unwrap_or_else(|| {
                    let own = self
                        .thread_pool()
                        .map(|pool| Box::new(pool) as Box<dyn Any>);
                    pools.push(HandlerPool { owner, own });
                    pools.len() - 1
                });
            pools[index]
                .own
                .as_ref()
                .and_then(|own| own.downcast_ref::<Pool<DB>>())
                .unwrap_or(&self.pool)
                .clone()
        })
    }

    /// A pool of the calling thread's own, or `None` on the runtime the broker connected on: the
    /// service's pool options and connect options, at most `per_thread` connections, opened on
    /// the thread's runtime when a handler first needs one.
    fn thread_pool(&self) -> Option<Pool<DB>> {
        // Why at run time: the core hands a subscription no word of where its handlers run, so a
        // handler's first read on a thread asks the runtime it polls on, once per thread.
        let current = Handle::try_current().ok()?;
        if current.id() == self.home.id() {
            return None;
        }
        let options = self
            .pool
            .options()
            .clone()
            .min_connections(0)
            .max_connections(self.per_thread.get());
        Some(options.connect_lazy_with(self.pool.connect_options().as_ref().clone()))
    }
}

impl<DB: Database> Deref for ServicePool<DB> {
    type Target = Pool<DB>;

    fn deref(&self) -> &Pool<DB> {
        &self.pool
    }
}

impl<DB: Database> fmt::Debug for ServicePool<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServicePool")
            .field("per_thread", &self.per_thread)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::thread;

    use sqlx::Sqlite;
    use sqlx::sqlite::SqlitePoolOptions;
    use tokio::runtime::Builder;

    use super::*;

    fn service_pool(per_thread: u32) -> &'static ServicePool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_lazy("sqlite::memory:")
            .expect("the URL parses");
        let per_thread = NonZeroU32::new(per_thread).expect("positive");
        ServicePool::leak(pool, Handle::current(), per_thread)
    }

    /// What a handler on a thread of its own, on a runtime of its own, reads: the most
    /// connections its pool opens, and the size a second read sees while the first holds one.
    fn read_on_a_thread(service: &'static ServicePool<Sqlite>) -> (u32, u32) {
        thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("builds");
            runtime.block_on(async move {
                let first = service.for_handler();
                let _held = first.acquire().await.expect("a connection");
                let again = service.for_handler();
                (first.options().get_max_connections(), again.size())
            })
        })
        .join()
        .expect("the thread ends")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_app_runtime_reads_the_service_pool() {
        let service = service_pool(1);
        let _held = service.acquire().await.expect("a connection");
        let read = service.for_handler();
        assert_eq!((read.options().get_max_connections(), read.size()), (4, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dedicated_thread_reads_a_pool_of_its_own_of_the_set_size() {
        let service = service_pool(2);
        let _held = service.acquire().await.expect("a connection");
        // The thread's pool opened one connection of its two; the service's holds the test's.
        assert_eq!(read_on_a_thread(service), (2, 1));
        assert_eq!(
            service.size(),
            1,
            "the thread opened nothing in the service's pool"
        );
    }
}
