//! The pool bound of a subscription on dedicated threads: the threads' rings hold deliveries
//! beside the handlers, and in every form whose delivery holds a connection the subscription
//! still leaves the pool its last one.

use std::convert::Infallible;
use std::time::Duration;

use ruststream_sqlx::prelude::*;

use crate::live;
use crate::pool::fresh_pool;
use crate::probe::{GUARD, Probe, until};

const POLL: Duration = Duration::from_millis(20);

/// How long the subscription stays at its bound before the suite takes it to stay there: many
/// polls of the subscription.
const QUIET: Duration = Duration::from_millis(500);

/// Rows enough to fill a pool of eight twice over.
const ROWS: i64 = 16;

live::matrix! {
    #[subscriber(InboxQueue::<Plain>::new("plain"), threads(2))]
    async fn held(n: &i64, State(probe): State<Probe>) {
        probe.handled("plain", *n);
        probe.gate().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_threads_leave_the_pool_its_last_connection() {
        // A lease delivery holds no connection; the claims suite covers its bound.
        if LEASED {
            return;
        }
        let Some(db) = database().await else { return };
        let pool = fresh_pool(&db);
        let size = pool.options().get_max_connections();
        let payloads: Vec<String> = (0..ROWS).map(|n| n.to_string()).collect();
        db.plain(&payloads).await;
        let probe = Probe::default();
        let state = probe.clone();
        let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(SqlxBroker::new(pool.clone()).poll_interval(POLL), |b| {
                b.include(held);
            });
        let running = tokio::time::timeout(GUARD, app.start())
            .await
            .expect("the service starts in time")
            .expect("the service starts");
        // Two handlers wait and the rings hold the rest: the subscription holds all it may.
        until("the subscription holds all the pool lends it", async || {
            pool.size() == size - 1 && pool.num_idle() == 0
        })
        .await;
        // A subscription that would take the last connection takes it within a few polls.
        let took_the_last = tokio::time::timeout(QUIET, async {
            while pool.size() < size {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(took_the_last.is_err(), "the subscription took the pool's last connection");
        let last = tokio::time::timeout(Duration::from_secs(5), pool.acquire())
            .await
            .expect("the pool lends its last connection")
            .expect("a connection");
        assert_eq!(pool.size(), size, "the last connection is the test's");
        drop(last);
        probe.open_gate();
        probe.reached(usize::try_from(ROWS).expect("fits")).await;
        until("every row settled", async || db.count("plain_jobs").await == 0).await;
        tokio::time::timeout(GUARD, running.shutdown())
            .await
            .expect("the service stops in time")
            .expect("the service stops");
        pool.close().await;
        db.finish().await;
    }
}
