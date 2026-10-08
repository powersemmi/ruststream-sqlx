//! Shutdown of a subscription on dedicated threads with work in flight: the threads drain what
//! they hold, every row is handled once or stays in the queue, and nothing stays locked.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::hint::spin_loop;
use std::time::{Duration, Instant};

use ruststream_sqlx::prelude::*;

use crate::live;
use crate::locks::HeldLocks;
use crate::pool::{every_connection_answers, fresh_pool};
use crate::probe::{GUARD, Probe};

const POLL: Duration = Duration::from_millis(20);

/// The rows the suite writes: more than the threads hold at once.
const ROWS: i64 = 60;

/// How long each handler computes.
const WORK: Duration = Duration::from_millis(2);

live::matrix! {
    #[subscriber(InboxQueue::<Plain>::new("plain"), threads(2))]
    async fn busy(n: &i64, State(probe): State<Probe>) {
        let start = Instant::now();
        while start.elapsed() < WORK {
            spin_loop();
        }
        probe.handled("plain", *n);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_the_threads_and_leaves_nothing_locked() {
        let Some(db) = database().await else { return };
        let pool = fresh_pool(&db);
        let payloads: Vec<String> = (0..ROWS).map(|n| n.to_string()).collect();
        db.plain(&payloads).await;
        let probe = Probe::default();
        let state = probe.clone();
        let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(SqlxBroker::new(pool.clone()).poll_interval(POLL), |b| {
                b.include(busy);
            });
        let running = tokio::time::timeout(GUARD, app.start())
            .await
            .expect("the service starts in time")
            .expect("the service starts");
        probe.reached(3).await;
        tokio::time::timeout(GUARD, running.shutdown())
            .await
            .expect("the service stops in time")
            .expect("the service stops");
        let handled: Vec<i64> = probe.order().into_iter().map(|(_, n)| n).collect();
        let once: BTreeSet<i64> = handled.iter().copied().collect();
        assert_eq!(once.len(), handled.len(), "no row handled twice: {handled:?}");
        let left = db.count("plain_jobs").await;
        assert_eq!(
            i64::try_from(handled.len()).expect("a count") + left,
            ROWS,
            "each row handled and settled, or left in the queue"
        );
        assert!(left > 0, "the service stopped before it took every row");
        let ids: Vec<i64> = (1..=ROWS).collect();
        assert_eq!(
            Db::held_locks(&db.pool, "plain_jobs", &ids).await,
            0,
            "the stopped service holds no lock"
        );
        every_connection_answers(&pool, "after the shutdown").await;
        pool.close().await;
        db.finish().await;
    }
}
