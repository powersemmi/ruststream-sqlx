//! The core's conformance suites against each stand and form: the routing contract over by-name
//! subscriptions of a payload-mode table, and the lifecycle ladder and the retry cap through a typed
//! descriptor and repository.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::time::Duration;

use futures::executor::block_on;
use ruststream::conformance::{harness, retry};
use ruststream::{PublishPolicy, nonzero};
use ruststream_sqlx::{InboxQueue, Repository, SqlxBroker};
use sqlx::Pool;

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        // The suites subscribe to names they generate; a prefix route opens them all.
        SqlxBroker::new(pool.clone())
            .poll_interval(Duration::from_millis(50))
            .route::<ConformanceRow>("conformance.*")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn by_name_subscriptions_pass_the_routing_contract() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        harness::run_suite(move || broker(&pool)).await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_typed_descriptor_and_repository_climb_the_lifecycle() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        // The ladder holds a stream, a delivery and its settlement at once, past the 16 KiB a
        // future may take on the stack.
        Box::pin(harness::lifecycle(
            move || SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(50)),
            |name| InboxQueue::<LifecycleRow>::new(name.to_owned()),
            // Pairing a repository does no I/O, so its future is ready at once.
            |connected| {
                block_on(Repository::<LifecycleRow>::default().pair(connected))
                    .expect("a repository pairs with the connected broker")
            },
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_typed_descriptor_moves_a_spent_row_to_its_dead_letter_group() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        retry::broker_moves(
            move || SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(50)),
            |name| InboxQueue::<LifecycleRow>::new(name.to_owned()),
            |connected| {
                block_on(Repository::<LifecycleRow>::default().pair(connected))
                    .expect("a repository pairs with the connected broker")
            },
            nonzero!(3u32),
        )
        .await;
        db.finish().await;
    }
}
