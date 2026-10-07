//! The core's conformance suites against each stand and form: the routing contract over by-name
//! subscriptions of a payload-mode table, the lifecycle ladder and the retry cap through a typed
//! descriptor and repository, and the carried lane of a row-mode table, for single deliveries and
//! for batches.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::convert::Infallible;
use std::time::Duration;

use futures::executor::block_on;
use ruststream::conformance::{capabilities, harness, retry};
use ruststream::{Connected, PublishPolicy, Publisher, nonzero};
use ruststream_sqlx::{InboxQueue, Repository, SqlxBroker};
use sqlx::Pool;

live::matrix! {
    /// A publisher of the connected broker: the carried suites take one, and a subscription whose
    /// broker moves its spent rows itself leaves it unused.
    fn any_publisher(connected: &Connected<SqlxBroker<Db>>) -> impl Publisher + use<> {
        // Pairing a repository does no I/O, so its future is ready at once.
        block_on(Repository::<LifecycleRow>::default().pair(connected))
            .expect("a repository pairs with the connected broker")
    }

    /// Two mails a producer writes, to different recipients.
    fn two_mails() -> Vec<WrittenMail> {
        vec![
            WrittenMail::queued("unnamed", "ann@example.com", None),
            WrittenMail::queued("unnamed", "bob@example.com", Some("hello")),
        ]
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_mode_delivery_lends_the_row_it_claimed() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(capabilities::carries(
            move || SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(50)),
            |name| InboxQueue::<WrittenMail>::new(name.to_owned()),
            any_publisher,
            async |_: &Connected<SqlxBroker<Db>>, name: &str, mail: &WrittenMail| {
                db.mail(&[WrittenMail { name: name.to_owned(), ..mail.clone() }]).await;
                Ok::<(), Infallible>(())
            },
            async |_: &Connected<SqlxBroker<Db>>, name: &str| {
                db.unreadable_mail(name).await;
                Ok::<(), Infallible>(())
            },
            &two_mails(),
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_mode_batch_lends_its_rows_in_order() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(capabilities::carries_batch(
            move || SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(50)),
            |name| InboxQueue::<WrittenMail>::new(name.to_owned()),
            any_publisher,
            async |_: &Connected<SqlxBroker<Db>>, name: &str, mail: &WrittenMail| {
                db.mail(&[WrittenMail { name: name.to_owned(), ..mail.clone() }]).await;
                Ok::<(), Infallible>(())
            },
            async |_: &Connected<SqlxBroker<Db>>, name: &str| {
                db.unreadable_mail(name).await;
                Ok::<(), Infallible>(())
            },
            &two_mails(),
        ))
        .await;
        db.finish().await;
    }
}
