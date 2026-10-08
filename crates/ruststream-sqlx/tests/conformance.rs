//! The core's conformance suites against each stand and form: the routing contract over by-name
//! subscriptions of a payload-mode table, the lifecycle ladder and the retry cap through a typed
//! descriptor and repository, the carried lane of a row-mode table, for single deliveries and for
//! batches, what each settlement means on the server and in process, the shutdown, the backlog and
//! the refusals of the in-process transport against the server's, batches on the payload lane, the
//! order of a key, and the generated document's silence about credentials.
//!
//! Two suites of the core do not apply. `lifecycle::shared_handle_closes` checks clones of a
//! shareable connected form, and the connected broker is not `Clone`. `message_shape::publish_options`
//! checks per-message settings, and the inbox's publishers take none (`Options = ()`).

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
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::{capabilities, harness, lifecycle, message_shape, retry, settlement};
use ruststream::testing::Backlog;
use ruststream::{Bytes, Connected, PublishPolicy, Publisher, nonzero};
use ruststream_sqlx::{InboxQueue, Repository, Routed, SqlxBroker};
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

    /// The broker the typed suites connect with: one subscription's claims, settled or dropped,
    /// show within the poll interval.
    fn typed_broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(50))
    }

    /// A repository of the lifecycle rows, paired with the connected broker.
    fn lifecycle_publisher(connected: &Connected<SqlxBroker<Db>>) -> impl Publisher + use<> {
        // Pairing a repository does no I/O, so its future is ready at once.
        block_on(Repository::<LifecycleRow>::default().pair(connected))
            .expect("a repository pairs with the connected broker")
    }

    /// The routed publisher: a publish reaches the table its name's route leads to.
    fn routed_publisher(connected: &Connected<SqlxBroker<Db>>) -> impl Publisher + use<> {
        // Pairing the routed policy does no I/O, so its future is ready at once.
        block_on(Routed.pair(connected)).expect("the routed policy pairs with the connected broker")
    }

    // A delivery dropped unsettled returns at once in every form, so no redelivery timeout
    // applies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settlements_mean_on_the_server_what_they_say() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(settlement::suite(
            move || typed_broker(&pool),
            |name| InboxQueue::<LifecycleRow>::new(name.to_owned()),
            lifecycle_publisher,
            Duration::ZERO,
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settlements_in_process_answer_as_on_the_server() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(settlement::matches_in_process(
            move || typed_broker(&pool),
            |name| InboxQueue::<LifecycleRow>::new(name.to_owned()),
            lifecycle_publisher,
            Duration::ZERO,
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shutdown_keeps_what_was_published_and_acknowledged() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(lifecycle::shutdown_flushes(
            move || typed_broker(&pool),
            |name| InboxQueue::<LifecycleRow>::new(name.to_owned()),
            lifecycle_publisher,
            Backlog::Delivered,
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_in_process_backlog_is_the_servers() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(in_process::backlog_matches_server(move || broker(&pool), routed_publisher)).await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_in_process_transport_refuses_what_the_server_refuses() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        let queue = |name: &str| InboxQueue::<LifecycleRow>::new(name.to_owned());
        let held = unique_subject("conformance.refused");
        Box::pin(in_process::refuses_like_the_server(
            move || broker(&pool),
            routed_publisher,
            [
                // No route leads the name to a table.
                Refusal::Publish {
                    name: unique_subject("unrouted"),
                },
                // One queue has one subscription per process.
                Refusal::Conflicting {
                    open: queue(&held),
                    refused: queue(&held),
                },
            ],
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batches_of_the_payload_lane_settle_one_by_one() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(capabilities::batches(
            move || typed_broker(&pool),
            |name| InboxQueue::<LifecycleRow>::new(name.to_owned()),
            lifecycle_publisher,
        ))
        .await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_key_keeps_its_order() {
        let Some(db) = database().await else { return };
        let pool = db.pool.clone();
        Box::pin(message_shape::keyed_order(
            move || typed_broker(&pool),
            &unique_subject("conformance.keyed"),
            |name| InboxQueue::<SendEmail>::new(name.to_owned()),
            |connected| {
                // Pairing a repository does no I/O, so its future is ready at once.
                block_on(Repository::<SendEmail>::default().pair(connected))
                    .expect("a repository pairs with the connected broker")
            },
            // The row's publish reads its key from the `customer` header.
            |key, headers| {
                headers.insert("customer", Bytes::copy_from_slice(key));
                None
            },
        ))
        .await;
        db.finish().await;
    }
}

/// The generated document of a broker on a server's pool names no credential: pools are built from
/// URLs with passwords, and the document is published. The pools are lazy, so the checks reach no
/// server.
#[cfg(feature = "asyncapi")]
mod credentials {
    use ruststream::Connected;
    use ruststream::conformance::{harness, message_shape};
    use ruststream_sqlx::{InboxQueue, Repository, Routed, SqlxBroker};

    use crate::live::rows::lease::LifecycleRow;

    const SECRET: &str = "conformance-secret-7f3a";

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_describes_itself_without_credentials() {
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{PgPool, Postgres};

        let lazy = |url: &str| -> PgPool {
            PgPoolOptions::new()
                .connect_lazy(url)
                .expect("the URL parses")
        };
        let pool = lazy(&format!("postgres://app:{SECRET}@db.invalid:5432/app"));
        harness::describes_without_credentials(
            &SqlxBroker::new(pool),
            &InboxQueue::<LifecycleRow>::new("orders"),
            SECRET,
        );
        message_shape::describes_addresses_without_credentials(
            |addrs| SqlxBroker::new(lazy(addrs[0])),
            "postgres",
        );
        message_shape::publishes_without_credentials::<Connected<SqlxBroker<Postgres>>, _>(
            &Repository::<LifecycleRow>::default(),
            SECRET,
        );
        message_shape::publishes_without_credentials::<Connected<SqlxBroker<Postgres>>, _>(
            &Routed, SECRET,
        );
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    async fn mysql_describes_itself_without_credentials() {
        use sqlx::mysql::MySqlPoolOptions;
        use sqlx::{MySql, MySqlPool};

        let lazy = |url: &str| -> MySqlPool {
            MySqlPoolOptions::new()
                .connect_lazy(url)
                .expect("the URL parses")
        };
        let pool = lazy(&format!("mysql://app:{SECRET}@db.invalid:3306/app"));
        harness::describes_without_credentials(
            &SqlxBroker::new(pool),
            &InboxQueue::<LifecycleRow>::new("orders"),
            SECRET,
        );
        message_shape::describes_addresses_without_credentials(
            |addrs| SqlxBroker::new(lazy(addrs[0])),
            "mysql",
        );
        message_shape::publishes_without_credentials::<Connected<SqlxBroker<MySql>>, _>(
            &Repository::<LifecycleRow>::default(),
            SECRET,
        );
        message_shape::publishes_without_credentials::<Connected<SqlxBroker<MySql>>, _>(
            &Routed, SECRET,
        );
    }
}
