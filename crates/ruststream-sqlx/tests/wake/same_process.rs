//! A publish from the same process wakes the subscriptions of the table and the group it wrote,
//! run as an application through `TestApp::start` against each stand and form, with a poll
//! interval no test outlives. A test first has the subscription it watches handle one job, in a
//! batch larger than one: its claim as it opened is then over, and so is the claim that took the
//! job, which found fewer rows than a batch holds, so the subscription waits its interval and only
//! a wake-up brings it to the next job. Every row is published through the harness, which settles
//! once each job was handled, so no assertion depends on when a subscription runs a claim. Which
//! subscriptions a publish wakes is pinned by the wake-up list's own tests.

use ruststream::runtime::PublishError;
use ruststream::testing::{TestApp, TestError};
use ruststream_sqlx::Repository;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::Pool;

use std::time::Duration;

use super::INTERVAL;
use crate::live;

/// A job a test publishes.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    n: u32,
}

/// The invoice a handler of `orders` answers with, a row of the group `invoices`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    n: u32,
}

/// How long a test waits for a job before it reports that no wake-up brought the subscription to
/// it: a bound on a failure, far below the poll interval.
const HANDLED: Duration = Duration::from_secs(10);

/// A header `plain_jobs` has no column for.
#[derive(Serialize)]
struct Tenant {
    tenant: &'static str,
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(INTERVAL)
            .route::<Plain>("plain")
            .route::<SendEmail>("orders")
            .route::<SendEmail>("a")
            .route::<SendEmail>("invoices")
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn handle(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn handle_all(jobs: &[Job]) -> Vec<HandlerOutcome> {
        jobs.iter().map(|_| HandlerOutcome::ack()).collect()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("orders"), reply)]
    async fn bill(job: &Job) -> Invoice {
        Invoice { n: job.n }
    }

    #[subscriber(InboxQueue::<SendEmail>::new("invoices"))]
    async fn file(invoices: &[Invoice]) -> Vec<HandlerOutcome> {
        invoices.iter().map(|_| HandlerOutcome::ack()).collect()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("a"))]
    async fn first(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("b"))]
    async fn second(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    /// Starts `app` in process.
    async fn started(app: RustStream) -> TestApp<()> {
        TestApp::start(app).await.expect("the app starts")
    }

    /// Publishes job `n` to `to`, and returns once it was handled.
    ///
    /// # Panics
    ///
    /// When it was not handled within [`HANDLED`].
    async fn publish<State: Send + Sync + 'static>(
        tb: &TestApp<State>,
        to: &str,
        n: u32,
    ) -> Result<(), PublishError<TestError>> {
        let job = Job { n };
        let published = tb.broker::<SqlxBroker<Db>>().message(&job).to(to).publish();
        tokio::time::timeout(HANDLED, published)
            .await
            .expect("a wake-up brings the subscription to the job")
    }

    /// Brings the batch subscription of `name` to its interval: it handles job 0.
    async fn waiting<State: Send + Sync + 'static>(tb: &TestApp<State>, name: &str) {
        publish(tb, name, 0).await.expect("the subscription handles a job");
    }

    /// The jobs `name` handled, in the order its batches came.
    fn handled<State: Send + Sync + 'static>(tb: &TestApp<State>, name: &str) -> Vec<u32> {
        tb.broker::<SqlxBroker<Db>>()
            .subscriber(name)
            .batches::<Job>()
            .into_iter()
            .flatten()
            .map(|job| job.n)
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_wakes_its_subscription_before_the_interval() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("jobs", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(handle_all.batch(nonzero!(4)));
            });
        let tb = started(app).await;
        waiting(&tb, "plain").await;
        publish(&tb, "plain", 1).await.expect("the publish wakes the subscription");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_batch_sizes(&[1, 1])
            .settled(HandlerOutcome::ack());
        assert_eq!(handled(&tb, "plain"), [0, 1]);
        assert_eq!(db.count("plain_jobs").await, 0, "the row was acknowledged");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_repository_reply_wakes_the_subscription_that_reads_it() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("billing", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(bill).out_reply(Repository::<SendEmail>::default());
                b.include(file.batch(nonzero!(4)));
            });
        let tb = started(app).await;
        waiting(&tb, "invoices").await;
        publish(&tb, "orders", 7).await.expect("the order and its invoice are handled");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("invoices")
            .assert_batch_sizes(&[1, 1])
            .settled(HandlerOutcome::ack());
        assert_eq!(handled(&tb, "invoices"), [0, 7]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_to_one_group_reaches_only_its_subscription() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("groups", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(first);
                b.include(second);
            });
        let tb = started(app).await;
        publish(&tb, "a", 1).await.expect("the publish wakes `a`");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("a")
            .assert_called_once()
            .with(&Job { n: 1 })
            .settled(HandlerOutcome::ack());
        // `b` reads the same table, and its claim takes only rows of its own group.
        tb.broker::<SqlxBroker<Db>>().subscriber("b").assert_not_called();
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publishes_during_a_claim_are_claimed_right_after_it() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("batches", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(handle_all.batch(nonzero!(2)));
            });
        let tb = started(app).await;
        waiting(&tb, "plain").await;
        // The first write wakes a claim; the others land while it runs or right after, and each
        // keeps a wake-up for the claim after it.
        let (one, two, three) = tokio::join!(
            publish(&tb, "plain", 1),
            publish(&tb, "plain", 2),
            publish(&tb, "plain", 3),
        );
        one.expect("the first publish is handled");
        two.expect("the second publish is handled");
        three.expect("the third publish is handled");
        let mut handled = handled(&tb, "plain");
        handled.sort_unstable();
        assert_eq!(handled, [0, 1, 2, 3], "every row was claimed before the interval");
        assert_eq!(db.count("plain_jobs").await, 0, "and acknowledged");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_publish_writes_and_delivers_nothing() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("refused", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(handle);
            });
        let tb = started(app).await;
        // A header the table has no column for refuses the write. In process the harness's
        // injection reports no outcome to the test, so the refusal shows in the table below.
        tb.broker::<SqlxBroker<Db>>()
            .publish_with_headers("plain", &Job { n: 2 }, &Tenant { tenant: "acme" })
            .await
            .expect("the injection is taken");
        tb.settle().await.expect("the refused write settles");
        tb.broker::<SqlxBroker<Db>>().subscriber("plain").assert_not_called();
        assert_eq!(db.count("plain_jobs").await, 0, "the refused row never reached the table");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
