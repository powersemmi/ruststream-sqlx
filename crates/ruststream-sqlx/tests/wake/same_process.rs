//! A publish from the same process wakes the subscriptions of the table and the group it wrote,
//! run as an application through `TestApp::start_live` against each stand and form, with a poll
//! interval no test outlives.

use ruststream::runtime::PublishError;
use ruststream::testing::{TestApp, TestError};
use ruststream_sqlx::Repository;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::Pool;

use super::{ASLEEP, IDLE, INTERVAL, WOKEN};
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
    async fn file(_invoice: &Invoice) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("a"))]
    async fn first(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("b"))]
    async fn second(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    async fn publish<State: Send + Sync + 'static>(
        tb: &TestApp<State>,
        to: &str,
        n: u32,
    ) -> Result<(), PublishError<TestError>> {
        tb.broker::<SqlxBroker<Db>>().message(&Job { n }).to(to).publish().await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_wakes_its_subscription_before_the_interval() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("jobs", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(handle);
            });
        let tb = TestApp::start_live_within(app, WOKEN).await.expect("the app starts");
        tb.advance(IDLE).await.expect("the subscription waits its interval");
        publish(&tb, "plain", 1).await.expect("the publish wakes the subscription");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .with(&Job { n: 1 })
            .settled(HandlerOutcome::ack());
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
                b.include(file);
            });
        let tb = TestApp::start_live_within(app, WOKEN).await.expect("the app starts");
        tb.advance(IDLE).await.expect("both subscriptions wait their interval");
        publish(&tb, "orders", 7).await.expect("the order and its invoice are handled");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("invoices")
            .assert_called_once()
            .with(&Invoice { n: 7 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_to_one_group_wakes_only_its_subscription() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("groups", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(first);
                b.include(second);
            });
        let tb = TestApp::start_live_within(app, WOKEN).await.expect("the app starts");
        tb.advance(IDLE).await.expect("both subscriptions wait their interval");
        // A row of `b` written by another process: only the interval brings `b` to it.
        let waiting = serde_json::to_vec(&Job { n: 2 }).expect("json");
        db.mail(&[SendEmail::queued("b", waiting)]).await;
        publish(&tb, "a", 1).await.expect("the publish wakes `a`");
        tb.advance(ASLEEP).await.expect("`b` sleeps on");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("a")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Db>>().subscriber("b").assert_not_called();
        let rows = db.email_rows("email_jobs").await;
        assert!(
            rows.iter().any(|(group, _, _, finished)| group == "b" && !finished),
            "the row of `b` waits: {rows:?}"
        );
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
        let tb = TestApp::start_live_within(app, WOKEN).await.expect("the app starts");
        tb.advance(IDLE).await.expect("the subscription waits its interval");
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
        let mut handled: Vec<u32> = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .batches::<Job>()
            .into_iter()
            .flatten()
            .map(|job| job.n)
            .collect();
        handled.sort_unstable();
        assert_eq!(handled, [1, 2, 3], "every row was claimed before the interval");
        assert_eq!(db.count("plain_jobs").await, 0, "and acknowledged");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_publish_wakes_nothing() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("refused", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(handle);
            });
        let tb = TestApp::start_live_within(app, WOKEN).await.expect("the app starts");
        tb.advance(IDLE).await.expect("the subscription waits its interval");
        // A row written by another process, which a wake-up would bring the subscription to.
        db.plain(&[serde_json::to_vec(&Job { n: 1 }).expect("json")]).await;
        let refused = tb
            .broker::<SqlxBroker<Db>>()
            .publish_with_headers("plain", &Job { n: 2 }, &Tenant { tenant: "acme" })
            .await;
        assert!(
            matches!(refused, Err(TestError::Publish { .. })),
            "a header the table has no column for refuses the publish: {refused:?}"
        );
        tb.advance(ASLEEP).await.expect("the subscription sleeps on");
        tb.broker::<SqlxBroker<Db>>().subscriber("plain").assert_not_called();
        assert_eq!(db.count("plain_jobs").await, 1, "the row waits for the interval");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
