//! A delivery settles as in the other forms and then frees its key, so no lock outlives it: an
//! acknowledgement, a retry, a delayed retry, a dead letter and a handler's panic each leave the
//! database without a lock of the broker.

use std::time::Duration;

use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::keys::Attempt;
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use sqlx::Pool;

use super::{Email, POLL, assert_no_lock, email};
use crate::live;

live::advisory_stands! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .route::<SendEmail>("emails")
            .route::<Plain>("plain")
            .route::<Keyed>("keyed")
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<Keyed>::new("keyed"))]
    async fn keyed_acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_acks_and_frees_its_key() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(acked);
                b.include(keyed_acked);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        // Two jobs of one tenant share one lock key: the second goes into work once the first's
        // acknowledgement freed the key.
        for _ in 0..2 {
            tb.broker::<SqlxBroker<Db>>()
                .message(&email())
                .to("keyed")
                .publish()
                .await
                .expect("the publish settles");
        }
        tb.settle().await.expect("the deliveries settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let keyed = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("keyed")
            .assert_called(2)
            .outcomes();
        assert_eq!(keyed, [Outcome::Ack, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "both keyed jobs are done");
        assert_no_lock(&db.pool, &["email_jobs-1", "plain-acme"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn retried_once(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        match attempt {
            Some(1) => HandlerOutcome::retry(),
            // The second delivery reads the attempt the first one's take counted; any other
            // reading drops the row, which the outcomes would show.
            Some(2) => HandlerOutcome::ack(),
            _ => HandlerOutcome::drop(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_returns_at_once_with_its_attempt_counted() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(retried_once);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the retry settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the acknowledgement deleted the row");
        assert_no_lock(&db.pool, &["plain_jobs-1"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // In process the harness counts each row the broker returns: the retried row is counted
    // before its key goes, so the publish waits for the second delivery too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process_the_harness_waits_for_a_retried_row() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(retried_once);
            });
        let tb = TestApp::start(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the acknowledgement deleted the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn later(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        match attempt {
            Some(1) => HandlerOutcome::retry_after(Duration::from_millis(300)),
            Some(2) => HandlerOutcome::ack(),
            _ => HandlerOutcome::drop(),
        }
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn always_retried(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::retry()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_after_and_dead_letter_settle_as_in_the_other_forms() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(later);
                b.include(always_retried)
                    .max_attempts(nonzero!(2u32))
                    .dead_letter("plain_jobs_dead");
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        assert!(db.email_waits().await, "the delayed row waits for its time in the table");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(600))
            .await
            .expect("the redeliveries settle");
        let emails = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2)
            .outcomes();
        assert_eq!(emails, [Outcome::Nack, Outcome::Ack]);
        let plain = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(plain, [Outcome::Nack, Outcome::Nack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the spent row left the queue");
        assert_eq!(db.count("plain_jobs_dead").await, 1, "the spent row moved");
        assert_no_lock(&db.pool, &["email_jobs-1", "plain_jobs-1"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"), on_failure(panic = retry))]
    async fn fails_first(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        assert_ne!(attempt, Some(1), "the first attempt fails");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_panic_leaves_no_lock() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("advisory", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(fails_first);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the retry settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Panicked, Outcome::Ack]);
        assert_eq!(db.count("plain_jobs").await, 0, "the second attempt acknowledged the row");
        assert_no_lock(&db.pool, &["plain_jobs-1"]).await;
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
