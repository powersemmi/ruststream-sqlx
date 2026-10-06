//! What happens when a row's attempts are spent: the declared move, or the discard, run as an
//! application against each stand and form, for rows that decode and rows that do not.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Pool};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Task {
    n: u32,
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<SendEmail>("emails")
            .route::<Plain>("plain")
            .route::<Unreadable>("unreadable")
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn always_retry(_task: &Task) -> HandlerOutcome {
        HandlerOutcome::retry()
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn plain_always_retry(_task: &Task) -> HandlerOutcome {
        HandlerOutcome::retry()
    }

    async fn run(app: RustStream, to: &str) -> TestApp<()> {
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&Task { n: 1 })
            .to(to)
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(800))
            .await
            .expect("the retries settle");
        tb
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn at_the_cap_a_row_moves_to_the_dead_letter_group() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(always_retry)
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("emails.dead");
            });
        let tb = run(app, "emails").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(3);
        let rows = db.email_rows("email_jobs").await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "emails.dead");
        assert_eq!(
            rows[0].2,
            attempts_after(3),
            "the row keeps the attempts of its three deliveries, the last of which moved it"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn at_the_cap_a_row_moves_into_the_dead_letter_table() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(plain_always_retry)
                    .max_attempts(nonzero!(2u32))
                    .dead_letter("plain_jobs_dead");
            });
        let tb = run(app, "plain").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2);
        assert_eq!(db.plain_rows("plain_jobs").await, Vec::<Vec<u8>>::new());
        assert_eq!(
            db.plain_rows("plain_jobs_dead").await,
            [serde_json::to_vec(&Task { n: 1 }).expect("json")]
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn at_the_cap_without_a_destination_a_row_is_finished() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(always_retry).max_attempts(nonzero!(2u32));
            });
        let tb = run(app, "emails").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2);
        let rows = db.email_rows("email_jobs").await;
        assert_eq!(rows[0].0, "emails");
        assert!(
            rows[0].3,
            "processed_at means finished, a rejection after the last attempt included"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn always_later(_task: &Task) -> HandlerOutcome {
        HandlerOutcome::retry_after(Duration::from_millis(50))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delayed_retry_obeys_the_cap_too() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(always_later)
                    .max_attempts(nonzero!(2u32))
                    .dead_letter("emails.dead");
            });
        let tb = run(app, "emails").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2);
        assert_eq!(db.email_rows("email_jobs").await[0].0, "emails.dead");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Unreadable>::new("unreadable"))]
    async fn never_decoded(_task: &Task) -> HandlerOutcome {
        // No row of the table decodes; reaching this would acknowledge instead.
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_that_does_not_decode_counts_its_attempts_to_the_cap() {
        let Some(db) = database().await else { return };
        let retries = FailurePolicies::default().with_decode(FailurePolicy::Retry);
        let app =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(never_decoded.on_failure(retries))
                    .max_attempts(nonzero!(2u32));
            });
        let tb = run(app, "unreadable").await;
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("unreadable")
            .assert_called(2)
            .outcomes();
        assert_eq!(
            outcomes,
            [Outcome::DecodeFailed; 2],
            "the decode policy settled both deliveries"
        );
        assert_eq!(
            db.count("unreadable_jobs").await,
            0,
            "the second delivery spent the row's attempts, and the cap finished it"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn plain_once(_task: &Task) -> HandlerOutcome {
        HandlerOutcome::retry()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cap_needs_an_attempt_column_and_a_destination_a_table_name() {
        // A lease, which every stand serves: the cap is refused whatever the form.
        #[derive(Debug, Inbox, FromRow)]
        #[inbox(table = "plain_jobs")]
        struct Uncounted {
            #[field(id)]
            id: i64,
            #[field(locked_until)]
            locked_until: Option<DateTime<Utc>>,
            #[field(payload)]
            payload: Vec<u8>,
        }

        #[subscriber(InboxQueue::<Uncounted>::new("plain"))]
        async fn uncounted(_task: &Task) -> HandlerOutcome {
            HandlerOutcome::ack()
        }

        let Some(db) = database().await else { return };
        let capped =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(uncounted).max_attempts(nonzero!(3u32));
            });
        let message = format!(
            "{:?}",
            TestApp::start_live(capped)
                .await
                .map(|_| ())
                .expect_err("no attempt column")
        );
        assert!(message.contains("#[field(attempt)]"), "{message}");

        let malformed =
            RustStream::new(AppInfo::new("retries", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(plain_once)
                    .max_attempts(nonzero!(3u32))
                    .dead_letter("a.b.c");
            });
        let message = format!(
            "{:?}",
            TestApp::start_live(malformed)
                .await
                .map(|_| ())
                .expect_err("no such table name")
        );
        assert!(message.contains("a.b.c"), "{message}");
        let _ = |row: Uncounted| (row.id, row.locked_until, row.payload);
        db.finish().await;
    }
}
