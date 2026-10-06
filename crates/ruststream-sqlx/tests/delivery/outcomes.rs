//! Every outcome a handler can give a row, run as an application through `TestApp::start_live`
//! against each stand and form: acknowledgement, drop, retry, delayed retry, and what stops a
//! service at startup.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

use std::time::Duration;

use ruststream::prelude::*;
use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::keys::Attempt;
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::Pool;

use crate::live;

const POLL: Duration = Duration::from_millis(20);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "a@example.com".to_owned(),
    }
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .route::<SendEmail>("emails")
            .route::<Plain>("plain")
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn plain_acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_row_is_marked_finished_or_deleted() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(acked);
                b.include(plain_acked);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");

        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&email())
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        // `processed_at` marks the row; a table without it loses the row.
        let rows = db.email_rows("email_jobs").await;
        assert_eq!(rows.len(), 1);
        assert!(rows[0].3, "an acknowledged row carries processed_at");
        assert_eq!(db.plain_rows("plain_jobs").await, Vec::<Vec<u8>>::new());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn dropped(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::drop()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_row_is_finished_too() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(dropped);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        // A dropped row stays finished: it never comes back.
        tb.advance(Duration::from_millis(200))
            .await
            .expect("nothing else runs");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        assert!(db.email_rows("email_jobs").await[0].3);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn flaky(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        if attempt < Some(2) {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_counts_the_attempt_and_releases_the_row_at_once() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(flaky);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the retry settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        assert_eq!(
            db.email_rows("email_jobs").await,
            [(
                "emails".to_owned(),
                serde_json::to_vec(&email()).expect("json"),
                attempts_after(2),
                true
            )]
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn later(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        if attempt < Some(2) {
            HandlerOutcome::retry_after(Duration::from_millis(300))
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delayed_retry_comes_back_when_its_time_has_come() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(later);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        assert!(
            db.email_waits().await,
            "the row waits for its time in the table"
        );
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once();
        tb.advance(Duration::from_millis(600))
            .await
            .expect("the redelivery settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn plain_later(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        if attempt < Some(2) {
            HandlerOutcome::retry_after(Duration::from_secs(60))
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delayed_retry_without_a_retry_after_column_comes_back_at_once() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(plain_later);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&email())
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        // The table cannot hold the delay, so the runtime requeues at once instead of in a minute.
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the requeue settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    /// Bytes that are no model, published and received as they are.
    #[derive(Outgoing, Serialized)]
    struct Blob(Vec<u8>);

    #[derive(Deserialized)]
    struct Frame<'a>(&'a [u8]);

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn raw(frame: &Frame<'_>) -> HandlerOutcome {
        if frame.0 == [0, 0xff, 0xfe, b'\n'] {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::drop()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_binary_payload_reaches_the_handler_byte_for_byte() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(raw);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&Blob(vec![0, 0xff, 0xfe, b'\n']))
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .with_raw(&[0, 0xff, 0xfe, b'\n'])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn nothing(frame: &Frame<'_>) -> HandlerOutcome {
        if frame.0.is_empty() {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::drop()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_empty_payload_reaches_the_handler_empty() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(nothing);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&Blob(Vec::new()))
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .with_raw(&[])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    /// An email that carries its tenant in a header.
    #[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
    #[outgoing(headers = Tenant)]
    struct TenantEmail {
        to: String,
    }

    #[derive(Serialize, Deserialize)]
    struct Tenant {
        #[serde(rename = "x-tenant")]
        tenant: String,
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn tenanted(_email: &TenantEmail, ctx: &mut Context<'_>) -> HandlerOutcome {
        if ctx.headers().get_str("x-tenant") == Some("acme") {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::drop()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn headers_written_by_the_publish_reach_the_handler() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(tenanted);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&TenantEmail {
                to: "a@example.com".to_owned(),
            })
            .with_headers(&Tenant {
                tenant: "acme".to_owned(),
            })
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn twin(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_second_subscription_to_one_queue_stops_the_service() {
        let Some(db) = database().await else { return };
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(acked);
                b.include(twin);
            });
        let refused = TestApp::start_live(app).await.map(|_| ());
        let message = format!(
            "{:?}",
            refused.expect_err("a second subscription is refused")
        );
        assert!(message.contains("AlreadySubscribed"), "{message}");
        assert!(message.contains("email_jobs"), "{message}");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain.other"))]
    async fn plain_twin(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_second_subscription_to_a_table_without_groups_stops_the_service() {
        let Some(db) = database().await else { return };
        // A table without groups is one queue, whatever name a subscription gives it.
        let app =
            RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker(&db.pool), |b| {
                b.include(plain_acked);
                b.include(plain_twin);
            });
        let refused = TestApp::start_live(app).await.map(|_| ());
        let message = format!(
            "{:?}",
            refused.expect_err("a second subscription to the table is refused")
        );
        assert!(message.contains("AlreadySubscribed"), "{message}");
        assert!(message.contains("plain_jobs"), "{message}");
        db.finish().await;
    }
}

/// What only Postgres says: how its statements quote a column, and how it refuses one.
#[cfg(feature = "postgres")]
mod on_postgres {
    use ruststream_sqlx::Inbox;
    use sqlx::FromRow;

    use super::*;
    use crate::live::postgres::database;

    /// A struct that reads a column `broken_jobs` does not have.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "broken_jobs")]
    struct Broken {
        #[field(id, generated)]
        id: i64,
        #[field(attempt)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    #[subscriber(InboxQueue::<Broken>::new("broken"))]
    async fn never(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_missing_column_stops_the_service_naming_table_and_statement() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone()).poll_interval(POLL);
        let app = RustStream::new(AppInfo::new("inbox", "0.0.0")).with_broker(broker, |b| {
            b.include(never);
        });
        let refused = TestApp::start_live(app).await.map(|_| ());
        let message = refused
            .expect_err("the startup check refuses the table")
            .to_string();
        assert!(message.contains("table `broken_jobs`"), "{message}");
        assert!(message.contains(r#"SELECT "id", "attempt""#), "{message}");
        assert!(
            message.contains(r#"column "attempt" does not exist"#),
            "{message}"
        );
        let _ = Broken {
            id: 0,
            attempt: 0,
            payload: Vec::new(),
        };
        db.finish().await;
    }
}
