//! A flat table in payload mode, in the lease form: an attempt the delivery reads, a delayed
//! retry and a processed mark. A by-name subscription reads it through the same column kinds
//! whichever form describes it.

use ruststream::OutgoingMessage;
use ruststream::testing::TestApp;
use ruststream_sqlx::__private::Events;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{InboxRow, Publish};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use super::{SETTLED, broker, count, database};

const SCHEMA: &str = "
CREATE TABLE email_tasks (
    job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    locked_until TEXT,
    payload      BLOB NOT NULL
);
";

const QUEUED: &str =
    "INSERT INTO email_tasks (name, payload) VALUES ('emails', '{\"to\":\"ops@example.com\"}')";

const PROCESSED: &str = "SELECT count(*) FROM email_tasks WHERE processed_at IS NOT NULL";

/// The message a handler decodes from the payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "ops@example.com".to_owned(),
    }
}

mod derived {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::Inbox;

    #[derive(Debug, Inbox, sqlx::FromRow)]
    #[inbox(table = "email_tasks")]
    pub(super) struct EmailJob {
        #[field(id, generated)]
        job_id: i64,
        #[field(group)]
        name: String,
        #[field(retry_after)]
        retry_after: DateTime<Utc>,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(processed_at)]
        processed_at: Option<DateTime<Utc>>,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }
}

mod manual {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::dialect::Column;
    use ruststream_sqlx::spec::{Attempt, Lease, Payload, ProcessedAt, RetryAfter};
    use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};

    // The struct holds what the service reads; the columns the queue alone reads are named in the
    // description and need no field.
    #[derive(Debug, sqlx::FromRow)]
    pub(super) struct EmailJob {
        job_id: i64,
        attempt: i16,
        payload: Vec<u8>,
    }

    impl InboxTable for EmailJob {
        type Id = i64;
        type Table = InboxSpec<(
            Lease<DateTime<Utc>>,
            RetryAfter<DateTime<Utc>>,
            Attempt,
            ProcessedAt<DateTime<Utc>>,
            Payload,
        )>;
        const TABLE: Self::Table = InboxSpec::new("email_tasks", Column::new("job_id").generated())
            .lease(Column::new("locked_until"))
            .group(Column::new("name"))
            .retry_after(Column::new("retry_after"))
            .attempt(Column::new("attempt").generated())
            .processed_at(Column::new("processed_at"))
            .payload(Column::new("payload"));

        fn id(&self) -> &i64 {
            &self.job_id
        }
    }

    impl PayloadRow for EmailJob {
        type Column = Vec<u8>;

        fn payload(&self) -> &[u8] {
            &self.payload
        }
    }

    impl AttemptRow for EmailJob {
        type Attempt = i16;

        fn attempt(&self) -> &i16 {
            &self.attempt
        }
    }
}

/// Writes a published message into the table, for a route to either form.
async fn publish_into(
    conn: &mut SqliteConnection,
    message: &OutgoingMessage<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO email_tasks (name, payload) VALUES (?, ?)")
        .bind(message.name())
        .bind(message.payload())
        .execute(conn)
        .await?;
    Ok(())
}

impl Publish<Sqlite> for derived::EmailJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        publish_into(conn, message).await
    }
}

impl Publish<Sqlite> for manual::EmailJob {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        publish_into(conn, message).await
    }
}

#[subscriber(InboxQueue::<derived::EmailJob>::new("emails"))]
async fn send_derived(_email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    // The first delivery waits a moment and comes back counted as the second attempt.
    if attempt == Some(1) {
        HandlerOutcome::retry_after(SETTLED / 8)
    } else {
        HandlerOutcome::ack()
    }
}

#[subscriber(InboxQueue::<manual::EmailJob>::new("emails"))]
async fn send_manual(_email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    if attempt == Some(1) {
        HandlerOutcome::retry_after(SETTLED / 8)
    } else {
        HandlerOutcome::ack()
    }
}

#[subscriber("emails")]
async fn send_by_name(_email: &Email) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_derived_table_retries_then_marks_the_row() {
    let db = database(SCHEMA).await;
    sqlx::query(QUEUED)
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let app = RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send_derived);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("emails")
        .assert_called(2)
        .with(&email())
        .settled(HandlerOutcome::ack());
    assert_eq!(
        count(&db.pool, PROCESSED).await,
        1,
        "the ack set processed_at"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_manual_table_retries_then_marks_the_row() {
    let db = database(SCHEMA).await;
    sqlx::query(QUEUED)
        .execute(&db.pool)
        .await
        .expect("the row writes");
    let app = RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send_manual);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("emails")
        .assert_called(2)
        .with(&email())
        .settled(HandlerOutcome::ack());
    assert_eq!(
        count(&db.pool, PROCESSED).await,
        1,
        "the ack set processed_at"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_by_name_reads_the_manual_table() {
    let db = database(SCHEMA).await;
    let broker = broker(&db.pool).route::<manual::EmailJob>("emails");
    let app = RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker, |b| {
        b.include(send_by_name);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Sqlite>>()
        .message(&email())
        .to("emails")
        .publish()
        .await
        .expect("the publish settles");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("emails")
        .assert_called_once()
        .with(&email())
        .settled(HandlerOutcome::ack());
    assert_eq!(
        count(&db.pool, PROCESSED).await,
        1,
        "the ack set processed_at"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[test]
fn both_forms_describe_the_same_table() {
    assert_eq!(
        <derived::EmailJob as InboxRow>::SPEC,
        <manual::EmailJob as InboxRow>::SPEC
    );
    assert_eq!(
        <derived::EmailJob as Events<Sqlite>>::SHAPE,
        <manual::EmailJob as Events<Sqlite>>::SHAPE
    );
}

#[test]
fn a_subscription_by_name_reads_the_same_kinds_from_both_forms() {
    let derived = <derived::EmailJob as Events<Sqlite>>::kinds();
    assert!(derived.is_some(), "the derived table is readable by name");
    assert_eq!(derived, <manual::EmailJob as Events<Sqlite>>::kinds());
}
