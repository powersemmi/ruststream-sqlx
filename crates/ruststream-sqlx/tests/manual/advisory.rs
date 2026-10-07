//! A table in the advisory lock form, its key built from the row's id, on the database's clock
//! and opening its transactions in SQLite's IMMEDIATE mode.

use ruststream::testing::TestApp;
use ruststream_sqlx::__private::Events;
use ruststream_sqlx::InboxRow;
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::Sqlite;

use super::{SETTLED, broker, count, database};

const SCHEMA: &str = "
CREATE TABLE locked_jobs (
    job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    payload      BLOB NOT NULL
);
INSERT INTO locked_jobs (name, payload) VALUES ('jobs', '{\"n\":1}'), ('jobs', '{\"n\":2}');
";

const LEFT: &str = "SELECT count(*) FROM locked_jobs";

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Task {
    n: u32,
}

mod derived {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::{DatabaseClock, Inbox};

    #[derive(Debug, Inbox, sqlx::FromRow)]
    #[inbox(
        table = "locked_jobs",
        advisory_lock = "jobs-{job_id}",
        mode = immediate,
        clock = DatabaseClock
    )]
    pub(super) struct LockedJob {
        #[field(id, generated)]
        job_id: i64,
        #[field(group)]
        name: String,
        #[field(retry_after)]
        retry_after: DateTime<Utc>,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }
}

mod manual {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::dialect::{Column, KeyPart, level};
    use ruststream_sqlx::spec::{Advisory, Attempt, Clock, Opens, Payload, RetryAfter};
    use ruststream_sqlx::{AttemptRow, DatabaseClock, InboxSpec, InboxTable, PayloadRow};

    #[derive(Debug, sqlx::FromRow)]
    pub(super) struct LockedJob {
        job_id: i64,
        attempt: i16,
        payload: Vec<u8>,
    }

    impl InboxTable for LockedJob {
        type Id = i64;
        type Table = InboxSpec<(
            Advisory,
            RetryAfter<DateTime<Utc>>,
            Attempt,
            Payload,
            Clock<DatabaseClock>,
            Opens<level::Immediate>,
        )>;
        const TABLE: Self::Table = InboxSpec::new("locked_jobs", Column::new("job_id").generated())
            .advisory(&[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")])
            .group(Column::new("name"))
            .retry_after(Column::new("retry_after"))
            .attempt(Column::new("attempt").generated())
            .payload(Column::new("payload"))
            .clock::<DatabaseClock>()
            .opens::<level::Immediate>();

        fn id(&self) -> &i64 {
            &self.job_id
        }
    }

    impl PayloadRow for LockedJob {
        type Column = Vec<u8>;

        fn payload(&self) -> &[u8] {
            &self.payload
        }
    }

    impl AttemptRow for LockedJob {
        type Attempt = i16;

        fn attempt(&self) -> &i16 {
            &self.attempt
        }
    }
}

#[subscriber(InboxQueue::<derived::LockedJob>::new("jobs"))]
async fn run_derived(_task: &Task) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<manual::LockedJob>::new("jobs"))]
async fn run_manual(_task: &Task) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_derived_table_takes_each_row_under_its_lock() {
    let db = database(SCHEMA).await;
    let app = RustStream::new(AppInfo::new("worker", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(run_derived);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the deliveries settle");
    let mut tasks = tb
        .broker::<SqlxBroker<Sqlite>>()
        .subscriber("jobs")
        .assert_called(2)
        .settled(HandlerOutcome::ack())
        .received::<Task>();
    tasks.sort_by_key(|task| task.n);
    assert_eq!(tasks, [Task { n: 1 }, Task { n: 2 }]);
    assert_eq!(count(&db.pool, LEFT).await, 0, "the acks deleted both rows");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_manual_table_takes_each_row_under_its_lock() {
    let db = database(SCHEMA).await;
    let app = RustStream::new(AppInfo::new("worker", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(run_manual);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the deliveries settle");
    let mut tasks = tb
        .broker::<SqlxBroker<Sqlite>>()
        .subscriber("jobs")
        .assert_called(2)
        .settled(HandlerOutcome::ack())
        .received::<Task>();
    tasks.sort_by_key(|task| task.n);
    assert_eq!(tasks, [Task { n: 1 }, Task { n: 2 }]);
    assert_eq!(count(&db.pool, LEFT).await, 0, "the acks deleted both rows");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[test]
fn both_forms_describe_the_same_table() {
    assert_eq!(
        <derived::LockedJob as InboxRow>::SPEC,
        <manual::LockedJob as InboxRow>::SPEC
    );
    assert_eq!(
        <derived::LockedJob as Events<Sqlite>>::SHAPE,
        <manual::LockedJob as Events<Sqlite>>::SHAPE
    );
    let derived = <derived::LockedJob as Events<Sqlite>>::kinds();
    assert!(derived.is_some(), "the derived table is readable by name");
    assert_eq!(derived, <manual::LockedJob as Events<Sqlite>>::kinds());
}
