//! A delayed retry on a clock of the service's own reads that clock in process as it does live.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

use std::time::{Duration, SystemTime};

use chrono::{DateTime, TimeDelta, Utc};
use ruststream::OutgoingMessage;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{
    Clock, Inbox, InboxQueue, Insert, Publish, QueueDatabase, SqlxBroker, SystemClock,
};
use serde::{Deserialize, Serialize};
use sqlx::{Error, FromRow, Pool};

use crate::live;

/// How long the handler delays its row.
const HOUR: Duration = Duration::from_secs(3600);

/// How far the service's clock runs ahead of the host's.
const DAY: Duration = Duration::from_hours(24);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Reminder {
    n: u32,
}

/// The service's clock: a day ahead of the host's.
struct Tomorrow;

impl Clock for Tomorrow {
    fn now() -> SystemTime {
        SystemClock::now() + DAY
    }
}

/// A queue in the lease form on the service's clock, which every stand serves, SQLite in process
/// included: the database writes `retry_after` when a row arrives, and a retry delays the row
/// from `Tomorrow`.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "lifecycle_jobs", clock = Tomorrow)]
struct OnServiceTime {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl<DB> Publish<DB> for OnServiceTime
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), Error> {
        let job = Self {
            id: 0,
            name: message.name().to_owned(),
            // The database fills it in from its own clock.
            retry_after: Utc::now(),
            attempt: 1,
            locked_until: None,
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

live::lease_stands! {
    #[subscriber(InboxQueue::<OnServiceTime>::new("clock"))]
    async fn postponed(_reminder: &Reminder) -> HandlerOutcome {
        HandlerOutcome::retry_after(HOUR)
    }

    fn app(pool: &Pool<Db>) -> RustStream {
        let broker = SqlxBroker::new(pool.clone()).route::<OnServiceTime>("clock");
        RustStream::new(AppInfo::new("clock", "0.0.0")).with_broker(broker, |b| {
            b.include(postponed);
        })
    }

    /// How long the row waits: its `retry_after` against the host's clock.
    async fn wait(pool: &Pool<Db>) -> TimeDelta {
        let row: OnServiceTime = sqlx::query_as(
            "SELECT id, name, retry_after, attempt, locked_until, payload FROM lifecycle_jobs",
        )
        .fetch_one(pool)
        .await
        .expect("the row reads");
        row.retry_after - Utc::now()
    }

    /// Publishes a reminder, which the handler delays an hour, and checks the row waits an hour
    /// past the service's clock.
    async fn delayed_from_the_services_clock(tb: TestApp<()>, pool: &Pool<Db>) {
        tb.broker::<SqlxBroker<Db>>()
            .message(&Reminder { n: 1 })
            .to("clock")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("clock")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOUR));
        let waits = wait(pool).await;
        assert!(
            waits > TimeDelta::hours(24),
            "the row waits an hour past the service's clock, a day ahead, not {waits}"
        );
        tb.shutdown().await.expect("the app stops");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process_a_delayed_retry_reads_the_services_own_clock() {
        let Some(db) = database().await else { return };
        let tb = TestApp::start(app(&db.pool)).await.expect("the app starts");
        delayed_from_the_services_clock(tb, &db.pool).await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_a_delayed_retry_reads_the_services_own_clock() {
        let Some(db) = database().await else { return };
        let tb = TestApp::start_live(app(&db.pool)).await.expect("the app starts");
        delayed_from_the_services_clock(tb, &db.pool).await;
        db.finish().await;
    }
}
