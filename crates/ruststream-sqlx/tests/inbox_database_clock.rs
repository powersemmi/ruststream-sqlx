//! A delayed retry on the database's clock, settled on the connection that ran the startup check.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

mod live;

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use ruststream::OutgoingMessage;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{
    DatabaseClock, Inbox, InboxQueue, Insert, Publish, QueueDatabase, SqlxBroker,
};
use serde::{Deserialize, Serialize};
use sqlx::pool::PoolOptions;
use sqlx::{Error, FromRow, Pool};

/// How long the handler delays its row.
const HOUR: Duration = Duration::from_secs(3600);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Reminder {
    n: u32,
}

/// A queue on the database's clock: the database writes `retry_after` when a row arrives, and
/// computes it again when a retry delays the row.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "clock_jobs", clock = DatabaseClock)]
struct OnDatabaseTime {
    #[field(id, generated)]
    id: i64,
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl<DB> Publish<DB> for OnDatabaseTime
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
            // The database fills it in from its own clock.
            retry_after: Utc::now(),
            attempt: 1,
            processed_at: None,
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

live::row_lock_stands! {
    #[subscriber(InboxQueue::<OnDatabaseTime>::new("clock"))]
    async fn postponed(_reminder: &Reminder) -> HandlerOutcome {
        HandlerOutcome::retry_after(HOUR)
    }

    /// How long the row waits: its `retry_after` against the clock of the host the stand runs on.
    async fn wait(pool: &Pool<Db>) -> TimeDelta {
        let row: OnDatabaseTime = sqlx::query_as(
            "SELECT id, retry_after, attempt, processed_at, payload FROM clock_jobs",
        )
        .fetch_one(pool)
        .await
        .expect("the row reads");
        row.retry_after - Utc::now()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delayed_retry_waits_on_the_connection_that_ran_the_startup_check() {
        let Some(db) = database().await else { return };
        // One connection: the startup check prepares every statement on the connection that later
        // claims the row and settles it.
        let pool = PoolOptions::<Db>::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with((*db.pool.connect_options()).clone())
            .await
            .expect("the test database accepts a connection");
        let broker = SqlxBroker::new(pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<OnDatabaseTime>("clock");
        let app = RustStream::new(AppInfo::new("clock", "0.0.0")).with_broker(broker, |b| {
            b.include(postponed);
        });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&Reminder { n: 1 })
            .to("clock")
            .publish()
            .await
            .expect("the publish settles");
        let waits = wait(&db.pool).await;
        assert!(
            waits > TimeDelta::minutes(59),
            "the row waits an hour on the database's clock, not {waits}"
        );
        tb.advance(Duration::from_millis(300))
            .await
            .expect("nothing comes back");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("clock")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOUR));
        tb.shutdown().await.expect("the app stops");
        pool.close().await;
        db.finish().await;
    }
}
