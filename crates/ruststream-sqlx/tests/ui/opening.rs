use chrono::{DateTime, Utc};
use ruststream::testing::InProcess;
use ruststream::{OutgoingMessage, SubscriptionSource};
use ruststream_sqlx::{Inbox, InboxQueue, Publish, SqlxBroker};
use sqlx::{PgPool, Sqlite, SqliteConnection, SqlitePool};

/// A lease table, a form SQLite serves, at an isolation level, which SQLite does not open.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", isolation = serializable)]
struct Serializable {
    #[field(id)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Sqlite> for Serializable {
    async fn publish(
        _conn: &mut SqliteConnection,
        _message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        Ok(())
    }
}

/// A row lock table, a form Postgres serves, in a SQLite mode, which Postgres does not open.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", mode = immediate)]
struct Immediate {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

async fn on_sqlite(pool: SqlitePool) {
    let Ok(connected) = SqlxBroker::new(pool).connect_in_process().await else {
        return;
    };
    let _ = InboxQueue::<Serializable>::new("jobs").subscribe(&connected).await;
}

async fn on_postgres(pool: PgPool) {
    let Ok(connected) = SqlxBroker::new(pool).connect_in_process().await else {
        return;
    };
    let _ = InboxQueue::<Immediate>::new("jobs").subscribe(&connected).await;
}

/// The route a by-name subscription to the SQLite table reads it through, checked alike.
fn routed(pool: SqlitePool) -> SqlxBroker<Sqlite> {
    SqlxBroker::new(pool).route::<Serializable>("jobs")
}

fn main() {
    let _ = (on_sqlite, on_postgres, routed);
}
