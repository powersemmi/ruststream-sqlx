use chrono::{DateTime, Utc};
use ruststream::SubscriptionSource;
use ruststream::testing::InProcess;
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use sqlx::{PgPool, SqlitePool};

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

fn main() {
    let _ = (on_sqlite, on_postgres);
}
