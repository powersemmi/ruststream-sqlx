//! The queue tables the scenarios read, each described the way a service describes it, and the
//! schema each one gets on the database a run measures.

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::{Inbox, Outbox, Publish, outbox};
use sqlx::{FromRow, PgConnection, Postgres};

/// The row lock form: no `locked_until` and no advisory lock, so a claim locks its rows in a
/// transaction that stays open until they settle. Postgres and MySQL serve it.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "bench_row_lock")]
pub struct RowLockJob {
    #[field(id, generated)]
    pub id: i64,
    #[field(attempt, generated)]
    pub attempt: i16,
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// The lease form: the claim writes `locked_until` and commits, and the handler runs with no
/// transaction open. Postgres and SQLite serve it here.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "bench_lease")]
pub struct LeaseJob {
    #[field(id, generated)]
    pub id: i64,
    #[field(attempt, generated)]
    pub attempt: i16,
    #[field(locked_until)]
    pub locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// The advisory lock form: each row is held by a lock on its id, on the connection that claimed
/// it.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "bench_advisory", advisory_lock = "bench_advisory-{id}")]
pub struct AdvisoryJob {
    #[field(id, generated)]
    pub id: i64,
    #[field(attempt, generated)]
    pub attempt: i16,
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// The table a route leads a name into: a by-name subscription reads it, and a `Repository` or a
/// `Routed` publish writes it.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "bench_named")]
pub struct NamedJob {
    #[field(id, generated)]
    pub id: i64,
    #[field(attempt, generated)]
    pub attempt: i16,
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// The insert a publish into [`NamedJob`]'s table runs: the service's own statement.
pub const NAMED_INSERT: &str = "INSERT INTO bench_named (payload) VALUES ($1)";

impl Publish<Postgres> for NamedJob {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(NAMED_INSERT)
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// Row mode: no payload column, so a handler takes the row the driver decoded.
#[derive(Debug, Clone, Inbox, FromRow)]
#[inbox(table = "bench_row_mode")]
pub struct OrderRow {
    #[field(id, generated)]
    pub id: i64,
    #[field(attempt, generated)]
    pub attempt: i16,
    pub customer: String,
    pub quantity: i32,
}

/// The outbox table the tracked scenarios record into.
#[derive(Debug, Outbox, FromRow)]
#[outbox(table = "bench_outbox")]
pub struct OutboxRecord {
    #[field(id)]
    pub id: i64,
    #[field(name)]
    pub name: String,
    #[field(payload)]
    pub payload: Vec<u8>,
    #[field(processed_at)]
    pub processed_at: Option<DateTime<Utc>>,
}

/// The record's insert: the service's own statement, which the raw half runs too.
pub const OUTBOX_INSERT: &str =
    "INSERT INTO bench_outbox (name, payload) VALUES ($1, $2) RETURNING id";

impl outbox::Publish<Postgres> for OutboxRecord {
    async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar(OUTBOX_INSERT)
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}
