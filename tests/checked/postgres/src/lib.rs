//! Queue tables on Postgres described by checked structs, one per form and a headers struct.
//! `cargo sqlx prepare` captures the statements the derive generates for them in `.sqlx`, which
//! `just sqlx-prepare` regenerates against the stand and checks.

#![cfg(feature = "checked")]

use chrono::{DateTime, Utc};
use ruststream_sqlx::Fetch;
use ruststream_sqlx::prelude::*;
use sqlx::{Error, FromRow, PgConnection, Postgres};

/// The email of the crate overview's example, whose statements its doctest reads from this
/// crate's offline data.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "email_jobs", checked, db = postgres)]
pub struct SendEmail {
    /// The row's id.
    #[field(id, generated)]
    pub job_id: i64,
    /// The queue the email belongs to.
    #[field(group)]
    pub name: String,
    /// How many times the email was claimed.
    #[field(attempt, generated)]
    pub attempt: i16,
    /// The message.
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// An email claimed by row lock: a group per name, a delayed retry, a counted attempt and a mark
/// once processed.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "checked_emails", checked, db = postgres)]
pub struct Email {
    /// The row's id.
    #[field(id, generated)]
    pub id: i64,
    /// The queue the email belongs to.
    #[field(group)]
    pub name: String,
    /// When the email is claimed again after a delayed retry.
    #[field(retry_after)]
    pub retry_after: DateTime<Utc>,
    /// How many times the email was claimed.
    #[field(attempt, generated)]
    pub attempt: i16,
    /// When the email was processed.
    #[field(processed_at)]
    pub processed_at: Option<DateTime<Utc>>,
    /// The message.
    #[field(payload)]
    pub payload: Vec<u8>,
    /// A note the handler reads beside the message.
    pub note: String,
}

/// A ledger entry claimed by lease, in order within its account.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "checked_ledger", checked, db = postgres)]
pub struct Entry {
    /// The row's id.
    #[field(id, generated)]
    pub id: i64,
    /// The account whose entries keep their order.
    #[field(group, fifo = true)]
    pub account: String,
    /// How many times the entry was claimed.
    #[field(attempt, generated)]
    pub attempt: i16,
    /// Until when the entry's delivery holds it.
    #[field(locked_until)]
    pub locked_until: Option<DateTime<Utc>>,
    /// The message.
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// A webhook held by an advisory lock on its endpoint.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "checked_webhooks", advisory_lock = "webhook-{endpoint}", checked, db = postgres)]
pub struct Webhook {
    /// The row's id.
    #[field(id, generated)]
    pub id: i64,
    /// The endpoint the webhook calls.
    pub endpoint: String,
    /// How many times the webhook was claimed.
    #[field(attempt, generated)]
    pub attempt: i16,
    /// The message.
    #[field(payload)]
    pub payload: Vec<u8>,
}

/// The queue table of an order, described by its headers struct.
#[derive(Debug, Clone, InboxHeaders, FromRow)]
#[inbox(table = "order_jobs", checked, db = postgres)]
pub struct OrderHeaders {
    /// The row's id.
    #[field(id, generated)]
    pub job_id: i64,
    /// The queue the order belongs to.
    #[field(group)]
    pub name: String,
    /// When the order was processed.
    #[field(processed_at)]
    pub processed_at: Option<DateTime<Utc>>,
    /// A header: the trace the order was placed in.
    pub trace: Option<String>,
}

/// The message a handler takes: the headers, and the order's total, which the service's own fetch
/// reads.
#[derive(Debug, Clone, Inbox, FromRow)]
#[inbox(custom(fetch))]
pub struct Order {
    /// The order's headers.
    #[field(headers)]
    #[sqlx(flatten)]
    pub headers: OrderHeaders,
    /// The order's total.
    pub total: i64,
}

impl Fetch<Postgres> for Order {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
        let rows = sqlx::query!(
            "SELECT job_id, name, processed_at, trace, total FROM order_jobs \
             WHERE job_id = ANY($1)",
            ids
        )
        .fetch_all(conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self {
                headers: OrderHeaders {
                    job_id: row.job_id,
                    name: row.name,
                    processed_at: row.processed_at,
                    trace: row.trace,
                },
                total: row.total,
            })
            .collect())
    }
}
