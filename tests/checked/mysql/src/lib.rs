//! Queue tables on MySQL described by checked structs, one per form MySQL claims rows in.
//! `cargo sqlx prepare` captures the statements the derive generates for them in `.sqlx`, which
//! `just sqlx-prepare` regenerates against the stand and checks.

#![cfg(feature = "checked")]

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use sqlx::FromRow;

/// An email claimed by row lock: a group per name, a delayed retry, a counted attempt and a mark
/// once processed.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "checked_emails", checked, db = mysql)]
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
#[inbox(table = "checked_ledger", checked, db = mysql)]
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
#[inbox(table = "checked_webhooks", advisory_lock = "webhook-{endpoint}", checked, db = mysql)]
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
