//! Queue tables on SQLite described by checked structs, one per form SQLite claims rows in.
//! `cargo sqlx prepare` captures the statements the derive generates for them in `.sqlx`, which
//! `just sqlx-prepare` regenerates against the stand and checks.

#![cfg(feature = "checked")]

use chrono::{DateTime, Utc};
use ruststream_sqlx::prelude::*;
use sqlx::FromRow;

/// A ledger entry claimed by lease, in order within its account.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "checked_ledger", checked, db = sqlite)]
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
#[inbox(table = "checked_webhooks", advisory_lock = "webhook-{endpoint}", checked, db = sqlite)]
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
