//! The transactional outbox: what a service publishes is recorded in its own table until a
//! consumer has processed it, and what was not processed is published again at startup.
//!
//! A struct deriving [`Outbox`](crate::Outbox) describes the table, `outbox!` registers it under
//! the names it tracks, and the registry hands out the two middlewares and the republish. The
//! events below are what the struct's statements do; a service implements the ones it lists in
//! `#[outbox(custom(..))]`, and always [`Publish`], which has no default.

mod events;
mod row;

pub use events::{Ack, Discard, Fetch, Publish, Recover, Retry, Tracked};
pub use row::OutboxRow;

/// The header a tracked message carries its record's id in: written with the id's `Display`,
/// read back with its `FromStr`.
pub const OUTBOX_ID_HEADER: &str = "x-ruststream-outbox-id";
