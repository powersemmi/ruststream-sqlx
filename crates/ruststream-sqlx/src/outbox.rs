//! The transactional outbox: what a service publishes is recorded in its own table until a
//! consumer has processed it, and what was not processed is published again at startup.
//!
//! A struct deriving [`Outbox`](derive@crate::Outbox) describes the table, `outbox!` registers it under
//! the names it tracks, and the registry hands out the two middlewares and the republish. The
//! events below are what the struct's statements do; a service implements the ones it lists in
//! `#[outbox(custom(..))]`, and always [`Publish`], which has no default.

mod database;
mod error;
mod events;
mod layer;
mod publish;
mod registry;
mod republish;
mod row;
mod switch;
mod wrap;

pub use database::OutboxDatabase;
#[doc(hidden)]
pub use database::{OutboxSql, no_outbox_statement};
pub use error::{OutboxError, PoolAlreadySet, TrackedPublishError};
pub use events::{Ack, Discard, Fetch, Publish, Recover, Retry, Tracked};
pub use layer::TrackingLayer;
pub use publish::TrackingPublishLayer;
pub use registry::{Nil, Outbox, Registered};
#[doc(hidden)]
pub use registry::{RecordList, RecordNames};
pub use republish::Republishing;
pub use row::OutboxRow;
pub use wrap::TrackedPublisher;

/// The header a tracked message carries its record's id in: written with the id's `Display`,
/// read back with its `FromStr`.
pub const OUTBOX_ID_HEADER: &str = "x-ruststream-outbox-id";
