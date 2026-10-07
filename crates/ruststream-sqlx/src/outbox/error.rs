//! What can go wrong while the outbox tracks a message.

use std::error::Error as StdError;

use sqlx::Error as SqlError;
use thiserror::Error;

/// A failure of the outbox: a record it could not write or read, or a republish that did not
/// reach the broker.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OutboxError {
    /// A message under a registered name was published before the outbox had a pool.
    #[error(
        "the outbox has no pool, so a message under `{name}` cannot be recorded: give it one with \
         `Outbox::new` or with `set_pool` in `on_startup`"
    )]
    NoPool {
        /// The registered name.
        name: &'static str,
    },
    /// The record of a published message was not written; the message was not sent.
    #[error("the outbox did not record a message published under `{name}` in `{record}`")]
    Record {
        /// The registered name.
        name: &'static str,
        /// The record type.
        record: &'static str,
        /// The database's error.
        #[source]
        source: SqlError,
    },
    /// The unprocessed records of a name were not read at startup.
    #[error("the outbox did not read the unprocessed records of `{name}` from `{record}`")]
    Recover {
        /// The registered name.
        name: &'static str,
        /// The record type.
        record: &'static str,
        /// The database's error.
        #[source]
        source: SqlError,
    },
    /// A record read at startup was not published again.
    #[error("the outbox did not publish record {id} of `{name}` in `{record}` again")]
    Republish {
        /// The registered name.
        name: &'static str,
        /// The record type.
        record: &'static str,
        /// The record's id.
        id: String,
        /// The publisher's error.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

/// The outbox's pool was set already: by [`Outbox::new`](super::Outbox::new) or by an earlier
/// [`set_pool`](super::Outbox::set_pool).
#[derive(Debug, Error)]
#[error("the outbox's pool is set already; `set_pool` takes one pool, once")]
#[non_exhaustive]
pub struct PoolAlreadySet;

/// A publish through [`wrap`](super::Outbox::wrap) that failed: the outbox did not record the
/// message, or the publisher it wraps did not send it.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TrackedPublishError<PublishError> {
    /// The record was not written, so the message was not sent.
    #[error(transparent)]
    Outbox(OutboxError),
    /// The wrapped publisher's error. The record stays unprocessed, and the next startup
    /// publishes it again.
    #[error(transparent)]
    Publish(PublishError),
}
