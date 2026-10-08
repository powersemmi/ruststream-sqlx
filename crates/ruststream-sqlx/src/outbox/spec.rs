//! The settings of an outbox table described by hand, as types.
//!
//! Each typed setter of [`OutboxSpec`] adds one marker of this module to the
//! table's settings, and the table's `type Table` lists them in the order the chain sets them. A
//! setting the chain leaves out keeps its default: no headers column, a processed record deleted,
//! and the crate's events. A setting set twice does not compile ([`Merge`]).

mod builder;
mod declaration;

pub use builder::{Described, OutboxSpec, OutboxTable};
pub use declaration::Declaration;
use declaration::setting;

pub use crate::settings::{Merge, Push, Set, Unset};

setting!(
    /// The published headers, kept in one column: set by
    /// [`OutboxSpec::headers`](super::OutboxSpec::headers). The record implements
    /// [`HeaderRow`](crate::HeaderRow).
    Headers, Headers
);
setting!(
    /// The time a record was processed, which the database's clock writes: set by
    /// [`OutboxSpec::processed_at`](super::OutboxSpec::processed_at). Without it a processed record
    /// is deleted.
    ProcessedAt, ProcessedAt
);

/// The events a record runs itself instead of the crate's default, each set by
/// [`OutboxSpec::own`](super::OutboxSpec::own); the record implements the event's trait of
/// [`outbox`](super).
pub mod own {
    use super::declaration::{Declaration, setting};
    use super::{Set, Unset};

    setting!(
        /// The take of a record when its message reaches a subscription:
        /// [`outbox::Fetch`](crate::outbox::Fetch).
        Fetch, OwnFetch
    );
    setting!(
        /// The mark of an acknowledged record: [`outbox::Ack`](crate::outbox::Ack).
        Ack, OwnAck
    );
    setting!(
        /// What a retried record runs: [`outbox::Retry`](crate::outbox::Retry).
        Retry, OwnRetry
    );
    setting!(
        /// The mark of a dropped record: [`outbox::Discard`](crate::outbox::Discard).
        Discard, OwnDiscard
    );
    setting!(
        /// The selection of the unprocessed records at startup:
        /// [`outbox::Recover`](crate::outbox::Recover).
        Recover, OwnRecover
    );
}

/// An event a record runs itself: a marker of [`own`], which
/// [`OutboxSpec::own`](super::OutboxSpec::own) takes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an event of an outbox record",
    label = "not an outbox event",
    note = "name one of `outbox::spec::own::{{Fetch, Ack, Retry, Discard, Recover}}`"
)]
pub trait OwnEvent: Declaration {}

impl OwnEvent for own::Fetch {}
impl OwnEvent for own::Ack {}
impl OwnEvent for own::Retry {}
impl OwnEvent for own::Discard {}
impl OwnEvent for own::Recover {}
