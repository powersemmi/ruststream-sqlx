//! The settings of a queue table described by hand, as types.
//!
//! Each typed setter of [`InboxSpec`](crate::InboxSpec) adds one marker of this module to the
//! table's settings, and the table's `type Table` lists them in the order the chain sets them. A
//! setting the chain leaves out keeps its default: the row lock form, row mode, the
//! [`SystemClock`](crate::SystemClock), the database's default opening and the crate's events. A
//! setting set twice does not compile ([`Merge`]), and neither does a combination of settings no
//! table can hold.

pub(super) mod builder;
mod declaration;
mod rules;

use std::marker::PhantomData;

use ruststream_sqlx_dialect::{Isolation, Mode, Opening, level};

use declaration::setting;
pub use declaration::{Declaration, Merge, Push, Set, Unset};
#[doc(hidden)]
pub use rules::{
    ClaimOutsideAdvisory, ClockSlot, ExtendInLease, FifoOutsideAdvisory, LeaseOnServiceClock,
    LockInAdvisory, LockWithUnlock, PayloadOutsideHeaderFields, Valid,
};

setting!(
    /// The lease form, its expiry in `Time`: set by [`InboxSpec::lease`](crate::InboxSpec::lease).
    Lease<Time>, Form
);
setting!(
    /// The advisory lock form: set by [`InboxSpec::advisory`](crate::InboxSpec::advisory).
    Advisory, Form
);
setting!(
    /// Payload mode: set by [`InboxSpec::payload`](crate::InboxSpec::payload).
    Payload, Message
);
setting!(
    /// The partition key: set by [`InboxSpec::partition_key`](crate::InboxSpec::partition_key).
    Key, Key
);
setting!(
    /// The attempt count: set by [`InboxSpec::attempt`](crate::InboxSpec::attempt).
    Attempt, Attempt
);
setting!(
    /// The attempt count of a field sqlx converts from the column's `Decoded`
    /// (`#[sqlx(try_from = "..")]`): a row that does not decode reads its attempt as `Decoded`.
    /// Set by `#[derive(Inbox)]`.
    #[doc(hidden)]
    AttemptFrom<Decoded>, Attempt
);
setting!(
    /// The delivery's headers, read from one column: set by
    /// [`InboxSpec::headers`](crate::InboxSpec::headers).
    Headers, Headers
);
setting!(
    /// The delivery's headers, built from the row's header fields: set by
    /// [`InboxSpec::header_fields`](crate::InboxSpec::header_fields).
    HeaderFields, Headers
);
setting!(
    /// The time a retried row is due, in `Time`: set by
    /// [`InboxSpec::retry_after`](crate::InboxSpec::retry_after).
    RetryAfter<Time>, RetryAfter
);
setting!(
    /// The time a row was processed, in `Time`: set by
    /// [`InboxSpec::processed_at`](crate::InboxSpec::processed_at).
    ProcessedAt<Time>, ProcessedAt
);
setting!(
    /// Groups kept in order: set by [`InboxSpec::fifo_group`](crate::InboxSpec::fifo_group).
    Fifo, Fifo
);
setting!(
    /// The clock the table reads now from, a [`TimeSource`](crate::TimeSource): set by
    /// [`InboxSpec::clock`](crate::InboxSpec::clock).
    Clock<Source>, Clock
);
setting!(
    /// What the table's transactions open at, a [`level`] marker: set by
    /// [`InboxSpec::opens`](crate::InboxSpec::opens).
    Opens<Level>, Opening
);

/// The events a service writes itself, each set by [`InboxSpec::own`](crate::InboxSpec::own); the
/// row implements the event's trait.
pub mod own {
    use super::declaration::{Declaration, Set, Unset, setting};

    setting!(
        /// The service's own [`Claim`](crate::Claim): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Claim, OwnClaim
    );
    setting!(
        /// The service's own [`Fetch`](crate::Fetch): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Fetch, OwnFetch
    );
    setting!(
        /// The service's own [`Ack`](crate::Ack): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Ack, OwnAck
    );
    setting!(
        /// The service's own [`Retry`](crate::Retry): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Retry, OwnRetry
    );
    setting!(
        /// The service's own [`RetryAfter`](crate::RetryAfter): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        RetryAfter, OwnRetryAfter
    );
    setting!(
        /// The service's own [`Discard`](crate::Discard): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Discard, OwnDiscard
    );
    setting!(
        /// The service's own [`DeadLetter`](crate::DeadLetter): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        DeadLetter, OwnDeadLetter
    );
    setting!(
        /// The service's own [`Extend`](crate::Extend): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Extend, OwnExtend
    );
    setting!(
        /// The service's own [`Lock`](crate::Lock): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Lock, OwnLock
    );
    setting!(
        /// The service's own [`Unlock`](crate::Unlock): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        Unlock, OwnUnlock
    );
}

/// An event a service writes itself: a marker of [`own`], which
/// [`InboxSpec::own`](crate::InboxSpec::own) takes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an event of the service's own",
    label = "not an event",
    note = "name one of `spec::own::{{Claim, Fetch, Ack, Retry, RetryAfter, Discard, DeadLetter, \
            Extend, Lock, Unlock}}`"
)]
pub trait OwnEvent: Declaration {}

macro_rules! own_events {
    ($($event:ident),*) => {
        $(impl OwnEvent for own::$event {})*
    };
}

own_events!(
    Claim, Fetch, Ack, Retry, RetryAfter, Discard, DeadLetter, Extend, Lock, Unlock
);

/// An isolation level or a SQLite mode as a [`level`] marker, which
/// [`InboxSpec::opens`](crate::InboxSpec::opens) takes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an isolation level or a SQLite mode",
    label = "not a level",
    note = "name one of the `dialect::level` markers"
)]
pub trait OpeningLevel {
    /// The opening the marker stands for.
    const OPENING: Opening;
}

macro_rules! levels {
    ($($marker:ident => $opening:expr),*) => {
        $(impl OpeningLevel for level::$marker {
            const OPENING: Opening = $opening;
        })*
    };
}

levels!(
    ReadUncommitted => Opening::Isolation(Isolation::ReadUncommitted),
    ReadCommitted => Opening::Isolation(Isolation::ReadCommitted),
    RepeatableRead => Opening::Isolation(Isolation::RepeatableRead),
    Serializable => Opening::Isolation(Isolation::Serializable),
    Deferred => Opening::Mode(Mode::Deferred),
    Immediate => Opening::Mode(Mode::Immediate),
    Exclusive => Opening::Mode(Mode::Exclusive)
);
