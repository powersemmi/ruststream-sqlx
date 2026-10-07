//! The machinery `#[derive(Inbox)]` generates against: the statements a subscription prepared,
//! how a default event binds and runs one, and the hidden contract each row implements.
//!
//! Nothing here is named by a service; the derive and the broker are its only callers.

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use ruststream::HeaderMap;
use ruststream::codec::CodecError;
use ruststream_sqlx_dialect::{Param, TableSpec};
use sqlx::{Error, FromRow};

use super::QueueRow;
use super::database::QueueDatabase;
use super::form::advisory::events::Candidates;
use super::headers::HeaderCell;
use super::named::kinds::Kinds;
use super::queue::Queue;

mod claim;
mod context;
mod helpers;
mod settle;
mod statements;
mod values;

pub use claim::{claim_ids, claim_rows, fetch_by_ids, match_claimed, match_rows};
pub(crate) use claim::{stamp, take_group};
pub use context::{Claiming, Leasing, Now, Settling, TimeFor, Via};
use helpers::run;
pub(crate) use helpers::{arguments, unbound, unprepared};
pub use helpers::{attempt_in, first_header, later, lease, micros, no_lease, now, put};
pub use settle::{ack, dead_letter, discard, extend, retry, retry_after};
pub use statements::{Prepared, Savepoint, Stmt};
pub(crate) use statements::{intern, intern_name};
pub use values::Values;

/// A row's whole contract with the broker. Machinery; the derive implements it, a service never
/// names it.
pub trait Events<DB: QueueDatabase>: QueueRow + for<'r> FromRow<'r, DB::Row> + Unpin {
    /// Which events the service implements itself.
    const SHAPE: Shape;

    /// The lease a delivery holds: the expiry its claim wrote into `locked_until`, which its
    /// settlements match; `()` for a table in another form.
    type Token: Copy + Debug + Send + Sync + 'static;

    /// Where a delivery keeps its header map: the map moved out of the `headers` field for a flat
    /// struct, a cell built on the first read for a message assembled from a headers struct.
    type Headers: HeaderCell<DB, Self>;

    /// The kinds a by-name subscription reads and binds the row's columns by, or `None` when it
    /// needs the row's own code: an event of the service's own, or a column type outside them.
    fn kinds() -> Option<Kinds>;

    /// The row's id.
    fn id(&self) -> &Self::Id;

    /// The delivery's headers, moved out of the `headers` field; none without one.
    fn take_headers(&mut self) -> HeaderMap;

    /// The first of `headers` the row cannot hold byte for byte: any of them where it has no
    /// `headers` field.
    fn unfit_header(headers: &HeaderMap) -> Option<&str>;

    /// The delivery's key: the `partition_key` field's bytes.
    fn partition_key(&self) -> Option<&[u8]>;

    /// The delivery's attempt: the `attempt` field's.
    fn attempt(&self) -> Option<u64>;

    /// The attempt of a claimed `row` the struct could not decode, read alone the way the struct
    /// reads it; `None` without an `attempt` column, or where that column does not decode either.
    fn read_attempt(row: &DB::Row, queue: &'static Queue) -> Option<u64>;

    /// Binds one parameter of a default statement; `false` when the row has no value for it.
    ///
    /// # Errors
    ///
    /// The driver's encoding error.
    fn bind(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Self>,
    ) -> Result<bool, Error>;

    /// The lease the queue's claim takes now: "now" read once from the row's clock, which every
    /// time the claim binds starts from, and the lease from that instant.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`] where the queue holds no lease, the table reads the database's
    /// clock, or the table is in another form.
    fn lease(queue: &'static Queue, now: Now) -> Result<Leasing<Self::Token>, Error>;

    /// Claims up to `cx.limit` rows into `out`; in the lease form the claim takes `lease`.
    fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<Self::Token>>,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// The `ack` event; in the lease form `held` is the delivery's lease.
    fn ack<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Self::Id,
        held: Option<&'a Self::Token>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a;

    /// The `retry` event; says whether it wrote anything for the commit to keep.
    fn retry<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Self::Id,
        held: Option<&'a Self::Token>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a;

    /// The `retry_after` event.
    fn retry_after<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Self::Id,
        held: Option<&'a Self::Token>,
        delay: Duration,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a;

    /// The `discard` event.
    fn discard<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Self::Id,
        held: Option<&'a Self::Token>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a;

    /// The `dead_letter` event.
    fn dead_letter<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Self::Id,
        held: Option<&'a Self::Token>,
        destination: &'a str,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a;

    /// The `extend` event: writes `until` while the row holds `held`, and is [`Settled::Lost`]
    /// once it no longer does.
    fn extend<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Self::Id,
        held: &'a Self::Token,
        until: &'a Self::Token,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a;

    /// The `lock` event of the advisory lock form: tries the lock on `key` for the session of
    /// `conn`, without waiting; `true` when it took the lock.
    fn lock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;

    /// The `unlock` event of the advisory lock form: releases the lock on `key` the session of
    /// `conn` holds; `true` when it held it.
    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;

    /// Takes the candidate `id` of an advisory claim, whose lock the session of `conn` holds:
    /// counts its attempt and pushes the candidate's row onto `out` when the take found it still
    /// claimable; `false` when the row is gone or no longer claimable.
    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a Self::Id,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a;

    /// Selects the candidates of an advisory claim into `out`: up to `cx.limit` claimable rows in
    /// claim order, each id with its lock key, written over the last claim's.
    fn candidates<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        out: &'a mut Candidates<Self::Id>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;
}

/// Which events a row's service implements itself.
// One switch per event, read once per subscription at startup.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Shape {
    /// `custom(claim)`.
    pub custom_claim: bool,
    /// `custom(fetch)`.
    pub custom_fetch: bool,
    /// `custom(ack)`.
    pub custom_ack: bool,
    /// `custom(retry)`.
    pub custom_retry: bool,
    /// `custom(retry_after)`.
    pub custom_retry_after: bool,
    /// `custom(discard)`.
    pub custom_discard: bool,
    /// `custom(dead_letter)`.
    pub custom_dead_letter: bool,
    /// `custom(extend)`.
    pub custom_extend: bool,
    /// `custom(lock)`: the advisory lock form's lock is the service's, so the database, not the
    /// process, keeps the keys in work on every dialect.
    pub custom_lock: bool,
    /// `custom(unlock)`.
    pub custom_unlock: bool,
}

/// The event a statement serves, for binding and for messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Event {
    /// Claiming rows.
    Claim,
    /// Reading the rows of claimed ids.
    Fetch,
    /// `ack`.
    Ack,
    /// `retry()`.
    Retry,
    /// `retry_after(d)`.
    RetryAfter,
    /// `drop`.
    Discard,
    /// A spent delivery's move.
    DeadLetter,
    /// Leasing a row a claim selected: a claim that only selects stamps each row.
    Stamp,
    /// Extending a delivery's lease, or confirming it.
    Extend,
    /// Taking the advisory lock on a candidate's key.
    Lock,
    /// Releasing the advisory lock on a delivery's key.
    Unlock,
    /// Taking a candidate whose key the delivery's session holds.
    Take,
}

impl Event {
    /// The event's name, as statements and `custom(..)` spell it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Fetch => "fetch",
            Self::Ack => "ack",
            Self::Retry => "retry",
            Self::RetryAfter => "retry_after",
            Self::Discard => "discard",
            Self::DeadLetter => "dead_letter",
            Self::Stamp => "stamp",
            Self::Extend => "extend",
            Self::Lock => "lock",
            Self::Unlock => "unlock",
            Self::Take => "take",
        }
    }
}

/// One claimed row, a claimed id the fetch found no row for, or a row whose columns do not decode
/// into its struct.
#[derive(Debug)]
pub enum Claimed<Row: QueueRow> {
    /// The row.
    Row(Row),
    /// The id; its delivery reports that its row is gone, and the decode-failure policy settles
    /// it.
    Missing(Row::Id),
    /// The id of a row the struct could not decode, its attempt, and why; its delivery reports
    /// the driver's error, and the decode-failure policy settles it.
    Undecodable {
        /// The row's id, read alone.
        id: Row::Id,
        /// The row's attempt, read alone: the attempt cap spends such a row as any other.
        attempt: Option<u64>,
        /// The driver's error of decoding the whole row, as the delivery reports it.
        // Boxed: every delivery holds its `Claimed` inline, so the error would otherwise widen
        // the deliveries of rows that decode.
        error: Box<CodecError>,
    },
}

/// The driver's `error` of decoding a whole row, as a delivery of the row reports it.
pub(crate) fn undecodable(error: Error) -> CodecError {
    CodecError::Decode(Box::new(error))
}

/// Where a claim's select carries the id, read alone from a row the struct could not decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdAt {
    /// The first column: the generated select lists the id first.
    First,
    /// The column of this name: the select reads `*` where the struct flattens another.
    Named(&'static str),
}

impl IdAt {
    /// Where the statements built from `spec` carry the id.
    #[must_use]
    pub const fn of(spec: &TableSpec<'static>) -> Self {
        if spec.selects_all() {
            Self::Named(spec.id().name())
        } else {
            Self::First
        }
    }
}

/// What a settlement did, which decides between the commit and the rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Settled {
    /// A statement ran; the commit keeps it.
    Written,
    /// Nothing ran; the rollback releases the row.
    Untouched,
    /// The statement changed no row: in the lease form the row no longer holds the delivery's
    /// lease, and the settlement took no effect.
    Lost,
}

impl<Row: QueueRow> Claimed<Row> {
    /// The claimed id.
    pub fn id<DB: QueueDatabase>(&self) -> &Row::Id
    where
        Row: Events<DB>,
    {
        match self {
            Self::Row(row) => Row::id(row),
            Self::Missing(id) | Self::Undecodable { id, .. } => id,
        }
    }
}

const _: fn() = || {
    fn debug<T: Debug>() {}
    debug::<Shape>();
};

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Event, micros};

    #[test]
    fn delays_bind_in_microseconds_and_saturate() {
        assert_eq!(micros(Duration::from_millis(1500)), 1_500_000);
        assert_eq!(micros(Duration::MAX), i64::MAX);
        assert_eq!(Event::RetryAfter.name(), "retry_after");
    }

    #[test]
    fn the_advisory_events_name_themselves_as_messages_read_them() {
        let names = [Event::Lock, Event::Unlock, Event::Take].map(Event::name);
        assert_eq!(names, ["lock", "unlock", "take"]);
    }
}
