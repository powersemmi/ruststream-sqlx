//! The machinery `#[derive(Inbox)]` generates against: the statements a subscription prepared,
//! how a default event binds and runs one, and the hidden contract each row implements.
//!
//! Nothing here is named by a service; the derive and the broker are its only callers.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use ruststream::HeaderMap;
use ruststream_sqlx_dialect::{Param, Statement, TableSpec};
use sqlx::{Arguments, Database, Decode, Encode, Error, FromRow, Type};

use super::QueueRow;
use super::columns::AttemptColumn;
use super::database::QueueDatabase;
use super::kinds::Kinds;
use super::queue::Queue;
#[cfg(feature = "testing")]
use super::testing::TestClock;
use super::time::{QueueTime, TimeSource};

mod advisory;
mod claim;
mod settle;

pub use advisory::{Candidates, lock, match_taken, take, take_id, unlock};
pub use claim::{claim_ids, claim_rows, fetch_by_ids, match_claimed, match_rows};
pub(crate) use claim::{stamp, take_group};
pub use settle::{ack, dead_letter, discard, extend, retry, retry_after};

/// A row's whole contract with the broker. Machinery; the derive implements it, a service never
/// names it.
pub trait Events<DB: QueueDatabase>: QueueRow + for<'r> FromRow<'r, DB::Row> + Unpin {
    /// Which events the service implements itself.
    const SHAPE: Shape;

    /// The lease a delivery holds: the expiry its claim wrote into `locked_until`, which its
    /// settlements match; `()` for a table in another form.
    type Token: Copy + Debug + Send + Sync + 'static;

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

/// The values one statement may bind, by meaning.
#[derive(Debug)]
pub struct Values<'a, DB: QueueDatabase, Row: Events<DB>> {
    /// The event the statement serves.
    pub event: Event,
    /// The subscription: its name (its group, or the table's address) and what it knows of the
    /// table.
    pub queue: &'static Queue,
    /// The most rows a claim takes.
    pub limit: i64,
    /// The row a settlement settles.
    pub id: Option<&'a Row::Id>,
    /// The ids a fetch reads.
    pub ids: &'a [Row::Id],
    /// A delayed retry's delay.
    pub delay: Duration,
    /// A dead letter's destination.
    pub destination: &'a str,
    /// Where "now" comes from where the statement takes no lease.
    pub now: Now,
    /// The lease a claim, a stamp or an extension writes.
    pub lease: Option<Row::Token>,
    /// The lease a claim and its stamps take: every time they bind starts from its instant.
    pub leasing: Option<&'a Leasing<Row::Token>>,
    /// The lease a settlement or an extension matches: the delivery's ownership token.
    pub held: Option<Row::Token>,
    /// The lock key the advisory lock form's lock or unlock names.
    pub key: Option<&'a str>,
}

impl<'a, DB: QueueDatabase, Row: Events<DB>> Values<'a, DB, Row> {
    const fn claiming(
        cx: Claiming,
        event: Event,
        leasing: Option<&'a Leasing<Row::Token>>,
    ) -> Self {
        Self {
            event,
            queue: cx.queue,
            limit: cx.limit,
            id: None,
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
            lease: match leasing {
                Some(leasing) => Some(leasing.expiry),
                None => None,
            },
            leasing,
            held: None,
            key: None,
        }
    }

    const fn settling(
        cx: Settling,
        event: Event,
        id: &'a Row::Id,
        held: Option<Row::Token>,
    ) -> Self {
        Self {
            event,
            queue: cx.queue,
            limit: 0,
            id: Some(id),
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
            lease: None,
            leasing: None,
            held,
            key: None,
        }
    }

    /// The values of an unlock of `key`: a settlement that names no row.
    const fn unlocking(cx: Settling, key: &'a str) -> Self {
        Self {
            event: Event::Unlock,
            queue: cx.queue,
            limit: 0,
            id: None,
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
            lease: None,
            leasing: None,
            held: None,
            key: Some(key),
        }
    }
}

/// One claimed row, a claimed id the fetch found no row for, or a row whose columns do not decode
/// into its struct.
#[derive(Debug)]
pub enum Claimed<Row: QueueRow> {
    /// The row.
    Row(Row),
    /// The id; its delivery carries no payload and fails to decode.
    Missing(Row::Id),
    /// The id of a row the struct could not decode, its attempt, and why; its delivery carries
    /// no payload and fails to decode.
    Undecodable {
        /// The row's id, read alone.
        id: Row::Id,
        /// The row's attempt, read alone: the attempt cap spends such a row as any other.
        attempt: Option<u64>,
        /// The error of decoding the whole row.
        // Boxed: every delivery holds its `Claimed` inline, so the error would otherwise widen
        // the deliveries of rows that decode.
        error: Box<Error>,
    },
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

/// One statement a subscription prepared: its text and the parameters it binds, interned for the
/// life of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stmt {
    /// The text.
    pub sql: &'static str,
    /// The parameters, in placeholder order.
    pub params: &'static [Param],
}

/// The statements of one subscription: one per event the crate runs by default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Prepared {
    /// Taking the subscription's group for a claim's transaction, in a table whose groups keep
    /// their order and on a dialect that takes one: the claim runs only once it answers nonzero.
    pub fifo_guard: Option<Stmt>,
    /// Claiming rows, or ids for a fetch of the service's own; in the advisory lock form, the
    /// candidates with their lock keys.
    pub claim: Option<Stmt>,
    /// Reading the rows of ids a claim of the service's own returned.
    pub fetch: Option<Stmt>,
    /// `ack`.
    pub ack: Option<Stmt>,
    /// `retry()`, where it writes anything.
    pub retry: Option<Stmt>,
    /// `retry_after(d)`, with a `retry_after` field.
    pub retry_after: Option<Stmt>,
    /// `drop`.
    pub discard: Option<Stmt>,
    /// The declared dead-letter move.
    pub dead_letter: Option<Stmt>,
    /// The second statement of a dead-letter move the dialect splits in two; the transaction of
    /// the first runs it.
    pub dead_letter_then: Option<Stmt>,
    /// Extending a delivery's lease, in the lease form.
    pub extend: Option<Stmt>,
    /// Leasing one claimed row, where the claim only selects.
    pub stamp: Option<Stmt>,
    /// Taking the advisory lock on a candidate's key for the delivery's session, in the advisory
    /// lock form, where the dialect's database keeps the locks.
    pub lock: Option<Stmt>,
    /// Releasing that lock.
    pub unlock: Option<Stmt>,
    /// Taking a candidate whose key the session holds, in the advisory lock form: it counts the
    /// attempt and reads the row while the row is still claimable.
    pub take: Option<Stmt>,
    /// The read of a take the dialect splits in two; it runs only once the first statement
    /// changed the row.
    pub take_then: Option<Stmt>,
    /// Whether the claim only selects its rows, so the claim's transaction stamps each one: a
    /// claim of the service's own, or a dialect whose lease claim writes no lease.
    pub stamps: bool,
}

/// A claim in progress.
#[derive(Debug, Clone, Copy)]
pub struct Claiming {
    /// The subscription: its name, its statements and what it knows of the table.
    pub queue: &'static Queue,
    /// The most rows to take.
    pub limit: i64,
    /// Where "now" comes from for a claim that takes no lease.
    pub now: Now,
}

/// The lease a claim takes, from one reading of the clock: that instant, and the instant and the
/// lease's end in the lease column's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Leasing<Token> {
    /// The instant the claim read: every time the claim binds starts from it.
    pub at: SystemTime,
    /// The same instant in the lease column's type: a row whose lease ended by then is free.
    pub now: Token,
    /// The lease's end, which the claim writes and its deliveries hold: the queue's lease after
    /// `now`, rounded up to a whole second.
    pub expiry: Token,
}

// A by-name lease turns its times into the time type its column names, and a column names one only
// with a time feature on.
#[cfg(any(feature = "chrono", feature = "time"))]
impl<Token> Leasing<Token> {
    /// The same lease, each of its times in the lease column's type turned by `into`.
    pub(crate) fn map<Other>(self, into: impl Fn(Token) -> Other) -> Leasing<Other> {
        Leasing {
            at: self.at,
            now: into(self.now),
            expiry: into(self.expiry),
        }
    }
}

/// A settlement in progress.
#[derive(Debug, Clone, Copy)]
pub struct Settling {
    /// The subscription: its name, its statements and what it knows of the table.
    pub queue: &'static Queue,
    /// Where "now" comes from.
    pub now: Now,
}

/// Where "now" comes from for one statement: the row's [`TimeSource`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Now {
    /// The clock of an in-process connection, which stands in for a host clock.
    #[cfg(feature = "testing")]
    test: Option<TestClock>,
    _private: (),
}

impl Now {
    /// Now as the host reads it, from `Source`; `None` where the database reads its own clock.
    fn instant<Source: TimeSource>(self) -> Option<SystemTime> {
        #[cfg(feature = "testing")]
        if let Some(clock) = self.test
            && !Source::DATABASE
        {
            return Some(clock.now());
        }
        Source::now()
    }

    /// "Now" of an in-process connection: `clock` instead of a host clock, where it is set.
    #[cfg(feature = "testing")]
    pub(crate) const fn test(clock: Option<TestClock>) -> Self {
        Self {
            test: clock,
            _private: (),
        }
    }
}

/// Phrases a bound over a type that names no generic parameter so that it does.
///
/// The derive's bounds on a field's type then rule an impl out instead of failing the build where
/// the field's type does not fit. Machinery.
pub trait Via<Marker> {
    /// The type itself.
    type Is: ?Sized;
}

impl<Marker, T: ?Sized> Via<Marker> for T {
    type Is = T;
}

/// The time a time column binds in `DB`.
///
/// Machinery: the derive bounds a time field's type by it, which names the database, so a field
/// the database cannot bind rules the impl out instead of failing the build.
pub trait TimeFor<DB: Database> {
    /// The time the column holds.
    type Time: QueueTime + for<'q> Encode<'q, DB> + Type<DB>;
}

impl<DB, C> TimeFor<DB> for C
where
    DB: Database,
    C: super::time::TimeColumn,
    C::Time: for<'q> Encode<'q, DB> + Type<DB>,
{
    type Time = C::Time;
}

/// The first of `headers`: what a row without a `headers` field cannot hold.
#[must_use]
pub fn first_header(headers: &HeaderMap) -> Option<&str> {
    headers.iter().next().map(|(name, _)| name)
}

/// The attempt in the column `name` of a claimed `row` the struct could not decode.
///
/// It is read as the struct reads it: decoded as `Decoded`, then converted into the field's
/// `Attempt`, the same type where the field names no `try_from`. `None` where that column does not
/// decode either.
pub fn attempt_in<DB, Decoded, Attempt>(row: &DB::Row, name: &str) -> Option<u64>
where
    DB: QueueDatabase,
    Decoded: for<'r> Decode<'r, DB> + Type<DB>,
    Attempt: AttemptColumn + TryFrom<Decoded>,
{
    let decoded = DB::column::<Decoded>(row, name).ok()?;
    Attempt::try_from(decoded)
        .ok()
        .map(|attempt| attempt.attempt())
}

/// Binds `value`.
///
/// # Errors
///
/// The driver's encoding error.
pub fn put<'q, DB, V>(arguments: &mut DB::Arguments, value: V) -> Result<(), Error>
where
    DB: Database,
    V: Encode<'q, DB> + Type<DB>,
{
    arguments.add(value).map_err(Error::Encode)
}

/// Now for one statement, in the column's time type: the instant its claim took a lease at, or a
/// reading of `Source`.
///
/// # Errors
///
/// [`Error::Configuration`] where the table reads the database's clock and the statement still
/// binds a time.
pub fn now<Source, T, DB, Row>(values: &Values<'_, DB, Row>) -> Result<T, Error>
where
    Source: TimeSource,
    T: QueueTime,
    DB: QueueDatabase,
    Row: Events<DB>,
{
    // A claim takes a lease on the host's clock only, so the instant it read is the one to bind.
    let at = values.leasing.map_or_else(
        || values.now.instant::<Source>(),
        |leasing| Some(leasing.at),
    );
    at.map(T::from_system)
        .ok_or_else(|| unbound(Param::Now, values.event))
}

/// When a delayed retry comes back, in the column's time type.
///
/// # Errors
///
/// As [`now`].
pub fn later<Source, T, DB, Row>(values: &Values<'_, DB, Row>) -> Result<T, Error>
where
    Source: TimeSource,
    T: QueueTime,
    DB: QueueDatabase,
    Row: Events<DB>,
{
    Ok(now::<Source, T, DB, Row>(values)?.after(values.delay))
}

/// The lease a claim of `queue` takes now: "now" read once from `Source`, which every time the
/// claim binds starts from, and the lease from that instant, in the lease column's time type.
///
/// # Errors
///
/// [`Error::Configuration`] where the queue holds no lease or the table reads the database's
/// clock.
pub fn lease<Source: TimeSource, T: QueueTime>(
    queue: &Queue,
    now: Now,
) -> Result<Leasing<T>, Error> {
    let lease = queue
        .lease
        .ok_or_else(|| unbound(Param::Lease, Event::Claim))?;
    let at = now
        .instant::<Source>()
        .ok_or_else(|| unbound(Param::Now, Event::Claim))?;
    let now = T::from_system(at);
    Ok(Leasing {
        at,
        now,
        expiry: now.after(lease).rounded_up(),
    })
}

/// The lease of a table in another form: it has none to take.
///
/// # Errors
///
/// Always [`Error::Configuration`].
pub fn no_lease<Token>() -> Result<Leasing<Token>, Error> {
    Err(unbound(Param::Lease, Event::Claim))
}

/// A delay in whole microseconds, saturating.
#[must_use]
pub fn micros(delay: Duration) -> i64 {
    i64::try_from(delay.as_micros()).unwrap_or(i64::MAX)
}

/// The error of a statement that binds a value its row has none of: a dialect and a table that
/// disagree.
pub(crate) fn unbound(param: Param, event: Event) -> Error {
    Error::Configuration(
        format!(
            "the {} statement binds {param:?}, which the table has no value for",
            event.name()
        )
        .into(),
    )
}

fn unprepared(event: Event) -> Error {
    Error::Configuration(format!("the subscription prepared no {} statement", event.name()).into())
}

fn arguments<DB, Row>(statement: Stmt, values: &Values<'_, DB, Row>) -> Result<DB::Arguments, Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let mut arguments = DB::Arguments::default();
    for &param in statement.params {
        if !Row::bind(param, &mut arguments, values)? {
            return Err(unbound(param, values.event));
        }
    }
    Ok(arguments)
}

/// Runs `statement` with `values` and returns the rows it changed.
async fn run<DB, Row>(
    conn: &mut DB::Connection,
    statement: Option<Stmt>,
    values: Values<'_, DB, Row>,
) -> Result<u64, Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let statement = statement.ok_or_else(|| unprepared(values.event))?;
    let arguments = arguments::<DB, Row>(statement, &values)?;
    DB::execute(conn, statement.sql, arguments).await
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

/// Statements and names for the life of the process, shared by every subscription that builds the
/// same one. Interned rather than reference-counted: a delivery reaches its statements with no
/// atomic per message, and a process builds few distinct statements, one set per table and
/// declaration.
static STATEMENTS: LazyLock<Mutex<HashMap<&'static str, &'static [Param]>>> =
    LazyLock::new(Mutex::default);
static NAMES: LazyLock<Mutex<HashSet<&'static str>>> = LazyLock::new(Mutex::default);

/// `statement` for the life of the process.
pub(crate) fn intern(statement: &Statement) -> Stmt {
    let mut interned = STATEMENTS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some((&sql, &params)) = interned.get_key_value(statement.sql()) {
        return Stmt { sql, params };
    }
    let sql: &'static str = Box::leak(statement.sql().into());
    let params: &'static [Param] = Box::leak(statement.params().into());
    interned.insert(sql, params);
    drop(interned);
    Stmt { sql, params }
}

/// `name` for the life of the process.
pub(crate) fn intern_name(name: &str) -> &'static str {
    let mut interned = NAMES.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(&name) = interned.get(name) {
        return name;
    }
    let name: &'static str = Box::leak(name.into());
    interned.insert(name);
    drop(interned);
    name
}

impl Prepared {
    /// Every statement it holds, for the startup check.
    pub(crate) fn statements(&self) -> impl Iterator<Item = Stmt> {
        [
            self.fifo_guard,
            self.claim,
            self.fetch,
            self.ack,
            self.retry,
            self.retry_after,
            self.discard,
            self.dead_letter,
            self.dead_letter_then,
            self.extend,
            self.stamp,
            self.lock,
            self.unlock,
            self.take,
            self.take_then,
        ]
        .into_iter()
        .flatten()
    }
}

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
