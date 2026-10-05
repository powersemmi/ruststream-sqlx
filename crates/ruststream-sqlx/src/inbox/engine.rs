//! The machinery `#[derive(Inbox)]` generates against: the statements a subscription prepared,
//! how a default event binds and runs one, and the hidden contract each row implements.
//!
//! Nothing here is named by a service; the derive and the broker are its only callers.

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Param;
use sqlx::{Arguments, Database, Decode, Encode, Error, FromRow, Type};

use super::InboxRow;
use super::database::QueueDatabase;
use super::time::{QueueTime, TimeSource};

/// A row's whole contract with the broker. Machinery; the derive implements it, a service never
/// names it.
pub trait Events<DB: QueueDatabase>: InboxRow + for<'r> FromRow<'r, DB::Row> + Unpin {
    /// Which events the service implements itself, and what the table can do natively.
    const SHAPE: Shape;

    /// The row's id.
    fn id(&self) -> &Self::Id;

    /// The delivery's headers: the `headers` field's, or none.
    fn headers(&self) -> HeaderMap;

    /// The delivery's key: the `partition_key` field's bytes.
    fn partition_key(&self) -> Option<&[u8]>;

    /// The delivery's attempt: the `attempt` field's.
    fn attempt(&self) -> Option<u64>;

    /// Binds one parameter of a default statement; `false` when the row has no value for it.
    ///
    /// # Errors
    ///
    /// The driver's encoding error.
    fn bind(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, Self>,
    ) -> Result<bool, Error>;

    /// Claims up to `cx.limit` rows into `out`.
    fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming<'a>,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// The `ack` event.
    fn ack<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling<'a>,
        id: &'a Self::Id,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// The `retry` event; says whether it wrote anything for the commit to keep.
    fn retry<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling<'a>,
        id: &'a Self::Id,
    ) -> impl Future<Output = Result<Released, Error>> + Send + 'a;

    /// The `retry_after` event.
    fn retry_after<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling<'a>,
        id: &'a Self::Id,
        delay: Duration,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// The `discard` event.
    fn discard<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling<'a>,
        id: &'a Self::Id,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;

    /// The `dead_letter` event.
    fn dead_letter<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling<'a>,
        id: &'a Self::Id,
        destination: &'a str,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a;
}

/// Which events a row's service implements itself, and whether the table holds `retry_after`.
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
    /// A field plays `retry_after`.
    pub retry_after_column: bool,
}

impl Shape {
    /// Whether a delayed retry is the database's own: the table holds `retry_after`, or the
    /// service implements the event.
    #[must_use]
    pub const fn native_retry_after(self) -> bool {
        self.custom_retry_after || self.retry_after_column
    }
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
        }
    }
}

/// The values one statement may bind, by meaning.
#[derive(Debug)]
pub struct Values<'a, Row: InboxRow> {
    /// The event the statement serves.
    pub event: Event,
    /// The subscription's name: its group, or the table's address.
    pub queue: &'a str,
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
    /// Where "now" comes from.
    pub now: Now,
}

impl<'a, Row: InboxRow> Values<'a, Row> {
    const fn claiming(cx: Claiming<'a>, event: Event) -> Self {
        Self {
            event,
            queue: cx.queue,
            limit: cx.limit,
            id: None,
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
        }
    }

    const fn settling(cx: Settling<'a>, event: Event, id: &'a Row::Id) -> Self {
        Self {
            event,
            queue: cx.queue,
            limit: 0,
            id: Some(id),
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
        }
    }
}

/// One claimed row, or a claimed id the fetch found no row for.
#[derive(Debug)]
pub enum Claimed<Row: InboxRow> {
    /// The row.
    Row(Row),
    /// The id; its delivery carries no payload and fails to decode.
    Missing(Row::Id),
}

/// What a settlement wrote, which decides between the commit and the rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Released {
    /// A statement ran; the commit keeps it.
    Written,
    /// Nothing ran; the rollback releases the row.
    Untouched,
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
    /// Claiming rows, or ids for a fetch of the service's own.
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
}

/// A claim in progress.
#[derive(Debug, Clone, Copy)]
pub struct Claiming<'a> {
    /// The subscription's name.
    pub queue: &'a str,
    /// The most rows to take.
    pub limit: i64,
    /// The subscription's statements.
    pub prepared: &'a Prepared,
    /// Where "now" comes from.
    pub now: Now,
}

/// A settlement in progress.
#[derive(Debug, Clone, Copy)]
pub struct Settling<'a> {
    /// The subscription's name.
    pub queue: &'a str,
    /// The subscription's statements.
    pub prepared: &'a Prepared,
    /// Where "now" comes from.
    pub now: Now,
}

/// Where "now" comes from for one statement: the row's [`TimeSource`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Now {
    _private: (),
}

impl Now {
    /// Now in `T`, from `Source`; `None` where the database reads its own clock.
    #[must_use]
    pub fn read<Source: TimeSource, T: QueueTime>(self) -> Option<T> {
        Source::now::<T>()
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

/// Now, in the column's time type.
///
/// # Errors
///
/// [`Error::Configuration`] where the table reads the database's clock and the statement still
/// binds a time.
pub fn now<Source: TimeSource, T: QueueTime>(now: Now, event: Event) -> Result<T, Error> {
    now.read::<Source, T>()
        .ok_or_else(|| unbound(Param::Now, event))
}

/// When a delayed retry comes back, in the column's time type.
///
/// # Errors
///
/// As [`now`].
pub fn later<Source: TimeSource, T: QueueTime>(
    now: Now,
    delay: Duration,
    event: Event,
) -> Result<T, Error> {
    Ok(self::now::<Source, T>(now, event)?.after(delay))
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

fn arguments<DB, Row>(statement: Stmt, values: &Values<'_, Row>) -> Result<DB::Arguments, Error>
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

/// The default claim: whole rows, in one statement.
///
/// # Errors
///
/// The database's error.
pub async fn claim_rows<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming<'_>,
    out: &mut Vec<Claimed<Row>>,
) -> Result<(), Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let statement = cx.prepared.claim.ok_or_else(|| unprepared(Event::Claim))?;
    let arguments = arguments::<DB, Row>(statement, &Values::claiming(*cx, Event::Claim))?;
    DB::fetch_rows(conn, statement.sql, arguments, out).await
}

/// The default claim of ids, for a fetch of the service's own.
///
/// # Errors
///
/// The database's error.
pub async fn claim_ids<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming<'_>,
) -> Result<Vec<Row::Id>, Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB> + Unpin,
{
    let statement = cx.prepared.claim.ok_or_else(|| unprepared(Event::Claim))?;
    let arguments = arguments::<DB, Row>(statement, &Values::claiming(*cx, Event::Claim))?;
    DB::fetch_ids(conn, statement.sql, arguments).await
}

/// The default fetch of the rows of `ids`, after a claim of the service's own.
///
/// # Errors
///
/// The database's error.
pub async fn fetch_by_ids<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming<'_>,
    ids: &[Row::Id],
) -> Result<Vec<Row>, Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let statement = cx.prepared.fetch.ok_or_else(|| unprepared(Event::Fetch))?;
    let values = Values {
        ids,
        ..Values::claiming(*cx, Event::Fetch)
    };
    let arguments = arguments::<DB, Row>(statement, &values)?;
    DB::fetch_all(conn, statement.sql, arguments).await
}

/// Pairs claimed ids with the rows a fetch returned, in claim order; an id with no row is
/// [`Claimed::Missing`], a row no id claimed is left alone.
pub fn match_claimed<DB, Row>(ids: Vec<Row::Id>, mut rows: Vec<Row>, out: &mut Vec<Claimed<Row>>)
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: PartialEq,
{
    for id in ids {
        match rows.iter().position(|row| Row::id(row) == &id) {
            Some(position) => out.push(Claimed::Row(rows.swap_remove(position))),
            None => out.push(Claimed::Missing(id)),
        }
    }
}

async fn run<DB, Row>(
    conn: &mut DB::Connection,
    statement: Option<Stmt>,
    values: Values<'_, Row>,
) -> Result<(), Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let statement = statement.ok_or_else(|| unprepared(values.event))?;
    let arguments = arguments::<DB, Row>(statement, &values)?;
    DB::execute(conn, statement.sql, arguments).await
}

/// The default `ack`.
///
/// # Errors
///
/// The database's error.
pub async fn ack<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling<'_>,
    id: &Row::Id,
) -> Result<(), Error> {
    run::<DB, Row>(conn, cx.prepared.ack, Values::settling(*cx, Event::Ack, id)).await
}

/// The default `retry()`.
///
/// # Errors
///
/// The database's error.
pub async fn retry<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling<'_>,
    id: &Row::Id,
) -> Result<Released, Error> {
    let Some(statement) = cx.prepared.retry else {
        return Ok(Released::Untouched);
    };
    run::<DB, Row>(
        conn,
        Some(statement),
        Values::settling(*cx, Event::Retry, id),
    )
    .await?;
    Ok(Released::Written)
}

/// The default `retry_after(d)`.
///
/// # Errors
///
/// The database's error.
pub async fn retry_after<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling<'_>,
    id: &Row::Id,
    delay: Duration,
) -> Result<(), Error> {
    let values = Values {
        delay,
        ..Values::settling(*cx, Event::RetryAfter, id)
    };
    run::<DB, Row>(conn, cx.prepared.retry_after, values).await
}

/// The default `drop`.
///
/// # Errors
///
/// The database's error.
pub async fn discard<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling<'_>,
    id: &Row::Id,
) -> Result<(), Error> {
    run::<DB, Row>(
        conn,
        cx.prepared.discard,
        Values::settling(*cx, Event::Discard, id),
    )
    .await
}

/// The default dead-letter move.
///
/// # Errors
///
/// The database's error.
pub async fn dead_letter<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling<'_>,
    id: &Row::Id,
    destination: &str,
) -> Result<(), Error> {
    let values = Values {
        destination,
        ..Values::settling(*cx, Event::DeadLetter, id)
    };
    run::<DB, Row>(conn, cx.prepared.dead_letter, values).await
}

impl<Row: InboxRow> Claimed<Row> {
    /// The claimed id.
    pub fn id<DB: QueueDatabase>(&self) -> &Row::Id
    where
        Row: Events<DB>,
    {
        match self {
            Self::Row(row) => Row::id(row),
            Self::Missing(id) => id,
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

    use super::{Event, Shape, micros};

    #[test]
    fn a_native_retry_after_comes_from_the_column_or_the_service() {
        assert!(!Shape::default().native_retry_after());
        let column = Shape {
            retry_after_column: true,
            ..Shape::default()
        };
        assert!(column.native_retry_after());
        let custom = Shape {
            custom_retry_after: true,
            ..Shape::default()
        };
        assert!(custom.native_retry_after());
    }

    #[test]
    fn delays_bind_in_microseconds_and_saturate() {
        assert_eq!(micros(Duration::from_millis(1500)), 1_500_000);
        assert_eq!(micros(Duration::MAX), i64::MAX);
        assert_eq!(Event::RetryAfter.name(), "retry_after");
    }
}
