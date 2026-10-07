//! What the derive's generated code calls, and how a default statement binds and runs.

use std::time::Duration;

use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Param;
use sqlx::{Arguments, Database, Decode, Encode, Error, Type};

use super::{Event, Events, Leasing, Now, Stmt, Values};
use crate::inbox::columns::AttemptColumn;
use crate::inbox::database::QueueDatabase;
use crate::inbox::queue::Queue;
use crate::inbox::time::{QueueTime, TimeSource};

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

pub(crate) fn unprepared(event: Event) -> Error {
    Error::Configuration(format!("the subscription prepared no {} statement", event.name()).into())
}

pub(crate) fn arguments<DB, Row>(
    statement: Stmt,
    values: &Values<'_, DB, Row>,
) -> Result<DB::Arguments, Error>
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
pub(super) async fn run<DB, Row>(
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
