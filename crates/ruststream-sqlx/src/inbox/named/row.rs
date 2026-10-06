//! The row a by-name subscription reads by role: its parts, how its statements bind, and how it
//! is held to the types of the route's own struct.

use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::time::Duration;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Param;
use sqlx::{Column, Database, Error, FromRow, Row, ValueRef};
#[cfg(feature = "time")]
use time::OffsetDateTime;

use super::by_name::ByName;
use super::database::RoleColumns;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{
    self, Candidates, Claimed, Claiming, Event, Events, Leasing, Now, Settled, Settling, Shape,
    Values,
};
#[cfg(any(feature = "chrono", feature = "time"))]
use crate::inbox::kinds::{ClockKind, TimeKind};
use crate::inbox::kinds::{IntKind, Kinds};
use crate::inbox::queue::Queue;
#[cfg(any(feature = "chrono", feature = "time"))]
use crate::inbox::time::{QueueTime, SystemClock};
use crate::inbox::{PayloadRow, QueueRow};

/// A claimed row of a by-name subscription, read by the role aliases of `ClaimShape::Roles`, its
/// headers and times read and bound by the dialect `D`. Machinery: a by-name subscription
/// delivers it in place of the route's own row.
#[doc(hidden)]
pub struct NamedRow<D> {
    id: NamedId,
    payload: NamedBytes,
    /// Taken at decode: the alias `headers`, read as JSON.
    headers: HeaderMap,
    key: Option<NamedBytes>,
    attempt: Option<u64>,
    fits: Fits,
    dialect: PhantomData<fn() -> D>,
}

impl<D> fmt::Debug for NamedRow<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedRow")
            .field("id", &self.id)
            .field("payload", &self.payload)
            .field("headers", &self.headers)
            .field("key", &self.key)
            .field("attempt", &self.attempt)
            .field("fits", &self.fits)
            .finish()
    }
}

/// Which of the types a by-name subscription reads each role column holds, a bit per type in the
/// order it tries them: what the types the struct reads are held to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fits {
    id: u8,
    payload: u8,
    /// Every bit for a null, which an optional key reads whatever the column's type.
    key: u8,
    attempt: u8,
}

impl<D> NamedRow<D> {
    /// The role column whose type in the table does not hold the type the struct reads it as,
    /// and that type, if one does not.
    fn unfit(&self, kinds: Kinds) -> Option<(&'static str, &'static str)> {
        if self.fits.id & kinds.id.bit() == 0 {
            return Some(("id", kinds.id.name()));
        }
        if self.fits.payload & kinds.payload.bit() == 0 {
            return Some(("payload", kinds.payload.name()));
        }
        if let Some(key) = kinds.key
            && self.fits.key & key.bit() == 0
        {
            return Some(("partition_key", key.name()));
        }
        if let Some(attempt) = kinds.attempt
            && !attempt_fits(attempt, self.fits.attempt)
        {
            return Some(("attempt", attempt.name()));
        }
        None
    }

    /// The row's attempt where its column holds the type the struct reads it as, as the struct's
    /// own `FromRow` would read it.
    fn fitting_attempt(&self, kinds: Kinds) -> Option<u64> {
        let kind = kinds.attempt?;
        self.attempt
            .filter(|_| attempt_fits(kind, self.fits.attempt))
    }
}

/// Whether an attempt column that holds the types `held` holds `kind`.
const fn attempt_fits(kind: IntKind, held: u8) -> bool {
    held & kind.bit() != 0
}

/// Holds a claimed row to the types its struct reads, as the struct's own `FromRow` would: a row
/// whose role column does not hold its type goes to the decode-failure policy, and an id that
/// does not fails the claim.
fn hold_to<D: 'static>(kinds: Kinds, claimed: &mut Claimed<NamedRow<D>>) -> Result<(), Error> {
    let Claimed::Row(row) = claimed else {
        return Ok(());
    };
    let Some((alias, read_as)) = row.unfit(kinds) else {
        return Ok(());
    };
    let error = Error::ColumnDecode {
        index: format!("{alias:?}"),
        source: format!(
            "the struct reads this column as {read_as}, which its type in the table does not hold"
        )
        .into(),
    };
    if alias == "id" {
        return Err(error);
    }
    // The row is dropped in place of its delivery; its id moves into the one that replaces it.
    let attempt = row.fitting_attempt(kinds);
    let id = mem::replace(&mut row.id, NamedId::I64(0));
    *claimed = Claimed::Undecodable {
        id,
        attempt,
        error: Box::new(error),
    };
    Ok(())
}

/// The id of a [`NamedRow`], in the type its column holds. Machinery.
#[doc(hidden)]
#[derive(PartialEq, Eq, Hash)]
pub enum NamedId {
    /// A `SMALLINT` id.
    I16(i16),
    /// An `INTEGER` id.
    I32(i32),
    /// A `BIGINT` id.
    I64(i64),
    /// A text id.
    Text(String),
    /// A byte id.
    Bytes(Vec<u8>),
}

// Written out rather than derived: `clone_from` copies a text or byte id into the storage the id
// it replaces held, which a lease book does for every delivery it enters.
impl Clone for NamedId {
    fn clone(&self) -> Self {
        match self {
            Self::I16(id) => Self::I16(*id),
            Self::I32(id) => Self::I32(*id),
            Self::I64(id) => Self::I64(*id),
            Self::Text(id) => Self::Text(id.clone()),
            Self::Bytes(id) => Self::Bytes(id.clone()),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        match (self, source) {
            (Self::Text(kept), Self::Text(id)) => kept.clone_from(id),
            (Self::Bytes(kept), Self::Bytes(id)) => kept.clone_from(id),
            (kept, id) => *kept = id.clone(),
        }
    }
}

// Written out rather than derived: logs name a row by its id as the table holds it, the way they
// name a row of the route's own struct.
impl fmt::Debug for NamedId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::I16(id) => fmt::Debug::fmt(id, f),
            Self::I32(id) => fmt::Debug::fmt(id, f),
            Self::I64(id) => fmt::Debug::fmt(id, f),
            Self::Text(id) => fmt::Debug::fmt(id, f),
            Self::Bytes(id) => fmt::Debug::fmt(id, f),
        }
    }
}

/// A payload or a key of a [`NamedRow`], as bytes or as text. Machinery.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NamedBytes {
    /// A byte column.
    Bytes(Vec<u8>),
    /// A text column.
    Text(String),
}

impl NamedBytes {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Text(text) => text.as_bytes(),
        }
    }
}

/// A time a by-name subscription binds, in the type its column holds: the current time, a delayed
/// retry's, or in the lease form a lease's expiry.
///
/// A dialect binds it in [`ByName::bind_time`](crate::ByName::bind_time). Each variant comes with
/// the feature of its time type, so a match keeps a wildcard arm.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))] {
/// use ruststream_sqlx::NamedTime;
/// use sqlx::postgres::PgArguments;
/// use sqlx::{Arguments, Error};
///
/// // How a dialect over a driver that binds `chrono` times alone binds one.
/// fn bind_time(arguments: &mut PgArguments, time: NamedTime) -> Result<(), Error> {
///     match time {
///         NamedTime::Chrono(at) => arguments.add(at).map_err(Error::Encode),
///         _ => Err(Error::Configuration("this driver binds `chrono` times alone".into())),
///     }
/// }
///
/// let mut arguments = PgArguments::default();
/// bind_time(&mut arguments, NamedTime::Chrono(chrono::Utc::now()))?;
/// # }
/// # Ok::<(), sqlx::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NamedTime {
    /// A `chrono` column.
    #[cfg(feature = "chrono")]
    Chrono(DateTime<Utc>),
    /// A `time` column.
    #[cfg(feature = "time")]
    Time(OffsetDateTime),
}

/// Reads a claimed row by the role aliases of `ClaimShape::Roles`: one look at each column's name
/// and one decode per column, as a struct's own `FromRow` does; the dialect `D` reads the headers.
fn read<DB, D>(row: &DB::Row) -> Result<NamedRow<D>, Error>
where
    DB: RoleColumns,
    D: ByName<DB>,
{
    let mut id = None;
    let mut payload = None;
    let mut headers = HeaderMap::new();
    let mut key = None;
    let mut attempt = None;
    let mut fits = Fits {
        id: 0,
        payload: 0,
        key: 0,
        attempt: 0,
    };
    for column in row.columns() {
        let alias = column.name();
        let failed = |source| Error::ColumnDecode {
            index: format!("{alias:?}"),
            source,
        };
        let value = || DB::value(row, column.ordinal());
        match alias {
            "id" => {
                let (read, held) = DB::id(value()?).map_err(failed)?;
                (id, fits.id) = (Some(read), held);
            }
            "payload" => {
                let (read, held) = DB::bytes(value()?).map_err(failed)?;
                (payload, fits.payload) = (Some(read), held);
            }
            "partition_key" => {
                let value = value()?;
                if value.is_null() {
                    fits.key = u8::MAX;
                } else {
                    let (read, held) = DB::bytes(value).map_err(failed)?;
                    (key, fits.key) = (Some(read), held);
                }
            }
            "headers" => {
                let value = value()?;
                if !value.is_null() {
                    headers = D::headers(value).map_err(failed)?;
                }
            }
            "attempt" => {
                let (read, held) = DB::attempt(value()?).map_err(failed)?;
                (attempt, fits.attempt) = (Some(read), held);
            }
            _ => {}
        }
    }
    Ok(NamedRow {
        id: id.ok_or_else(|| Error::ColumnNotFound("id".to_owned()))?,
        payload: payload.ok_or_else(|| Error::ColumnNotFound("payload".to_owned()))?,
        headers,
        key,
        attempt,
        fits,
        dialect: PhantomData,
    })
}

impl<'r, R, D> FromRow<'r, R> for NamedRow<D>
where
    R: Row,
    R::Database: RoleColumns + Database<Row = R>,
    D: ByName<R::Database>,
{
    fn from_row(row: &'r R) -> Result<Self, Error> {
        read::<R::Database, D>(row)
    }
}

impl<D: 'static> QueueRow for NamedRow<D> {
    type Id = NamedId;
}

impl<D: 'static> PayloadRow for NamedRow<D> {
    fn payload(&self) -> &[u8] {
        self.payload.as_bytes()
    }
}

/// Binds now on the host's clock, or `delay` later, in the time `column` holds; `false` where
/// the table has no such column or reads the database's clock.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_now<DB, D>(
    arguments: &mut DB::Arguments,
    values: &Values<'_, DB, NamedRow<D>>,
    column: fn(Kinds) -> Option<TimeKind>,
    delay: Option<Duration>,
) -> Result<bool, Error>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    let Some(kinds) = values.queue.kinds else {
        return Ok(false);
    };
    let (Some(kind), ClockKind::System) = (column(kinds), kinds.clock) else {
        return Ok(false);
    };
    match kind {
        #[cfg(feature = "chrono")]
        TimeKind::Chrono => {
            bind_at::<DB, D, DateTime<Utc>>(arguments, values, delay, NamedTime::Chrono)
        }
        #[cfg(feature = "time")]
        TimeKind::Time => {
            bind_at::<DB, D, OffsetDateTime>(arguments, values, delay, NamedTime::Time)
        }
    }
}

/// Binds `lease`; `false` where the statement has no lease to bind.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_lease<DB, D>(arguments: &mut DB::Arguments, lease: Option<NamedTime>) -> Result<bool, Error>
where
    DB: Database,
    D: ByName<DB>,
{
    let Some(lease) = lease else {
        return Ok(false);
    };
    D::bind_time(arguments, lease)?;
    Ok(true)
}

/// Binds now, or `delay` later, as a `Time`.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_at<DB, D, Time>(
    arguments: &mut DB::Arguments,
    values: &Values<'_, DB, NamedRow<D>>,
    delay: Option<Duration>,
    named: fn(Time) -> NamedTime,
) -> Result<bool, Error>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
    Time: QueueTime,
{
    let now = engine::now::<SystemClock, Time, DB, NamedRow<D>>(values)?;
    let at = delay.map_or(now, |delay| now.after(delay));
    D::bind_time(arguments, named(at))?;
    Ok(true)
}

impl<DB, D> Events<DB> for NamedRow<D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    const SHAPE: Shape = Shape {
        custom_claim: false,
        custom_fetch: false,
        custom_ack: false,
        custom_retry: false,
        custom_retry_after: false,
        custom_discard: false,
        custom_dead_letter: false,
        custom_extend: false,
    };

    // The lease in the type of the route's `locked_until` column, which the queue's kinds name.
    type Token = NamedTime;

    fn kinds() -> Option<Kinds> {
        // The route's own row answers when the subscription opens.
        None
    }

    fn id(&self) -> &NamedId {
        &self.id
    }

    fn take_headers(&mut self) -> HeaderMap {
        mem::take(&mut self.headers)
    }

    fn unfit_header(headers: &HeaderMap) -> Option<&str> {
        // Nothing publishes through a by-name row: the route's own row writes the table.
        engine::first_header(headers)
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.key.as_ref().map(NamedBytes::as_bytes)
    }

    fn attempt(&self) -> Option<u64> {
        self.attempt
    }

    fn read_attempt(row: &DB::Row, queue: &'static Queue) -> Option<u64> {
        // The alias `ClaimShape::Roles` gives the column, held to the type the struct reads it as.
        let kind = queue.kinds?.attempt?;
        let column = row
            .columns()
            .iter()
            .find(|column| column.name() == "attempt")?;
        let (attempt, held) = DB::attempt(DB::value(row, column.ordinal()).ok()?).ok()?;
        attempt_fits(kind, held).then_some(attempt)
    }

    fn bind(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Self>,
    ) -> Result<bool, Error> {
        Ok(match (param, values.event) {
            (Param::Id, _) => match values.id {
                Some(id) => {
                    DB::bind_id(arguments, id)?;
                    true
                }
                None => false,
            },
            (Param::Group, _) => {
                DB::bind_str(arguments, values.queue.name)?;
                true
            }
            (Param::Limit, _) => {
                DB::bind_i64(arguments, values.limit)?;
                true
            }
            (Param::Destination, Event::DeadLetter) => {
                DB::bind_str(arguments, values.destination)?;
                true
            }
            (Param::Delay, Event::RetryAfter) => {
                DB::bind_i64(arguments, engine::micros(values.delay))?;
                true
            }
            (Param::Key, _) => match values.key {
                Some(key) => {
                    DB::bind_str(arguments, key)?;
                    true
                }
                None => false,
            },
            // A time binds only where a time type is enabled: without one no table has a time
            // column, and these fall through to `false`.
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Now, Event::Claim | Event::Take) => {
                bind_now::<DB, D>(arguments, values, |kinds| kinds.retry_after, None)?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Now, Event::Ack | Event::Discard) => {
                bind_now::<DB, D>(arguments, values, |kinds| kinds.processed_at, None)?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::RetryAfter, Event::RetryAfter) => bind_now::<DB, D>(
                arguments,
                values,
                |kinds| kinds.retry_after,
                Some(values.delay),
            )?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::LeaseNow, _) => {
                bind_lease::<DB, D>(arguments, values.leasing.map(|leasing| leasing.now))?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Lease, _) => bind_lease::<DB, D>(arguments, values.lease)?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Held, _) => bind_lease::<DB, D>(arguments, values.held)?,
            _ => false,
        })
    }

    fn lease(queue: &'static Queue, now: Now) -> Result<Leasing<NamedTime>, Error> {
        #[cfg(any(feature = "chrono", feature = "time"))]
        if let Some(Kinds {
            locked_until: Some(kind),
            clock: ClockKind::System,
            ..
        }) = queue.kinds
        {
            return Ok(match kind {
                #[cfg(feature = "chrono")]
                TimeKind::Chrono => {
                    engine::lease::<SystemClock, DateTime<Utc>>(queue, now)?.map(NamedTime::Chrono)
                }
                #[cfg(feature = "time")]
                TimeKind::Time => {
                    engine::lease::<SystemClock, OffsetDateTime>(queue, now)?.map(NamedTime::Time)
                }
            });
        }
        let _ = (queue, now);
        // A table whose kinds name no lease has none for the role columns to bind.
        Err(engine::unbound(Param::Lease, Event::Claim))
    }

    async fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<NamedTime>>,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> Result<(), Error> {
        let claimed = out.len();
        engine::claim_rows::<DB, Self>(conn, cx, lease, out).await?;
        if let Some(kinds) = cx.queue.kinds {
            for row in &mut out[claimed..] {
                hold_to(kinds, row)?;
            }
        }
        Ok(())
    }

    fn ack<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::ack::<DB, Self>(conn, cx, id, held)
    }

    fn retry<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::retry::<DB, Self>(conn, cx, id, held)
    }

    fn retry_after<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
        delay: Duration,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::retry_after::<DB, Self>(conn, cx, id, held, delay)
    }

    fn discard<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::discard::<DB, Self>(conn, cx, id, held)
    }

    fn dead_letter<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
        destination: &'a str,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::dead_letter::<DB, Self>(conn, cx, id, held, destination)
    }

    fn extend<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: &'a NamedTime,
        until: &'a NamedTime,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::extend::<DB, Self>(conn, cx, id, held, until)
    }

    fn lock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        engine::lock::<DB, Self>(conn, cx, key)
    }

    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        engine::unlock::<DB, Self>(conn, cx, key)
    }

    fn candidates<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        out: &'a mut Candidates<NamedId>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        engine::candidates::<DB, Self>(conn, cx, out)
    }

    async fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a NamedId,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> Result<bool, Error> {
        let taken = out.len();
        let found = engine::take::<DB, Self>(conn, cx, id, out).await?;
        if let Some(kinds) = cx.queue.kinds {
            for row in &mut out[taken..] {
                hold_to(kinds, row)?;
            }
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests;
