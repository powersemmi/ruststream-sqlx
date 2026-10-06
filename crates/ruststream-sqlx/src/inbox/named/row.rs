//! The row a by-name subscription reads by role: its parts, how its statements bind, and how it
//! is held to the types of the route's own struct.

use std::fmt;
use std::mem;
use std::time::Duration;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Param;
use sqlx::{Column, Error, FromRow, Row, ValueRef};
#[cfg(feature = "time")]
use time::OffsetDateTime;

use super::database::NamedDatabase;
use crate::inbox::engine::{
    self, Claimed, Claiming, Event, Events, Leasing, Now, Settled, Settling, Shape, Values,
};
#[cfg(any(feature = "chrono", feature = "time"))]
use crate::inbox::kinds::{ClockKind, TimeKind};
use crate::inbox::kinds::{IntKind, Kinds};
use crate::inbox::queue::Queue;
#[cfg(any(feature = "chrono", feature = "time"))]
use crate::inbox::time::{QueueTime, SystemClock};
use crate::inbox::{PayloadRow, QueueRow};

/// A claimed row of a by-name subscription, read by the role aliases of `ClaimShape::Roles`.
/// Machinery: a by-name subscription delivers it in place of the route's own row.
#[doc(hidden)]
#[derive(Debug)]
pub struct NamedRow {
    id: NamedId,
    payload: NamedBytes,
    /// Taken at decode: the alias `headers`, read as JSON.
    headers: HeaderMap,
    key: Option<NamedBytes>,
    attempt: Option<u64>,
    fits: Fits,
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

impl NamedRow {
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
fn hold_to(kinds: Kinds, claimed: &mut Claimed<NamedRow>) -> Result<(), Error> {
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

/// A time a by-name subscription binds, in the type its column holds; in the lease form, the
/// lease a delivery holds. Machinery.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedTime {
    /// A `chrono` column.
    #[cfg(feature = "chrono")]
    Chrono(DateTime<Utc>),
    /// A `time` column.
    #[cfg(feature = "time")]
    Time(OffsetDateTime),
}

/// Reads a claimed row by the role aliases of `ClaimShape::Roles`: one look at each column's name
/// and one decode per column, as a struct's own `FromRow` does.
fn read<DB: NamedDatabase>(row: &DB::Row) -> Result<NamedRow, Error> {
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
                    headers = DB::headers(value).map_err(failed)?;
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
    })
}

impl<'r, R> FromRow<'r, R> for NamedRow
where
    R: Row,
    R::Database: NamedDatabase<Row = R>,
{
    fn from_row(row: &'r R) -> Result<Self, Error> {
        read::<R::Database>(row)
    }
}

impl QueueRow for NamedRow {
    type Id = NamedId;
}

impl PayloadRow for NamedRow {
    fn payload(&self) -> &[u8] {
        self.payload.as_bytes()
    }
}

/// Binds now on the host's clock, or `delay` later, in the time `column` holds; `false` where
/// the table has no such column or reads the database's clock.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_now<DB: NamedDatabase>(
    arguments: &mut DB::Arguments,
    values: &Values<'_, DB, NamedRow>,
    column: fn(Kinds) -> Option<TimeKind>,
    delay: Option<Duration>,
) -> Result<bool, Error> {
    let Some(kinds) = values.queue.kinds else {
        return Ok(false);
    };
    let (Some(kind), ClockKind::System) = (column(kinds), kinds.clock) else {
        return Ok(false);
    };
    match kind {
        #[cfg(feature = "chrono")]
        TimeKind::Chrono => {
            bind_at::<DB, DateTime<Utc>>(arguments, values, delay, NamedTime::Chrono)
        }
        #[cfg(feature = "time")]
        TimeKind::Time => bind_at::<DB, OffsetDateTime>(arguments, values, delay, NamedTime::Time),
    }
}

/// Binds `lease`; `false` where the statement has no lease to bind.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_lease<DB: NamedDatabase>(
    arguments: &mut DB::Arguments,
    lease: Option<NamedTime>,
) -> Result<bool, Error> {
    let Some(lease) = lease else {
        return Ok(false);
    };
    DB::bind_time(arguments, lease)?;
    Ok(true)
}

/// Binds now, or `delay` later, as a `Time`.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_at<DB: NamedDatabase, Time: QueueTime>(
    arguments: &mut DB::Arguments,
    values: &Values<'_, DB, NamedRow>,
    delay: Option<Duration>,
    named: fn(Time) -> NamedTime,
) -> Result<bool, Error> {
    let at = match delay {
        Some(delay) => engine::later::<SystemClock, Time>(values.now, delay, values.event)?,
        None => engine::now::<SystemClock, Time>(values.now, values.event)?,
    };
    DB::bind_time(arguments, named(at))?;
    Ok(true)
}

impl<DB: NamedDatabase> Events<DB> for NamedRow {
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
            // A time binds only where a time type is enabled: without one no table has a time
            // column, and these fall through to `false`.
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Now, Event::Claim) => {
                bind_now::<DB>(arguments, values, |kinds| kinds.retry_after, None)?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Now, Event::Ack | Event::Discard) => {
                bind_now::<DB>(arguments, values, |kinds| kinds.processed_at, None)?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::RetryAfter, Event::RetryAfter) => bind_now::<DB>(
                arguments,
                values,
                |kinds| kinds.retry_after,
                Some(values.delay),
            )?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::LeaseNow, _) => bind_lease::<DB>(arguments, values.lease_now)?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Lease, _) => bind_lease::<DB>(arguments, values.lease)?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Held, _) => bind_lease::<DB>(arguments, values.held)?,
            _ => false,
        })
    }

    fn lease(queue: &'static Queue, now: &mut Now) -> Result<Leasing<NamedTime>, Error> {
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
}

#[cfg(test)]
mod tests {
    use ruststream::HeaderMap;
    use sqlx::Error;

    use super::{Fits, NamedBytes, NamedId, NamedRow, hold_to};
    use crate::inbox::engine::Claimed;
    use crate::inbox::kinds::{BytesKind, ClockKind, IdKind, IntKind, Kinds};

    /// A struct of an `i64` id, a byte payload, a text key and an `i16` attempt.
    const KINDS: Kinds = Kinds {
        clock: ClockKind::System,
        id: IdKind::I64,
        payload: BytesKind::Bytes,
        key: Some(BytesKind::Text),
        attempt: Some(IntKind::I16),
        retry_after: None,
        processed_at: None,
        locked_until: None,
    };

    /// A row whose columns hold the types `KINDS` reads, and only those.
    fn row(fits: Fits) -> NamedRow {
        NamedRow {
            id: NamedId::I64(7),
            payload: NamedBytes::Bytes(b"{}".to_vec()),
            headers: HeaderMap::new(),
            key: Some(NamedBytes::Text("acme".to_owned())),
            attempt: Some(2),
            fits,
        }
    }

    const FITTING: Fits = Fits {
        id: IdKind::I64.bit(),
        payload: BytesKind::Bytes.bit(),
        key: BytesKind::Text.bit(),
        attempt: IntKind::I16.bit(),
    };

    /// The column a held row names as not holding its struct's type, and the attempt it keeps.
    fn unfit_column(claimed: &Claimed<NamedRow>) -> Option<(String, Option<u64>)> {
        match claimed {
            Claimed::Undecodable { id, attempt, error } => {
                assert_eq!(id, &NamedId::I64(7), "the row keeps its id for the policy");
                match &**error {
                    Error::ColumnDecode { index, .. } => Some((index.clone(), *attempt)),
                    other => panic!("not a column's error: {other}"),
                }
            }
            Claimed::Row(_) | Claimed::Missing(_) => None,
        }
    }

    #[test]
    fn logs_name_a_row_by_its_id_as_the_table_holds_it() {
        let ids = [
            NamedId::I16(1),
            NamedId::I32(2),
            NamedId::I64(3),
            NamedId::Text("job-4".to_owned()),
            NamedId::Bytes(vec![5]),
        ];
        let logged: Vec<String> = ids.iter().map(|id| format!("{id:?}")).collect();
        assert_eq!(logged, ["1", "2", "3", "\"job-4\"", "[5]"]);
    }

    #[test]
    fn an_id_is_copied_into_the_storage_of_the_one_it_replaces() {
        let mut kept = NamedId::Text("job-0001".to_owned());
        let storage = match &kept {
            NamedId::Text(text) => text.as_ptr(),
            other => panic!("not a text id: {other:?}"),
        };
        kept.clone_from(&NamedId::Text("job-0002".to_owned()));
        assert!(
            matches!(&kept, NamedId::Text(text) if text == "job-0002" && text.as_ptr() == storage),
            "{kept:?}"
        );
        let mut bytes = NamedId::Bytes(vec![1, 2]);
        bytes.clone_from(&NamedId::Bytes(vec![3, 4]));
        assert_eq!(bytes, NamedId::Bytes(vec![3, 4]));
        // An id of another type takes the new one's type.
        kept.clone_from(&NamedId::I64(7));
        assert_eq!(kept, NamedId::I64(7));
        assert_eq!(kept.clone(), NamedId::I64(7));
        let ids = [
            NamedId::I16(1),
            NamedId::I32(2),
            NamedId::Text("job-3".to_owned()),
            NamedId::Bytes(vec![4]),
        ];
        for id in ids {
            assert_eq!(id.clone(), id);
        }
    }

    #[test]
    fn a_row_whose_columns_hold_its_structs_types_stays_a_row() -> Result<(), Error> {
        let mut claimed = Claimed::Row(row(FITTING));
        hold_to(KINDS, &mut claimed)?;
        assert!(matches!(claimed, Claimed::Row(_)));
        // A null key fits whatever its column's type.
        let mut keyless = Claimed::Row(row(Fits {
            key: u8::MAX,
            ..FITTING
        }));
        hold_to(KINDS, &mut keyless)?;
        assert!(matches!(keyless, Claimed::Row(_)));
        Ok(())
    }

    #[test]
    fn a_column_that_does_not_hold_its_structs_type_sends_the_row_to_the_policy()
    -> Result<(), Error> {
        let text_payload = Fits {
            payload: BytesKind::Text.bit(),
            ..FITTING
        };
        let byte_key = Fits {
            key: BytesKind::Bytes.bit(),
            ..FITTING
        };
        let wide_attempt = Fits {
            attempt: IntKind::I64.bit(),
            ..FITTING
        };
        let text_payload_wide_attempt = Fits {
            attempt: IntKind::I64.bit(),
            ..text_payload
        };
        // The attempt goes along, so the cap spends the row, unless its own column is one the
        // struct does not read: the struct's `FromRow` reads no attempt from it either.
        for (fits, column, attempt) in [
            (text_payload, "\"payload\"", Some(2)),
            (byte_key, "\"partition_key\"", Some(2)),
            (wide_attempt, "\"attempt\"", None),
            (text_payload_wide_attempt, "\"payload\"", None),
        ] {
            let mut claimed = Claimed::Row(row(fits));
            hold_to(KINDS, &mut claimed)?;
            assert_eq!(
                unfit_column(&claimed),
                Some((column.to_owned(), attempt)),
                "{fits:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn an_id_that_does_not_hold_its_structs_type_fails_the_claim() {
        let mut claimed = Claimed::Row(row(Fits {
            id: IdKind::I32.bit(),
            ..FITTING
        }));
        let failed = hold_to(KINDS, &mut claimed);
        assert!(
            matches!(&failed, Err(Error::ColumnDecode { index, .. }) if index == "\"id\""),
            "{failed:?}"
        );
    }

    #[test]
    fn a_row_already_undecodable_is_left_to_the_policy() -> Result<(), Error> {
        let mut claimed = Claimed::<NamedRow>::Undecodable {
            id: NamedId::I64(7),
            attempt: Some(3),
            error: Box::new(Error::ColumnNotFound("payload".to_owned())),
        };
        hold_to(KINDS, &mut claimed)?;
        assert!(matches!(
            &claimed,
            Claimed::Undecodable { attempt: Some(3), error, .. }
                if matches!(**error, Error::ColumnNotFound(_))
        ));
        Ok(())
    }

    #[cfg(all(feature = "postgres", feature = "chrono"))]
    mod on_postgres {
        use std::time::Duration;

        use chrono::{TimeDelta, Utc};
        use ruststream::HeaderMap;
        use ruststream_sqlx_dialect::{Column, Form, Param, TableSpec};
        use sqlx::postgres::PgArguments;
        use sqlx::{Arguments, Postgres};

        use super::super::{NamedId, NamedRow, NamedTime};
        use super::{FITTING, KINDS, row};
        use crate::inbox::PayloadRow;
        use crate::inbox::engine::{Event, Events, IdAt, Leasing, Now, Prepared, Shape, Values};
        use crate::inbox::kinds::{ClockKind, Kinds, TimeKind};
        use crate::inbox::queue::Queue;
        use crate::inbox::time::QueueTime;

        const SPEC: TableSpec<'static> =
            TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
                .payload(Column::new("payload"));

        /// The subscription `emails` of a table read with `kinds`.
        fn queue(kinds: Option<Kinds>) -> &'static Queue {
            Box::leak(Box::new(Queue {
                name: "emails",
                table: "email_jobs",
                row: "SendEmail",
                spec: SPEC,
                id_at: IdAt::First,
                native_retry_after: true,
                kinds,
                prepared: Prepared::default(),
                begin_claim: None,
                counted_attempt: false,
                poll_interval: Duration::from_secs(1),
                lease: None,
                max_attempts: None,
                dead_letter: None,
            }))
        }

        /// The subscription `emails` of a lease table read with `kinds`, with a lease of thirty
        /// seconds.
        fn leased(kinds: Option<Kinds>) -> &'static Queue {
            Box::leak(Box::new(Queue {
                spec: TableSpec::new(
                    "email_jobs",
                    Column::new("job_id"),
                    Form::Lease(Column::new("locked_until")),
                )
                .payload(Column::new("payload")),
                lease: Some(Duration::from_secs(30)),
                ..*queue(kinds)
            }))
        }

        /// Binds `param` for `event`, and says whether it bound and how many values are bound.
        fn bind(
            param: Param,
            event: Event,
            queue: &'static Queue,
            id: Option<&NamedId>,
        ) -> Result<(bool, usize), sqlx::Error> {
            bind_leased(param, event, queue, id, None)
        }

        /// Binds `param` for `event` where the statement writes and matches `lease`, and finds a
        /// lease ended by it.
        fn bind_leased(
            param: Param,
            event: Event,
            queue: &'static Queue,
            id: Option<&NamedId>,
            lease: Option<NamedTime>,
        ) -> Result<(bool, usize), sqlx::Error> {
            let values = Values {
                event,
                queue,
                limit: 10,
                id,
                ids: &[],
                delay: Duration::from_secs(30),
                destination: "emails.dead",
                now: Now::default(),
                lease,
                lease_now: lease,
                held: lease,
            };
            let mut arguments = PgArguments::default();
            let bound = <NamedRow as Events<Postgres>>::bind(param, &mut arguments, &values)?;
            Ok((bound, arguments.len()))
        }

        #[test]
        fn a_lease_is_taken_and_bound_in_the_type_of_its_column() -> Result<(), sqlx::Error> {
            let queue = leased(Some(Kinds {
                locked_until: Some(TimeKind::Chrono),
                ..KINDS
            }));
            let before = Utc::now();
            let lease = <NamedRow as Events<Postgres>>::lease(queue, &mut Now::default())?;
            assert!(
                matches!(
                    lease,
                    Leasing { now: NamedTime::Chrono(now), expiry: NamedTime::Chrono(at) }
                        if at.timestamp_subsec_nanos() == 0
                            && at >= before + TimeDelta::seconds(30)
                            && at == now.after(Duration::from_secs(30)).rounded_up()
                ),
                "a whole second, a lease from the claim's now: {lease:?}"
            );
            for param in [Param::Lease, Param::Held, Param::LeaseNow] {
                assert_eq!(
                    bind_leased(param, Event::Extend, queue, None, Some(lease.expiry))?,
                    (true, 1),
                    "{param:?}"
                );
            }
            // A claim holds no lease yet, and a statement outside the lease form writes none.
            assert_eq!(
                bind_leased(Param::Held, Event::Claim, queue, None, None)?,
                (false, 0)
            );
            // A queue whose kinds name no lease cannot tell one.
            let refused =
                <NamedRow as Events<Postgres>>::lease(leased(Some(KINDS)), &mut Now::default());
            assert!(
                matches!(refused, Err(sqlx::Error::Configuration(_))),
                "{refused:?}"
            );
            assert_eq!(
                bind(Param::LeaseNow, Event::Claim, leased(Some(KINDS)), None)?,
                (false, 0)
            );
            Ok(())
        }

        fn timed(clock: ClockKind) -> Kinds {
            Kinds {
                clock,
                retry_after: Some(TimeKind::Chrono),
                processed_at: Some(TimeKind::Chrono),
                ..KINDS
            }
        }

        #[test]
        fn every_id_binds_as_its_column_holds_it() -> Result<(), sqlx::Error> {
            let queue = queue(Some(KINDS));
            for id in [
                NamedId::I16(1),
                NamedId::I32(2),
                NamedId::I64(3),
                NamedId::Text("job-4".to_owned()),
                NamedId::Bytes(vec![5]),
            ] {
                assert_eq!(bind(Param::Id, Event::Ack, queue, Some(&id))?, (true, 1));
            }
            assert_eq!(bind(Param::Id, Event::Ack, queue, None)?, (false, 0));
            Ok(())
        }

        #[test]
        fn the_queue_binds_its_name_limit_destination_and_delay() -> Result<(), sqlx::Error> {
            let queue = queue(Some(KINDS));
            assert_eq!(bind(Param::Group, Event::Claim, queue, None)?, (true, 1));
            assert_eq!(bind(Param::Limit, Event::Claim, queue, None)?, (true, 1));
            assert_eq!(
                bind(Param::Destination, Event::DeadLetter, queue, None)?,
                (true, 1)
            );
            assert_eq!(
                bind(Param::Destination, Event::Ack, queue, None)?,
                (false, 0)
            );
            assert_eq!(
                bind(Param::Delay, Event::RetryAfter, queue, None)?,
                (true, 1)
            );
            assert_eq!(bind(Param::Ids, Event::Fetch, queue, None)?, (false, 0));
            Ok(())
        }

        #[test]
        fn times_bind_on_the_hosts_clock_in_their_columns_type() -> Result<(), sqlx::Error> {
            let host = queue(Some(timed(ClockKind::System)));
            assert_eq!(bind(Param::Now, Event::Claim, host, None)?, (true, 1));
            assert_eq!(bind(Param::Now, Event::Ack, host, None)?, (true, 1));
            assert_eq!(bind(Param::Now, Event::Discard, host, None)?, (true, 1));
            assert_eq!(
                bind(Param::RetryAfter, Event::RetryAfter, host, None)?,
                (true, 1)
            );
            // The database's clock is read by the statement itself; a table without the column,
            // or a queue that never described its kinds, has no time to bind.
            let database = queue(Some(timed(ClockKind::Database)));
            assert_eq!(bind(Param::Now, Event::Claim, database, None)?, (false, 0));
            let untimed = queue(Some(KINDS));
            assert_eq!(bind(Param::Now, Event::Ack, untimed, None)?, (false, 0));
            assert_eq!(
                bind(Param::Now, Event::Claim, queue(None), None)?,
                (false, 0)
            );
            Ok(())
        }

        #[cfg(feature = "time")]
        #[test]
        fn time_crate_columns_bind_too() -> Result<(), sqlx::Error> {
            let kinds = Some(Kinds {
                retry_after: Some(TimeKind::Time),
                ..KINDS
            });
            assert_eq!(
                bind(Param::RetryAfter, Event::RetryAfter, queue(kinds), None)?,
                (true, 1)
            );
            Ok(())
        }

        #[test]
        fn a_named_row_lends_its_parts_and_leaves_every_event_to_the_crate() {
            let mut row = row(FITTING);
            assert_eq!(row.payload(), b"{}");
            assert_eq!(<NamedRow as Events<Postgres>>::id(&row), &NamedId::I64(7));
            assert_eq!(
                <NamedRow as Events<Postgres>>::partition_key(&row),
                Some(b"acme".as_slice())
            );
            assert_eq!(<NamedRow as Events<Postgres>>::attempt(&row), Some(2));
            let mut headers = HeaderMap::new();
            headers.insert("x-tenant", "acme");
            row.headers = headers.clone();
            // The delivery takes the headers once: they move out of the row.
            assert_eq!(
                <NamedRow as Events<Postgres>>::take_headers(&mut row),
                headers
            );
            assert!(<NamedRow as Events<Postgres>>::take_headers(&mut row).is_empty());
            assert_eq!(<NamedRow as Events<Postgres>>::SHAPE, Shape::default());
            assert_eq!(<NamedRow as Events<Postgres>>::kinds(), None);
            assert_eq!(
                <NamedRow as Events<Postgres>>::unfit_header(&headers),
                Some("x-tenant")
            );
        }
    }
}
