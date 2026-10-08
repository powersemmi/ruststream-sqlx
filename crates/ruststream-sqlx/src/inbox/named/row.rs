//! The row a by-name subscription reads by role: its parts, how its statements bind, and how it
//! is held to the types of the route's own struct.

use std::fmt;
use std::marker::PhantomData;
use std::mem;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
use ruststream::HeaderMap;
use sqlx::{Column, Database, Error, FromRow, Row, ValueRef};
#[cfg(feature = "time")]
use time::OffsetDateTime;

use super::by_name::ByName;
use super::database::RoleColumns;
use super::kinds::{IntKind, Kinds};
use crate::inbox::engine::{Claimed, undecodable};
use crate::inbox::{PayloadLane, PayloadRow, QueueRow};

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
        error: Box::new(undecodable(error)),
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
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// # use ruststream::OutgoingMessage;
/// # use ruststream_sqlx::dialect::{self, ClaimShape, Dialect, Opening, RowLock, Statement, StatementError, TableName, TableSpec};
/// # use sqlx::PgConnection;
/// use ruststream::HeaderMap;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::{BuiltIn, ByName, NamedTime};
/// use serde::Deserialize;
/// use sqlx::error::BoxDynError;
/// use sqlx::postgres::{PgArguments, PgValueRef};
/// use sqlx::{Arguments, PgPool, Postgres};
///
/// /// A dialect of the service's own over Postgres, whose tables keep `chrono` times.
/// #[derive(Debug)]
/// pub struct Audited;
/// # impl Dialect for Audited {
/// #     fn name(&self) -> &'static str { "audited" }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { dialect::Postgres.quote_into(ident, out) }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { dialect::Postgres.placeholder_into(index, out) }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.fetch(spec) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.ack(spec) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { dialect::Postgres.retry(spec) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.retry_after(spec) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.discard(spec) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { dialect::Postgres.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.insert(spec) }
/// #     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> { dialect::Postgres.begin(opening) }
/// #     fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { dialect::Postgres.fifo_guard(spec) }
/// # }
/// # impl RowLock for Audited {
/// #     fn lock_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { dialect::Postgres.lock_claim(spec, shape) }
/// # }
///
/// impl ByName<Postgres> for Audited {
///     fn headers(value: PgValueRef<'_>) -> Result<HeaderMap, BoxDynError> {
///         <BuiltIn<Postgres> as ByName<Postgres>>::headers(value)
///     }
///
///     fn bind_time(arguments: &mut PgArguments, time: NamedTime) -> Result<(), sqlx::Error> {
///         match time {
///             NamedTime::Chrono(at) => arguments.add(at).map_err(sqlx::Error::Encode),
///             _ => Err(sqlx::Error::Configuration("these tables keep `chrono` times".into())),
///         }
///     }
/// }
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
/// # impl Publish<Postgres> for SendEmail {
/// #     async fn publish(_: &mut PgConnection, _: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> { Ok(()) }
/// # }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// #[subscriber("emails")]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     let broker = SqlxBroker::with_dialect(pool, Audited).route::<SendEmail>("emails");
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
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
    type Lane = PayloadLane;
}

impl<D: 'static> PayloadRow for NamedRow<D> {
    type Column = NamedBytes;

    fn payload(&self) -> &[u8] {
        self.payload.as_bytes()
    }
}

mod events;

#[cfg(test)]
mod tests {
    //! How a by-name row holds its columns to the struct's types.

    use std::marker::PhantomData;

    use ruststream::HeaderMap;
    use ruststream::codec::CodecError;
    use sqlx::Error;

    use super::{Fits, NamedBytes, NamedId, NamedRow, hold_to};
    use crate::inbox::engine::{Claimed, undecodable};
    use crate::inbox::named::kinds::{BytesKind, ClockKind, IdKind, IntKind, Kinds};

    /// A struct of an `i64` id, a byte payload, a text key and an `i16` attempt.
    pub(super) const KINDS: Kinds = Kinds {
        clock: ClockKind::System,
        id: IdKind::I64,
        payload: BytesKind::Bytes,
        key: Some(BytesKind::Text),
        attempt: Some(IntKind::I16),
        retry_after: None,
        processed_at: None,
        locked_until: None,
    };

    /// A row whose columns hold the types `KINDS` reads, and only those, for the dialect `D`.
    pub(super) fn row<D>(fits: Fits) -> NamedRow<D> {
        NamedRow {
            id: NamedId::I64(7),
            payload: NamedBytes::Bytes(b"{}".to_vec()),
            headers: HeaderMap::new(),
            key: Some(NamedBytes::Text("acme".to_owned())),
            attempt: Some(2),
            fits,
            dialect: PhantomData,
        }
    }

    pub(super) const FITTING: Fits = Fits {
        id: IdKind::I64.bit(),
        payload: BytesKind::Bytes.bit(),
        key: BytesKind::Text.bit(),
        attempt: IntKind::I16.bit(),
    };

    /// The driver's error a delivery of an undecodable row reports.
    fn driver_error(error: &CodecError) -> Option<&Error> {
        match error {
            CodecError::Decode(source) => source.downcast_ref::<Error>(),
            _ => None,
        }
    }

    /// The column a held row names as not holding its struct's type, and the attempt it keeps.
    fn unfit_column(claimed: &Claimed<NamedRow<()>>) -> Option<(String, Option<u64>)> {
        match claimed {
            Claimed::Undecodable { id, attempt, error } => {
                assert_eq!(id, &NamedId::I64(7), "the row keeps its id for the policy");
                match driver_error(error) {
                    Some(Error::ColumnDecode { index, .. }) => Some((index.clone(), *attempt)),
                    other => panic!("not a column's error: {other:?}"),
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
        let mut claimed = Claimed::Row(row::<()>(FITTING));
        hold_to(KINDS, &mut claimed)?;
        assert!(matches!(claimed, Claimed::Row(_)));
        // A null key fits whatever its column's type.
        let mut keyless = Claimed::Row(row::<()>(Fits {
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
            let mut claimed = Claimed::Row(row::<()>(fits));
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
        let mut claimed = Claimed::Row(row::<()>(Fits {
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
        let mut claimed = Claimed::<NamedRow<()>>::Undecodable {
            id: NamedId::I64(7),
            attempt: Some(3),
            error: Box::new(undecodable(Error::ColumnNotFound("payload".to_owned()))),
        };
        hold_to(KINDS, &mut claimed)?;
        assert!(matches!(
            &claimed,
            Claimed::Undecodable { attempt: Some(3), error, .. }
                if matches!(driver_error(error), Some(Error::ColumnNotFound(_)))
        ));
        Ok(())
    }
}
