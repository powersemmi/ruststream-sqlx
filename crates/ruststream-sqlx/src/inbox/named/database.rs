//! What a database gives a by-name subscription: the columns every driver reads, and the JSON
//! headers and times each database with a built-in dialect decodes and binds.

#[cfg(feature = "json")]
use std::collections::BTreeMap;

use ruststream::HeaderMap;
use sqlx::error::BoxDynError;
#[cfg(feature = "json")]
use sqlx::types::Json;
use sqlx::{
    Arguments, ColumnIndex, Database, Decode, Encode, Error, Row, Type, TypeInfo, ValueRef,
};

use super::row::{NamedBytes, NamedId, NamedTime};
#[cfg(feature = "json")]
use crate::inbox::HeaderColumn;
use crate::inbox::database::QueueDatabase;

/// The integer, text and byte columns a by-name subscription reads and binds: what every sqlx
/// driver decodes and encodes. Machinery; every such database implements it.
#[doc(hidden)]
pub trait RoleColumns: Database {
    /// The value of the column at `ordinal`.
    ///
    /// # Errors
    ///
    /// The driver's error for an ordinal past the row's columns.
    fn value(row: &Self::Row, ordinal: usize) -> Result<Self::ValueRef<'_>, Error>;

    /// An id, as the first of `i64`, `i32`, `i16`, `String` and `Vec<u8>` its column holds, and
    /// which of them the column holds, a bit each in that order.
    ///
    /// # Errors
    ///
    /// A column of none of those types, or the driver's decoding error.
    fn id(value: Self::ValueRef<'_>) -> Result<(NamedId, u8), BoxDynError>;

    /// Whether a column of type `ty` holds an id [`RoleColumns::id`] reads.
    fn holds_id(ty: &Self::TypeInfo) -> bool;

    /// The type an id is declared as.
    fn id_type() -> Self::TypeInfo;

    /// Bytes or text, the first of `Vec<u8>` and `String` the column holds, and which of them it
    /// holds, a bit each in that order.
    ///
    /// # Errors
    ///
    /// A column of neither, or the driver's decoding error.
    fn bytes(value: Self::ValueRef<'_>) -> Result<(NamedBytes, u8), BoxDynError>;

    /// An attempt, from the first of `i64`, `i32` and `i16` its column holds, and which of them
    /// it holds, a bit each in that order; below zero reads as zero.
    ///
    /// # Errors
    ///
    /// A column of none of those types, or the driver's decoding error.
    fn attempt(value: Self::ValueRef<'_>) -> Result<(u64, u8), BoxDynError>;

    /// Binds `id`.
    ///
    /// # Errors
    ///
    /// The driver's encoding error.
    fn bind_id(arguments: &mut Self::Arguments, id: &NamedId) -> Result<(), Error>;
}

impl<DB> RoleColumns for DB
where
    DB: Database,
    usize: ColumnIndex<DB::Row>,
    i16: for<'r> Decode<'r, DB> + for<'q> Encode<'q, DB> + Type<DB>,
    i32: for<'r> Decode<'r, DB> + for<'q> Encode<'q, DB> + Type<DB>,
    i64: for<'r> Decode<'r, DB> + for<'q> Encode<'q, DB> + Type<DB>,
    String: for<'r> Decode<'r, DB> + Type<DB>,
    Vec<u8>: for<'r> Decode<'r, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    fn value(row: &Self::Row, ordinal: usize) -> Result<Self::ValueRef<'_>, Error> {
        row.try_get_raw(ordinal)
    }

    fn id(value: Self::ValueRef<'_>) -> Result<(NamedId, u8), BoxDynError> {
        let held = {
            let ty = value.type_info();
            let held = bits([
                <i64 as Type<DB>>::compatible(&ty),
                <i32 as Type<DB>>::compatible(&ty),
                <i16 as Type<DB>>::compatible(&ty),
                <String as Type<DB>>::compatible(&ty),
                <Vec<u8> as Type<DB>>::compatible(&ty),
            ]);
            if held == 0 {
                return Err(unread(&*ty, "i64, i32, i16, String or Vec<u8>"));
            }
            held
        };
        let id = match held.trailing_zeros() {
            0 => NamedId::I64(<i64 as Decode<'_, DB>>::decode(value)?),
            1 => NamedId::I32(<i32 as Decode<'_, DB>>::decode(value)?),
            2 => NamedId::I16(<i16 as Decode<'_, DB>>::decode(value)?),
            3 => NamedId::Text(<String as Decode<'_, DB>>::decode(value)?),
            _ => NamedId::Bytes(<Vec<u8> as Decode<'_, DB>>::decode(value)?),
        };
        Ok((id, held))
    }

    fn holds_id(ty: &Self::TypeInfo) -> bool {
        <i64 as Type<DB>>::compatible(ty)
            || <i32 as Type<DB>>::compatible(ty)
            || <i16 as Type<DB>>::compatible(ty)
            || <String as Type<DB>>::compatible(ty)
            || <Vec<u8> as Type<DB>>::compatible(ty)
    }

    fn id_type() -> Self::TypeInfo {
        <i64 as Type<DB>>::type_info()
    }

    fn bytes(value: Self::ValueRef<'_>) -> Result<(NamedBytes, u8), BoxDynError> {
        let held = {
            let ty = value.type_info();
            let held = bits([
                <Vec<u8> as Type<DB>>::compatible(&ty),
                <String as Type<DB>>::compatible(&ty),
            ]);
            if held == 0 {
                return Err(unread(&*ty, "Vec<u8> or String"));
            }
            held
        };
        let bytes = if held.trailing_zeros() == 0 {
            NamedBytes::Bytes(<Vec<u8> as Decode<'_, DB>>::decode(value)?)
        } else {
            NamedBytes::Text(<String as Decode<'_, DB>>::decode(value)?)
        };
        Ok((bytes, held))
    }

    fn attempt(value: Self::ValueRef<'_>) -> Result<(u64, u8), BoxDynError> {
        let held = {
            let ty = value.type_info();
            let held = bits([
                <i64 as Type<DB>>::compatible(&ty),
                <i32 as Type<DB>>::compatible(&ty),
                <i16 as Type<DB>>::compatible(&ty),
            ]);
            if held == 0 {
                return Err(unread(&*ty, "i64, i32 or i16"));
            }
            held
        };
        let attempt = match held.trailing_zeros() {
            0 => <i64 as Decode<'_, DB>>::decode(value)?,
            1 => i64::from(<i32 as Decode<'_, DB>>::decode(value)?),
            _ => i64::from(<i16 as Decode<'_, DB>>::decode(value)?),
        };
        Ok((u64::try_from(attempt).unwrap_or(0), held))
    }

    fn bind_id(arguments: &mut Self::Arguments, id: &NamedId) -> Result<(), Error> {
        match id {
            NamedId::I16(id) => arguments.add(*id),
            NamedId::I32(id) => arguments.add(*id),
            NamedId::I64(id) => arguments.add(*id),
            NamedId::Text(id) => arguments.add(id.as_str()),
            NamedId::Bytes(id) => arguments.add(id.as_slice()),
        }
        .map_err(Error::Encode)
    }
}

/// A bit for each of `held` that is true, the first in the lowest bit.
fn bits<const TYPES: usize>(held: [bool; TYPES]) -> u8 {
    held.iter()
        .rev()
        .fold(0, |bits, &held| (bits << 1) | u8::from(held))
}

/// The error of a column whose type a by-name subscription does not read as `read_as`.
fn unread<Info: TypeInfo>(ty: &Info, read_as: &str) -> BoxDynError {
    format!(
        "a by-name subscription reads this column as {read_as}, and it holds {}",
        ty.name()
    )
    .into()
}

/// A database a by-name subscription reads from the table description alone: it decodes the
/// headers a row keeps as JSON and binds the times its statements need. Machinery; every
/// database with a built-in dialect implements it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a subscription by name needs a database with a built-in dialect, and `{Self}` has none",
    label = "no subscription by name on this database",
    note = "subscribe through a descriptor instead: `#[subscriber(InboxQueue::<Row>::new(\"..\"))]`"
)]
pub trait NamedDatabase: QueueDatabase + RoleColumns {
    /// The headers a row keeps as a JSON object of strings.
    ///
    /// # Errors
    ///
    /// A column that holds no JSON, or the driver's decoding error.
    fn headers(value: Self::ValueRef<'_>) -> Result<HeaderMap, BoxDynError>;

    /// Binds `time`.
    ///
    /// # Errors
    ///
    /// The driver's encoding error.
    fn bind_time(arguments: &mut Self::Arguments, time: NamedTime) -> Result<(), Error>;
}

/// Implements [`NamedDatabase`] for a database whose driver decodes JSON and binds the `chrono`
/// and `time` types, with the features that bring them: each built-in dialect's database.
#[cfg(feature = "postgres")]
macro_rules! named_database {
    ($database:ty) => {
        impl NamedDatabase for $database {
            fn headers(
                value: <$database as Database>::ValueRef<'_>,
            ) -> Result<HeaderMap, BoxDynError> {
                #[cfg(feature = "json")]
                {
                    type Headers = Json<BTreeMap<String, String>>;
                    let json = <Headers as Type<$database>>::compatible(&value.type_info());
                    if !json {
                        return Err(unread(&*value.type_info(), "JSON"));
                    }
                    let mut headers = <Headers as Decode<'_, $database>>::decode(value)?;
                    Ok(HeaderColumn::take_headers(&mut headers))
                }
                #[cfg(not(feature = "json"))]
                {
                    let _ = value;
                    Err("headers kept as JSON need the `json` feature".into())
                }
            }

            fn bind_time(
                arguments: &mut <$database as Database>::Arguments,
                time: NamedTime,
            ) -> Result<(), Error> {
                #[cfg(not(any(feature = "chrono", feature = "time")))]
                let _ = arguments;
                match time {
                    #[cfg(feature = "chrono")]
                    NamedTime::Chrono(at) => arguments.add(at).map_err(Error::Encode),
                    #[cfg(feature = "time")]
                    NamedTime::Time(at) => arguments.add(at).map_err(Error::Encode),
                }
            }
        }
    };
}

#[cfg(feature = "postgres")]
named_database!(sqlx::Postgres);

impl<DB: RoleColumns> Type<DB> for NamedId {
    fn type_info() -> DB::TypeInfo {
        DB::id_type()
    }

    fn compatible(ty: &DB::TypeInfo) -> bool {
        DB::holds_id(ty)
    }
}

impl<'r, DB: RoleColumns> Decode<'r, DB> for NamedId {
    fn decode(value: DB::ValueRef<'r>) -> Result<Self, BoxDynError> {
        DB::id(value).map(|(id, _)| id)
    }
}

#[cfg(test)]
mod tests {
    use super::bits;

    #[test]
    fn bits_follow_the_order_of_the_types() {
        assert_eq!(bits([true, false, true]), 0b101);
        assert_eq!(bits([false, true]), 0b10);
        assert_eq!(bits([false; 5]), 0);
    }
}
