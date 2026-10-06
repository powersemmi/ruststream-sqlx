//! By-name subscriptions as a feature of the dialect: the headers and times their statements read
//! and bind, per database.

#[cfg(all(
    feature = "json",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use std::collections::BTreeMap;

use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Dialect;
#[cfg(all(
    any(feature = "postgres", feature = "mysql", feature = "sqlite"),
    any(feature = "chrono", feature = "time")
))]
use sqlx::Arguments;
use sqlx::error::BoxDynError;
#[cfg(all(
    feature = "json",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use sqlx::types::Json;
use sqlx::{Database, Error};
#[cfg(all(
    feature = "json",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use sqlx::{Decode, Type, ValueRef};

#[cfg(all(
    feature = "json",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use super::database::unread;
use super::row::NamedTime;
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
use crate::inbox::BuiltIn;
#[cfg(all(
    feature = "json",
    any(feature = "postgres", feature = "mysql", feature = "sqlite")
))]
use crate::inbox::HeaderColumn;

/// Subscriptions by name on the database `DB`, as a feature of a dialect: the headers and the
/// times their rows hold, which no struct of the service's own names.
///
/// A by-name subscription (`#[subscriber("emails")]`) reads the table the name's route leads to.
/// Where the route's row leaves every event to the crate, the subscription reads the role
/// columns into a row of the crate's own and binds its statements from there, with no box and no
/// dynamic call per message. The ids, payloads, keys and attempts it reads are integers, text
/// and bytes, which every sqlx driver decodes alike; the headers kept as JSON and the times are
/// the database's own types, and this trait reads and binds them. A dialect that implements it
/// for its database gets subscriptions by name; on a dialect that does not, a by-name mount does
/// not compile, and its tables take [`InboxQueue`](crate::InboxQueue) descriptors.
///
/// [`BuiltIn<DB>`](crate::BuiltIn) implements it for each built-in database. A dialect of the
/// service's own that wraps a built-in one delegates to it; one for a driver outside sqlx reads
/// and binds through that driver.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::HeaderMap;
/// use ruststream_sqlx::dialect::{self, Dialect};
/// use ruststream_sqlx::{BuiltIn, ByName, NamedTime};
/// use sqlx::Postgres;
/// use sqlx::error::BoxDynError;
/// use sqlx::postgres::{PgArguments, PgValueRef};
///
/// /// A dialect of the service's own over Postgres.
/// #[derive(Debug)]
/// pub struct Audited;
/// # impl Dialect for Audited {
/// #     fn name(&self) -> &'static str { "audited" }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { dialect::Postgres.quote_into(ident, out) }
/// #     fn placeholder_into(&self, index: std::num::NonZeroUsize, out: &mut String) { dialect::Postgres.placeholder_into(index, out) }
/// #     fn fetch(&self, spec: &dialect::TableSpec<'_>) -> Result<dialect::Statement, dialect::StatementError> { dialect::Postgres.fetch(spec) }
/// #     fn ack(&self, spec: &dialect::TableSpec<'_>) -> Result<dialect::Statement, dialect::StatementError> { dialect::Postgres.ack(spec) }
/// #     fn retry(&self, spec: &dialect::TableSpec<'_>) -> Result<Option<dialect::Statement>, dialect::StatementError> { dialect::Postgres.retry(spec) }
/// #     fn retry_after(&self, spec: &dialect::TableSpec<'_>) -> Result<dialect::Statement, dialect::StatementError> { dialect::Postgres.retry_after(spec) }
/// #     fn discard(&self, spec: &dialect::TableSpec<'_>) -> Result<dialect::Statement, dialect::StatementError> { dialect::Postgres.discard(spec) }
/// #     fn dead_letter_group(&self, spec: &dialect::TableSpec<'_>) -> Result<dialect::Statement, dialect::StatementError> { dialect::Postgres.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &dialect::TableSpec<'_>, target: dialect::TableName<'_>) -> Result<Vec<dialect::Statement>, dialect::StatementError> { dialect::Postgres.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &dialect::TableSpec<'_>) -> Result<dialect::Statement, dialect::StatementError> { dialect::Postgres.insert(spec) }
/// # }
///
/// // Its rows keep their headers and times as the built-in dialect reads them.
/// impl ByName<Postgres> for Audited {
///     fn headers(value: PgValueRef<'_>) -> Result<HeaderMap, BoxDynError> {
///         <BuiltIn<Postgres> as ByName<Postgres>>::headers(value)
///     }
///
///     fn bind_time(arguments: &mut PgArguments, time: NamedTime) -> Result<(), sqlx::Error> {
///         <BuiltIn<Postgres> as ByName<Postgres>>::bind_time(arguments, time)
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "the `{Self}` dialect has no subscriptions by name on `{DB}`",
    label = "a subscription by name needs its dialect to read and bind its rows",
    note = "implement `ByName<{DB}>` for the dialect, or subscribe through a descriptor: \
            `#[subscriber(InboxQueue::<Row>::new(\"..\"))]`"
)]
pub trait ByName<DB: Database>: Dialect {
    /// The headers a row keeps as a JSON object of strings.
    ///
    /// # Errors
    ///
    /// A column that holds no JSON, or the driver's decoding error.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream::HeaderMap;
    /// use ruststream_sqlx::ByName;
    /// use sqlx::error::BoxDynError;
    /// use sqlx::postgres::PgRow;
    /// use sqlx::{Postgres, Row};
    ///
    /// /// The headers of a row a dialect reads by name, from its `meta` column.
    /// fn headers_of<D: ByName<Postgres>>(row: &PgRow) -> Result<HeaderMap, BoxDynError> {
    ///     D::headers(row.try_get_raw("meta")?)
    /// }
    /// # }
    /// ```
    fn headers(value: DB::ValueRef<'_>) -> Result<HeaderMap, BoxDynError>;

    /// Binds `time`: the current time, a delayed retry's, or a lease's expiry.
    ///
    /// # Errors
    ///
    /// The driver's encoding error, or a time of a type the database does not bind.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))] {
    /// use ruststream_sqlx::{BuiltIn, ByName, NamedTime};
    /// use sqlx::Postgres;
    /// use sqlx::postgres::PgArguments;
    ///
    /// // The arguments of a statement that releases a row an hour from now.
    /// let mut arguments = PgArguments::default();
    /// let later = chrono::Utc::now() + chrono::Duration::hours(1);
    /// <BuiltIn<Postgres> as ByName<Postgres>>::bind_time(&mut arguments, NamedTime::Chrono(later))?;
    /// # }
    /// # Ok::<(), sqlx::Error>(())
    /// ```
    fn bind_time(arguments: &mut DB::Arguments, time: NamedTime) -> Result<(), Error>;
}

/// Implements [`ByName`] for the built-in dialect of a database whose driver decodes JSON and
/// binds the `chrono` and `time` types, with the features that bring them.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
macro_rules! by_name {
    ($database:ty) => {
        impl ByName<$database> for BuiltIn<$database> {
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
by_name!(sqlx::Postgres);
#[cfg(feature = "mysql")]
by_name!(sqlx::MySql);
#[cfg(feature = "sqlite")]
by_name!(sqlx::Sqlite);

// Why errors: `sqlx::Any` decodes no JSON and binds no time, so no row on an `AnyPool` has a
// `headers` column of JSON or a time column, and a by-name subscription there reaches neither.
#[cfg(feature = "any")]
impl ByName<sqlx::Any> for BuiltIn<sqlx::Any> {
    fn headers(value: <sqlx::Any as Database>::ValueRef<'_>) -> Result<HeaderMap, BoxDynError> {
        let _ = value;
        Err("an `AnyPool` decodes no JSON headers".into())
    }

    fn bind_time(
        arguments: &mut <sqlx::Any as Database>::Arguments,
        time: NamedTime,
    ) -> Result<(), Error> {
        let _ = (arguments, time);
        Err(Error::Configuration("an `AnyPool` binds no time".into()))
    }
}
