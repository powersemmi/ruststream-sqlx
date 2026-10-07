//! Each event of a record dispatched by its slot in the record's settings: [`Unset`] runs the
//! crate's default statement, kept in the record's registry node, and `Set<own::Event>` runs the
//! record's own impl. One impl per slot value, so the two never overlap.

use std::any::type_name;
use std::future::{Future, ready};

use ruststream::HeaderMap;
use sqlx::{Arguments as _, Database, Encode, Error, FromRow, Type};

use super::database::{Defaults, OutboxDatabase, Statements, no_outbox_statement};
use super::events::{Ack, Discard, Fetch, Recover, Retry};
use super::spec::{Described, Headers, OutboxTable, Set, Unset, own};
use crate::{HeaderColumn, HeaderRow};

/// The folded settings of `Record`'s table.
pub(super) type Declared<Record> = <<Record as OutboxTable>::Table as Described>::Declared;

/// Whether a slot of the settings is set. Machinery.
#[doc(hidden)]
pub trait Slot {
    /// `true` for [`Set`].
    const SET: bool;
}

impl Slot for Unset {
    const SET: bool = false;
}

impl<Value> Slot for Set<Value> {
    const SET: bool = true;
}

/// The headers a record carries again when it is republished, by its `Headers` slot. Machinery.
#[doc(hidden)]
pub trait HeadersOf<Record> {
    /// The headers, moved out of the record.
    fn take(record: &mut Record) -> HeaderMap;
}

impl<Record> HeadersOf<Record> for Unset {
    #[inline]
    fn take(_record: &mut Record) -> HeaderMap {
        HeaderMap::new()
    }
}

impl<Record: HeaderRow> HeadersOf<Record> for Set<Headers> {
    #[inline]
    fn take(record: &mut Record) -> HeaderMap {
        record.headers_mut().take_headers()
    }
}

/// The statements of the connection's database, or the error naming the record and the event.
fn statements<'d, DB: OutboxDatabase, Record>(
    conn: &DB::Connection,
    defaults: &'d Defaults,
    event: &'static str,
) -> Result<&'d Statements, Error> {
    DB::statements(conn, defaults).ok_or_else(|| no_outbox_statement(type_name::<Record>(), event))
}

/// Arguments holding the record's id.
fn id_arguments<'q, DB, Record>(id: &'q <Record as OutboxTable>::Id) -> Result<DB::Arguments, Error>
where
    DB: Database,
    Record: OutboxTable,
    <Record as OutboxTable>::Id: Encode<'q, DB> + Type<DB>,
{
    let mut arguments = DB::Arguments::default();
    arguments.add(id).map_err(Error::Encode)?;
    Ok(arguments)
}

/// The take of a record by its id. Machinery.
#[doc(hidden)]
pub trait FetchBy<DB: Database, Record: OutboxTable> {
    /// Takes the record `id`, or `None` when it is taken or processed already.
    fn fetch<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Option<Record>, Error>> + Send + 'c;
}

impl<DB, Record> FetchBy<DB, Record> for Unset
where
    DB: OutboxDatabase,
    Record: OutboxTable + for<'r> FromRow<'r, DB::Row>,
    for<'q> <Record as OutboxTable>::Id: Encode<'q, DB> + Type<DB>,
{
    async fn fetch<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        defaults: &'c Defaults,
    ) -> Result<Option<Record>, Error> {
        let sql = statements::<DB, Record>(conn, defaults, "fetch")?.fetch;
        DB::fetch_optional(conn, sql, id_arguments::<DB, Record>(id)?).await
    }
}

impl<DB: Database, Record: Fetch<DB> + OutboxTable> FetchBy<DB, Record> for Set<own::Fetch> {
    fn fetch<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        _defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Option<Record>, Error>> + Send + 'c {
        Record::fetch(conn, id)
    }
}

/// An outcome's event for the record `id`, by its slot; `$own` is the event's own marker and
/// `$default` the default's body.
macro_rules! outcome_by {
    (
        $(#[$doc:meta])* $by:ident, $event:ident, $method:ident, $own:ident,
        |$conn:ident, $id:ident, $defaults:ident| $default:block,
        where $($bound:tt)*
    ) => {
        $(#[$doc])*
        #[doc(hidden)]
        pub trait $by<DB: Database, Record: OutboxTable> {
            /// Runs the event for the record `id`.
            fn $method<'c>(
                conn: &'c mut DB::Connection,
                id: &'c <Record as OutboxTable>::Id,
                defaults: &'c Defaults,
            ) -> impl Future<Output = Result<(), Error>> + Send + 'c;
        }

        impl<DB, Record> $by<DB, Record> for Unset
        where
            $($bound)*
        {
            #[allow(clippy::manual_async_fn)]
            fn $method<'c>(
                $conn: &'c mut DB::Connection,
                $id: &'c <Record as OutboxTable>::Id,
                $defaults: &'c Defaults,
            ) -> impl Future<Output = Result<(), Error>> + Send + 'c {
                $default
            }
        }

        impl<DB: Database, Record: $event<DB> + OutboxTable> $by<DB, Record> for Set<own::$own> {
            fn $method<'c>(
                conn: &'c mut DB::Connection,
                id: &'c <Record as OutboxTable>::Id,
                _defaults: &'c Defaults,
            ) -> impl Future<Output = Result<(), Error>> + Send + 'c {
                <Record as $event<DB>>::$method(conn, id)
            }
        }
    };
}

outcome_by!(
    /// The mark of an acknowledged record. Machinery.
    AckBy, Ack, ack, Ack,
    |conn, id, defaults| {
        async move {
            let sql = statements::<DB, Record>(conn, defaults, "ack")?.mark;
            DB::execute(conn, sql, id_arguments::<DB, Record>(id)?).await
        }
    },
    where
        DB: OutboxDatabase,
        Record: OutboxTable,
        for<'q> <Record as OutboxTable>::Id: Encode<'q, DB> + Type<DB>,
);

outcome_by!(
    /// The mark of a dropped record. Machinery.
    DiscardBy, Discard, discard, Discard,
    |conn, id, defaults| {
        async move {
            let sql = statements::<DB, Record>(conn, defaults, "discard")?.mark;
            DB::execute(conn, sql, id_arguments::<DB, Record>(id)?).await
        }
    },
    where
        DB: OutboxDatabase,
        Record: OutboxTable,
        for<'q> <Record as OutboxTable>::Id: Encode<'q, DB> + Type<DB>,
);

outcome_by!(
    /// What a retried record runs: nothing by default, so the record stays for the next startup.
    /// Machinery.
    RetryBy, Retry, retry, Retry,
    |_conn, _id, _defaults| { ready(Ok(())) },
    where
        DB: Database,
        Record: OutboxTable,
);

/// The selection of the unprocessed records of a name. Machinery.
#[doc(hidden)]
pub trait RecoverBy<DB: Database, Record: OutboxTable> {
    /// The unprocessed records published under `name`.
    fn recover<'c>(
        conn: &'c mut DB::Connection,
        name: &'c str,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Vec<Record>, Error>> + Send + 'c;
}

impl<DB, Record> RecoverBy<DB, Record> for Unset
where
    DB: OutboxDatabase,
    Record: OutboxTable + for<'r> FromRow<'r, DB::Row>,
{
    async fn recover<'c>(
        conn: &'c mut DB::Connection,
        name: &'c str,
        defaults: &'c Defaults,
    ) -> Result<Vec<Record>, Error> {
        let sql = statements::<DB, Record>(conn, defaults, "recover")?.recover;
        let mut arguments = DB::Arguments::default();
        DB::bind_name(&mut arguments, name)?;
        DB::fetch_all(conn, sql, arguments).await
    }
}

impl<DB: Database, Record: Recover<DB> + OutboxTable> RecoverBy<DB, Record> for Set<own::Recover> {
    fn recover<'c>(
        conn: &'c mut DB::Connection,
        name: &'c str,
        _defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Vec<Record>, Error>> + Send + 'c {
        Record::recover(conn, name)
    }
}
