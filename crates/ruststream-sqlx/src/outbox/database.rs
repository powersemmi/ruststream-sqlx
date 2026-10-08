//! The databases the outbox's default statements run on: each builds a record's statements in the
//! built-in dialects its connections speak, and a connection picks its own.

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use std::collections::BTreeSet;
use std::future::Future;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use std::sync::{Mutex, PoisonError};

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use ruststream_sqlx_dialect::{self as dialect, OutboxDialect};
use ruststream_sqlx_dialect::{StatementError, TableSpec};

#[cfg(feature = "any")]
use sqlx::Any;
#[cfg(feature = "any")]
use sqlx::AnyConnection;
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
use sqlx::Arguments as _;
use sqlx::{Database, Error, FromRow};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};

/// The texts of the default statements of one record on one dialect, interned for the life of the
/// process. Machinery.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Statements {
    /// The fetch of a record by its id.
    pub(super) fetch: &'static str,
    /// The mark of a processed record, by its id: `Ack` and `Discard`.
    pub(super) mark: &'static str,
    /// The recovery of the unprocessed records of a name.
    pub(super) recover: &'static str,
}

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
impl Statements {
    /// The statements `dialect` writes for `spec`.
    fn of(dialect: &impl OutboxDialect, spec: &TableSpec<'_>) -> Result<Self, StatementError> {
        Ok(Self {
            fetch: intern(dialect.outbox_fetch(spec)?.sql()),
            mark: intern(dialect.outbox_mark(spec)?.sql()),
            recover: intern(dialect.outbox_recover(spec)?.sql()),
        })
    }
}

/// The default statements of one record, built when the record type is registered and kept in its
/// registry node: one set per built-in dialect the record's database runs, `None` for the others.
/// Machinery.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Defaults {
    #[cfg(feature = "postgres")]
    postgres: Option<Statements>,
    #[cfg(feature = "mysql")]
    mysql: Option<Statements>,
    #[cfg(feature = "sqlite")]
    sqlite: Option<Statements>,
}

/// `text` for the life of the process, written once whatever the number of registries that build
/// it.
// Why leaked: sqlx 0.9 runs a `&'static str` as written and copies any shorter-lived text into a
// new `Arc<str>` on every run, so the per-message path needs `'static` statements. A registry
// builds them once, at construction; the set keeps each distinct text once, so building a
// registry again (a test suite, a service rebuilt in one process) leaks nothing new.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
fn intern(text: &str) -> &'static str {
    static TEXTS: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());
    let mut texts = TEXTS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(interned) = texts.get(text) {
        return interned;
    }
    let interned: &'static str = Box::leak(text.into());
    texts.insert(interned);
    interned
}

/// The name `AnyConnection::backend_name` gives each built-in database, as sqlx names it.
#[cfg(all(feature = "any", feature = "postgres"))]
const POSTGRES_BACKEND: &str = "PostgreSQL";
#[cfg(all(feature = "any", feature = "mysql"))]
const MYSQL_BACKEND: &str = "MySQL";
#[cfg(all(feature = "any", feature = "sqlite"))]
const SQLITE_BACKEND: &str = "SQLite";

#[cfg(feature = "any")]
impl Defaults {
    /// The statements of the database behind the `AnyPool` backend named `backend`.
    fn for_backend(&self, backend: &str) -> Option<&Statements> {
        match backend {
            #[cfg(feature = "postgres")]
            POSTGRES_BACKEND => self.postgres.as_ref(),
            #[cfg(feature = "mysql")]
            MYSQL_BACKEND => self.mysql.as_ref(),
            #[cfg(feature = "sqlite")]
            SQLITE_BACKEND => self.sqlite.as_ref(),
            _ => None,
        }
    }
}

/// A sqlx database the default events of an outbox record run on.
///
/// The registry builds the statements from the record's
/// [`OutboxTable::TABLE`](super::OutboxTable::TABLE) in each built-in dialect the database's
/// connections speak, once, when the record type is registered, and a connection picks its own.
///
/// [`Postgres`](sqlx::Postgres), [`MySql`](sqlx::MySql) (MySQL and MariaDB) and
/// [`Sqlite`](sqlx::Sqlite) implement it under their features, and [`Any`](sqlx::Any) under `any`,
/// where the database the connection reached picks the text. A record's default event on a
/// backend whose dialect the crate was built without fails with [`sqlx::Error::Configuration`]
/// naming the record and the event.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::{Outbox, OutboxDatabase, outbox};
/// use sqlx::{PgConnection, Postgres};
///
/// // outbox: id BIGSERIAL PRIMARY KEY, name TEXT, payload BYTEA; a processed record is deleted
/// #[derive(Outbox, sqlx::FromRow)]
/// #[outbox(table = "outbox")]
/// pub struct OrderEvent {
///     #[field(id)]
///     id: i64,
///     #[field(name)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// // The record of a published message is the service's own statement.
/// impl outbox::Publish<Postgres> for OrderEvent {
///     async fn publish(
///         conn: &mut PgConnection,
///         msg: &OutgoingMessage<'_>,
///     ) -> Result<i64, sqlx::Error> {
///         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
///             .bind(msg.name())
///             .bind(msg.payload())
///             .fetch_one(conn)
///             .await
///     }
/// }
///
/// // The default events run on any `OutboxDatabase`: here the mark of a processed record.
/// pub async fn mark<DB: OutboxDatabase>(
///     conn: &mut DB::Connection,
///     id: &i64,
/// ) -> Result<(), sqlx::Error>
/// where
///     OrderEvent: outbox::Ack<DB>,
/// {
///     <OrderEvent as outbox::Ack<DB>>::ack(conn, id).await
/// }
/// # }
/// # fn main() {}
/// ```
pub trait OutboxDatabase: Database {
    /// The default statements of the table `spec` describes, in each built-in dialect this
    /// database's connections may speak. Machinery.
    #[doc(hidden)]
    fn defaults(spec: &TableSpec<'_>) -> Result<Defaults, StatementError>;

    /// The statements this connection's database runs, or `None` when the crate was built without
    /// its dialect. Machinery.
    #[doc(hidden)]
    fn statements<'d>(conn: &Self::Connection, defaults: &'d Defaults) -> Option<&'d Statements>;

    /// Binds the name a recovery reads the records of. Machinery.
    #[doc(hidden)]
    fn bind_name(arguments: &mut Self::Arguments, name: &str) -> Result<(), Error>;

    /// Runs a statement. Machinery.
    #[doc(hidden)]
    fn execute<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Runs a statement that reads at most one record. Machinery.
    #[doc(hidden)]
    fn fetch_optional<'c, Record>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Option<Record>, Error>> + Send + 'c
    where
        Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c;

    /// Runs a statement that reads every record it selects. Machinery.
    #[doc(hidden)]
    fn fetch_all<'c, Record>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Vec<Record>, Error>> + Send + 'c
    where
        Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c;
}

/// `OutboxDatabase` for one sqlx database: `$build` builds the statements of a table, and the
/// connection picks the ones `$pick` names.
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
macro_rules! outbox_database {
    (
        $database:ty, $connection:ty,
        |$spec:ident| $build:expr,
        |$conn:ident, $defaults:ident| $pick:expr
    ) => {
        impl OutboxDatabase for $database {
            fn defaults($spec: &TableSpec<'_>) -> Result<Defaults, StatementError> {
                $build
            }

            fn statements<'d>(
                $conn: &$connection,
                $defaults: &'d Defaults,
            ) -> Option<&'d Statements> {
                $pick
            }

            fn bind_name(arguments: &mut Self::Arguments, name: &str) -> Result<(), Error> {
                arguments.add(name).map_err(Error::Encode)
            }

            async fn execute(
                conn: &mut $connection,
                sql: &'static str,
                arguments: Self::Arguments,
            ) -> Result<(), Error> {
                sqlx::query_with(sql, arguments).execute(conn).await?;
                Ok(())
            }

            async fn fetch_optional<'c, Record>(
                conn: &'c mut $connection,
                sql: &'static str,
                arguments: Self::Arguments,
            ) -> Result<Option<Record>, Error>
            where
                Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c,
            {
                sqlx::query_as_with(sql, arguments)
                    .fetch_optional(conn)
                    .await
            }

            async fn fetch_all<'c, Record>(
                conn: &'c mut $connection,
                sql: &'static str,
                arguments: Self::Arguments,
            ) -> Result<Vec<Record>, Error>
            where
                Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c,
            {
                sqlx::query_as_with(sql, arguments).fetch_all(conn).await
            }
        }
    };
}

#[cfg(feature = "postgres")]
outbox_database!(
    Postgres,
    PgConnection,
    |spec| Ok(Defaults {
        postgres: Some(Statements::of(&dialect::Postgres, spec)?),
        ..Defaults::default()
    }),
    |_conn, defaults| defaults.postgres.as_ref()
);
#[cfg(feature = "mysql")]
outbox_database!(
    MySql,
    MySqlConnection,
    |spec| Ok(Defaults {
        mysql: Some(Statements::of(&dialect::MySql, spec)?),
        ..Defaults::default()
    }),
    |_conn, defaults| defaults.mysql.as_ref()
);
#[cfg(feature = "sqlite")]
outbox_database!(
    Sqlite,
    SqliteConnection,
    |spec| Ok(Defaults {
        sqlite: Some(Statements::of(&dialect::Sqlite, spec)?),
        ..Defaults::default()
    }),
    |_conn, defaults| defaults.sqlite.as_ref()
);
// `Any` passes a statement to its backend as written, so the backend picks the text.
#[cfg(feature = "any")]
outbox_database!(
    Any,
    AnyConnection,
    |spec| Ok(Defaults {
        #[cfg(feature = "postgres")]
        postgres: Some(Statements::of(&dialect::Postgres, spec)?),
        #[cfg(feature = "mysql")]
        mysql: Some(Statements::of(&dialect::MySql, spec)?),
        #[cfg(feature = "sqlite")]
        sqlite: Some(Statements::of(&dialect::Sqlite, spec)?),
    }),
    |conn, defaults| defaults.for_backend(conn.backend_name())
);

/// The error of a default event on a connection whose database the crate was built without.
pub(super) fn no_outbox_statement(record: &'static str, event: &'static str) -> Error {
    Error::Configuration(
        format!("`{record}` has no {event} statement for this connection's database").into(),
    )
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "postgres")]
    use std::ptr;

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    use ruststream_sqlx_dialect::{Column, Form, OutboxDialect, StatementError, TableSpec};

    use super::no_outbox_statement;
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    use super::{Defaults, OutboxDatabase, dialect};

    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    const TABLE: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
        .group(Column::new("name"))
        .payload(Column::new("payload"))
        .processed_at(Column::new("processed_at"))
        .database_clock();

    #[test]
    fn the_missing_statement_names_the_record_and_the_event() {
        assert_eq!(
            no_outbox_statement("OrderEvent", "fetch").to_string(),
            "error with configuration: `OrderEvent` has no fetch statement for this connection's \
             database"
        );
    }

    #[cfg(all(
        feature = "any",
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite"
    ))]
    #[test]
    fn an_any_backend_runs_the_statements_of_its_database() -> Result<(), StatementError> {
        use sqlx::Any;

        use super::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

        let defaults = <Any as OutboxDatabase>::defaults(&TABLE)?;
        // The mark reads each database's clock, so its text differs on every dialect.
        let mark = |backend| {
            defaults
                .for_backend(backend)
                .map(|statements| statements.mark)
        };
        assert_eq!(
            mark(POSTGRES_BACKEND),
            Some(dialect::Postgres.outbox_mark(&TABLE)?.sql())
        );
        assert_eq!(
            mark(MYSQL_BACKEND),
            Some(dialect::MySql.outbox_mark(&TABLE)?.sql())
        );
        assert_eq!(
            mark(SQLITE_BACKEND),
            Some(dialect::Sqlite.outbox_mark(&TABLE)?.sql())
        );
        assert_eq!(mark("MSSQL"), None);
        Ok(())
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn a_statement_built_twice_is_kept_once() -> Result<(), StatementError> {
        use sqlx::Postgres;

        let first = <Postgres as OutboxDatabase>::defaults(&TABLE)?;
        let second = <Postgres as OutboxDatabase>::defaults(&TABLE)?;
        let mark = |defaults: &Defaults| defaults.postgres.map(|statements| statements.mark);
        let (Some(first), Some(second)) = (mark(&first), mark(&second)) else {
            panic!("Postgres builds its own statements");
        };
        assert!(
            ptr::eq(first, second),
            "the second build reuses the first's text"
        );
        Ok(())
    }

    #[cfg(all(
        feature = "any",
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite"
    ))]
    #[test]
    fn the_backend_names_are_sqlxs() {
        use sqlx::Database;

        use super::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

        assert_eq!(POSTGRES_BACKEND, <sqlx::Postgres as Database>::NAME);
        assert_eq!(MYSQL_BACKEND, <sqlx::MySql as Database>::NAME);
        assert_eq!(SQLITE_BACKEND, <sqlx::Sqlite as Database>::NAME);
    }
}
