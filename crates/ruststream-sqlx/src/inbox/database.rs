//! The databases the inbox runs on.

use std::future::Future;

use futures::TryStreamExt;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use ruststream_sqlx_dialect as dialect;
use ruststream_sqlx_dialect::Dialect;
use sqlx::Row as _;
use sqlx::any::AnyQueryResult;
#[cfg(feature = "any")]
use sqlx::{Any, AnyConnection};
use sqlx::{
    Arguments, ColumnIndex, Database, Decode, Encode, Error, Executor, FromRow, IntoArguments,
    SqlStr, Type,
};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};

use super::QueueRow;
use super::engine::{Claimed, IdAt};

/// A sqlx database the inbox runs on: one that binds the text and the integers the queue's own
/// statements bind, runs statements on a connection, and reports the rows a statement changed.
///
/// Every database whose sqlx driver does so implements it; Postgres, MySQL, SQLite and `Any` do.
/// The rows a statement changed come through sqlx's [`AnyQueryResult`], which every driver in sqlx
/// converts its result into; a driver outside sqlx provides that conversion for its own result
/// type.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx::QueueDatabase;
///
/// fn serves_queues<DB: QueueDatabase>() -> &'static str {
///     std::any::type_name::<DB>()
/// }
/// assert!(serves_queues::<sqlx::Postgres>().ends_with("Postgres"));
/// # }
/// ```
pub trait QueueDatabase: Database {
    /// Binds a text value. Machinery; the crate's statements call it.
    #[doc(hidden)]
    fn bind_str(arguments: &mut Self::Arguments, value: &str) -> Result<(), Error>;

    /// Binds an integer. Machinery; the crate's statements call it.
    #[doc(hidden)]
    fn bind_i64(arguments: &mut Self::Arguments, value: i64) -> Result<(), Error>;

    /// Runs a statement and returns the rows it changed. Machinery.
    #[doc(hidden)]
    fn execute<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<u64, Error>> + Send + 'c;

    /// Runs a claim or a fetch of whole rows into `out`: a row its struct does not decode is
    /// [`Claimed::Undecodable`], its id read alone at `id_at`. Machinery.
    #[doc(hidden)]
    fn fetch_rows<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        id_at: IdAt,
        out: &'c mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c
    where
        Row: QueueRow + for<'r> FromRow<'r, Self::Row> + Unpin,
        Row::Id: for<'r> Decode<'r, Self> + Type<Self>;

    /// Runs a claim of ids. Machinery.
    #[doc(hidden)]
    fn fetch_ids<'c, Id>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Vec<Id>, Error>> + Send + 'c
    where
        Id: for<'r> Decode<'r, Self> + Type<Self> + Send + Unpin + 'c;

    /// Prepares a statement on the server: the startup check. Machinery.
    #[doc(hidden)]
    fn prepare<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Runs a query of one text value, such as the server's version, and returns the value.
    /// Machinery.
    #[doc(hidden)]
    fn fetch_text<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
    ) -> impl Future<Output = Result<String, Error>> + Send + 'c;
}

impl<DB> QueueDatabase for DB
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'r> String: Decode<'r, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
    for<'a> &'a str: ColumnIndex<DB::Row>,
    DB::QueryResult: Into<AnyQueryResult>,
{
    fn bind_str(arguments: &mut Self::Arguments, value: &str) -> Result<(), Error> {
        arguments.add(value).map_err(Error::Encode)
    }

    fn bind_i64(arguments: &mut Self::Arguments, value: i64) -> Result<(), Error> {
        arguments.add(value).map_err(Error::Encode)
    }

    async fn execute(
        conn: &mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> Result<u64, Error> {
        let result: AnyQueryResult = sqlx::query_with(sql, arguments).execute(conn).await?.into();
        Ok(result.rows_affected())
    }

    async fn fetch_rows<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        id_at: IdAt,
        out: &'c mut Vec<Claimed<Row>>,
    ) -> Result<(), Error>
    where
        Row: QueueRow + for<'r> FromRow<'r, Self::Row> + Unpin,
        Row::Id: for<'r> Decode<'r, Self> + Type<Self>,
    {
        // The decode `query_as` runs, one row at a time, so a row that fails it fails alone.
        let mut rows = sqlx::query_with::<Self, _>(sql, arguments).fetch(conn);
        while let Some(raw) = rows.try_next().await? {
            match Row::from_row(&raw) {
                Ok(row) => out.push(Claimed::Row(row)),
                Err(error) => {
                    // Why the id alone: a row only its id can name is still a row the policy
                    // settles, and nothing settles a row whose id does not decode either.
                    let id = match id_at {
                        IdAt::First => raw.try_get::<Row::Id, _>(0_usize),
                        IdAt::Named(name) => raw.try_get::<Row::Id, _>(name),
                    };
                    match id {
                        Ok(id) => out.push(Claimed::Undecodable {
                            id,
                            error: Box::new(error),
                        }),
                        Err(_) => return Err(error),
                    }
                }
            }
        }
        Ok(())
    }

    fn fetch_ids<'c, Id>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Vec<Id>, Error>> + Send + 'c
    where
        Id: for<'r> Decode<'r, Self> + Type<Self> + Send + Unpin + 'c,
    {
        sqlx::query_scalar_with::<Self, Id, _>(sql, arguments).fetch_all(conn)
    }

    async fn prepare(conn: &mut Self::Connection, sql: &'static str) -> Result<(), Error> {
        conn.prepare(SqlStr::from_static(sql)).await?;
        Ok(())
    }

    fn fetch_text<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
    ) -> impl Future<Output = Result<String, Error>> + Send + 'c {
        sqlx::query_scalar::<Self, String>(sql).fetch_one(conn)
    }
}

/// A database whose dialect is built into this crate, so `SqlxBroker::new` needs no dialect of
/// the service's own.
///
/// Postgres (feature `postgres`), MySQL with MariaDB (feature `mysql`) and SQLite (feature
/// `sqlite`) have one. `Any` (feature `any`) takes the dialect of the database its pool reaches,
/// among those whose features are on, and the broker picks it when it connects. A row on an
/// `AnyPool` holds only the types `sqlx::Any` carries, and no time is among them, so a table with
/// `locked_until`, `retry_after` or `processed_at` is out of its reach.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "any")]
/// # async fn run(pool: sqlx::AnyPool) -> Result<(), sqlx::Error> {
/// use ruststream_sqlx::BuiltInDialect;
///
/// // The statements the broker builds for the database behind the pool.
/// let conn = pool.acquire().await?;
/// match <sqlx::Any as BuiltInDialect>::dialect(&conn) {
///     Some(dialect) => tracing::info!(dialect = dialect.name(), "the inbox's statements"),
///     None => tracing::warn!(backend = conn.backend_name(), "no built-in dialect"),
/// }
/// # Ok(())
/// # }
/// ```
pub trait BuiltInDialect: QueueDatabase {
    /// The dialect that builds the statements of the database `conn` reaches, or `None` when no
    /// built-in dialect serves it: an `AnyPool`'s backend whose feature is off.
    ///
    /// Postgres, MySQL and SQLite answer without reading `conn`; the broker asks with the
    /// connection it checks when it connects.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx::BuiltInDialect;
    /// use sqlx::Pool;
    ///
    /// /// The dialect the broker builds statements with for the database `pool` reaches.
    /// async fn dialect_of<DB: BuiltInDialect>(
    ///     pool: &Pool<DB>,
    /// ) -> Result<Option<&'static str>, sqlx::Error> {
    ///     let conn = pool.acquire().await?;
    ///     Ok(DB::dialect(&conn).map(|dialect| dialect.name()))
    /// }
    /// ```
    fn dialect(conn: &Self::Connection) -> Option<&'static dyn Dialect>;

    /// The name of the database `conn` reaches, which an error names when no built-in dialect
    /// serves it. Machinery; the broker calls it.
    #[doc(hidden)]
    fn backend(conn: &Self::Connection) -> &str {
        let _ = conn;
        Self::NAME
    }
}

#[cfg(feature = "postgres")]
impl BuiltInDialect for Postgres {
    fn dialect(_: &PgConnection) -> Option<&'static dyn Dialect> {
        Some(&dialect::Postgres)
    }
}

#[cfg(feature = "mysql")]
impl BuiltInDialect for MySql {
    fn dialect(_: &MySqlConnection) -> Option<&'static dyn Dialect> {
        Some(&dialect::MySql)
    }
}

#[cfg(feature = "sqlite")]
impl BuiltInDialect for Sqlite {
    fn dialect(_: &SqliteConnection) -> Option<&'static dyn Dialect> {
        Some(&dialect::Sqlite)
    }
}

#[cfg(feature = "any")]
impl BuiltInDialect for Any {
    fn dialect(conn: &AnyConnection) -> Option<&'static dyn Dialect> {
        any_dialect(conn.backend_name())
    }

    fn backend(conn: &AnyConnection) -> &str {
        conn.backend_name()
    }
}

/// The name an `AnyConnection` reports for a Postgres backend: its sqlx driver's
/// `Database::NAME`.
#[cfg(feature = "any")]
const POSTGRES_BACKEND: &str = "PostgreSQL";

/// The name an `AnyConnection` reports for a MySQL or MariaDB backend.
#[cfg(feature = "any")]
const MYSQL_BACKEND: &str = "MySQL";

/// The name an `AnyConnection` reports for a SQLite backend.
#[cfg(feature = "any")]
const SQLITE_BACKEND: &str = "SQLite";

/// The built-in dialect of the `AnyPool` backend named `backend`, where its feature is on.
#[cfg(feature = "any")]
fn any_dialect(backend: &str) -> Option<&'static dyn Dialect> {
    let dialects: &[(&str, &'static dyn Dialect)] = &[
        #[cfg(feature = "postgres")]
        (POSTGRES_BACKEND, &dialect::Postgres),
        #[cfg(feature = "mysql")]
        (MYSQL_BACKEND, &dialect::MySql),
        #[cfg(feature = "sqlite")]
        (SQLITE_BACKEND, &dialect::Sqlite),
    ];
    dialects
        .iter()
        .find(|(name, _)| *name == backend)
        .map(|&(_, dialect)| dialect)
}

/// A database that locks rows for a transaction, so a table on it may take its rows by row lock:
/// the form of a table that declares neither `#[field(locked_until)]` nor `advisory_lock`.
///
/// Postgres (feature `postgres`), MySQL with MariaDB (feature `mysql`) and `Any` (feature `any`)
/// implement it. SQLite locks the whole database for a writer, not rows: a subscription on it to
/// a table in the row lock form does not compile, and the error names the forms that run there.
/// An `AnyPool` names its database only when the broker connects, so a subscription there to a
/// row lock table on SQLite stops at startup with the dialect's refusal.
///
/// # Examples
///
/// A service on SQLite declares `locked_until`, and its queue takes rows by lease:
///
/// ```no_run
/// # #[cfg(all(feature = "sqlite", feature = "chrono"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
/// use serde::Deserialize;
/// use sqlx::SqlitePool;
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs")]
/// pub struct Job {
///     #[field(id, generated)]
///     id: i64,
///     // Without it the table is in the row lock form, which a database without row locks
///     // does not serve.
///     #[field(locked_until)]
///     locked_until: Option<DateTime<Utc>>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// pub struct Task {
///     n: u32,
/// }
///
/// #[subscriber(InboxQueue::<Job>::new("jobs"))]
/// async fn work(task: &Task) -> HandlerOutcome {
///     tracing::info!(n = task.n, "working");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(work);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no row locks, so a table on it cannot use the row lock form",
    label = "a table without `locked_until` or `advisory_lock` takes rows by row lock",
    note = "declare `#[field(locked_until)]` (the lease form) or \
            `#[inbox(advisory_lock = \"..\")]` (the advisory lock form) on the struct"
)]
pub trait RowLocks: QueueDatabase {}

#[cfg(feature = "postgres")]
impl RowLocks for Postgres {}

#[cfg(feature = "mysql")]
impl RowLocks for MySql {}

// Why a runtime refusal remains: the backend of an `AnyPool` is known only at connect, so a SQLite
// backend refuses a row lock table when its subscription starts.
#[cfg(feature = "any")]
impl RowLocks for Any {}

/// The inserts the derive builds at compile time, one per built-in dialect it was built with.
/// Machinery; the derive writes it, never a service.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct InsertSql<'s> {
    /// The Postgres insert.
    pub postgres: Option<&'s str>,
    /// The MySQL and MariaDB insert.
    pub mysql: Option<&'s str>,
    /// The SQLite insert.
    pub sqlite: Option<&'s str>,
}

#[cfg(feature = "any")]
impl<'s> InsertSql<'s> {
    /// The insert of the database behind the `AnyPool` backend named `backend`.
    fn for_backend(&self, backend: &str) -> Option<&'s str> {
        match backend {
            POSTGRES_BACKEND => self.postgres,
            MYSQL_BACKEND => self.mysql,
            SQLITE_BACKEND => self.sqlite,
            _ => None,
        }
    }
}

/// A connection the inserts the derive builds at compile time run on: its database, and which
/// of the derive's statements it runs. Machinery; never named directly.
#[doc(hidden)]
pub trait OnConnection: Send {
    /// The database the connection reaches.
    type Database: QueueDatabase;

    /// The connection itself.
    fn connection(&mut self) -> &mut <Self::Database as Database>::Connection;

    /// The insert of this connection's database among `sql`, or `None` when the derive built none
    /// for it.
    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str>;
}

#[cfg(feature = "postgres")]
impl OnConnection for PgConnection {
    type Database = Postgres;

    fn connection(&mut self) -> &mut Self {
        self
    }

    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.postgres
    }
}

#[cfg(feature = "mysql")]
impl OnConnection for MySqlConnection {
    type Database = MySql;

    fn connection(&mut self) -> &mut Self {
        self
    }

    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.mysql
    }
}

#[cfg(feature = "sqlite")]
impl OnConnection for SqliteConnection {
    type Database = Sqlite;

    fn connection(&mut self) -> &mut Self {
        self
    }

    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.sqlite
    }
}

#[cfg(feature = "any")]
impl OnConnection for AnyConnection {
    type Database = Any;

    fn connection(&mut self) -> &mut Self {
        self
    }

    /// The insert of the database the connection reaches: `Any` passes a statement to its
    /// backend as written.
    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.for_backend(self.backend_name())
    }
}

/// The error of a generated insert on a connection whose database the derive built no statement
/// for. Machinery; the derive calls it.
#[doc(hidden)]
#[must_use]
pub fn no_insert(row: &'static str) -> Error {
    Error::Configuration(
        format!("`{row}` has no generated insert for this connection's database").into(),
    )
}

#[cfg(all(test, feature = "any"))]
mod tests {
    use sqlx::Database;

    use super::{Dialect, InsertSql, MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND, any_dialect};

    #[test]
    fn the_backend_names_are_those_sqlx_reports() {
        assert_eq!(POSTGRES_BACKEND, <sqlx::Postgres as Database>::NAME);
        assert_eq!(MYSQL_BACKEND, <sqlx::MySql as Database>::NAME);
        assert_eq!(SQLITE_BACKEND, <sqlx::Sqlite as Database>::NAME);
    }

    #[test]
    fn an_any_backend_takes_the_dialect_of_its_database() {
        let picked = |backend| any_dialect(backend).map(Dialect::name);
        #[cfg(feature = "postgres")]
        assert_eq!(picked(POSTGRES_BACKEND), Some("postgres"));
        #[cfg(feature = "mysql")]
        assert_eq!(picked(MYSQL_BACKEND), Some("mysql"));
        #[cfg(feature = "sqlite")]
        assert_eq!(picked(SQLITE_BACKEND), Some("sqlite"));
        assert_eq!(
            picked("MSSQL"),
            None,
            "a backend without a built-in dialect"
        );
    }

    #[test]
    fn an_any_backend_runs_the_insert_of_its_database() {
        let sql = InsertSql {
            postgres: Some("INSERT .. ($1)"),
            mysql: Some("INSERT .. (?)"),
            sqlite: None,
        };
        assert_eq!(sql.for_backend(POSTGRES_BACKEND), Some("INSERT .. ($1)"));
        assert_eq!(sql.for_backend(MYSQL_BACKEND), Some("INSERT .. (?)"));
        assert_eq!(
            sql.for_backend(SQLITE_BACKEND),
            None,
            "the derive was built without that dialect"
        );
        assert_eq!(sql.for_backend("MSSQL"), None);
    }
}
