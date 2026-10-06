//! The databases the inbox runs on.

use std::future::Future;

use futures::TryStreamExt;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use ruststream_sqlx_dialect as dialect;
use ruststream_sqlx_dialect::{Advisory, Lease};
use sqlx::Row as _;
use sqlx::any::AnyQueryResult;
#[cfg(feature = "any")]
use sqlx::{Any, AnyConnection};
use sqlx::{
    Arguments, ColumnIndex, Database, Decode, Encode, Error, Executor, IntoArguments, SqlStr, Type,
};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};

#[cfg(feature = "any")]
use super::built_in::AnyDialect;
use super::built_in::BuiltIn;
use super::engine::{Candidates, Claimed, Events, IdAt};
use super::queue::Queue;

/// A sqlx database the inbox runs on: one that binds and reads the values the queue's own
/// statements use, runs statements on a connection, and reports the rows a statement changed.
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

    /// Runs a statement that binds nothing as text, unprepared: a savepoint, which MySQL does not
    /// prepare. Machinery.
    #[doc(hidden)]
    fn execute_text<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Runs a claim or a fetch of whole rows of `queue` into `out`: a row its struct does not
    /// decode is [`Claimed::Undecodable`], its id read alone where the queue's select carries it
    /// and its attempt as the struct reads it. Machinery.
    #[doc(hidden)]
    fn fetch_rows<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        queue: &'static Queue,
        out: &'c mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c
    where
        Row: Events<Self>,
        Row::Id: for<'r> Decode<'r, Self> + Type<Self>;

    /// Reads the column `name` of `row` as a `T`. Machinery.
    #[doc(hidden)]
    fn column<T>(row: &Self::Row, name: &str) -> Result<T, Error>
    where
        T: for<'r> Decode<'r, Self> + Type<Self>;

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

    /// Runs a statement whose one row starts with a 64-bit integer, such as the guard a FIFO
    /// claim takes its group with, or the lock and the unlock of the advisory lock form, and
    /// returns whether the integer is nonzero. Machinery.
    #[doc(hidden)]
    fn fetch_flag<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'c;

    /// Runs the advisory claim of candidates into `out`: the id in the first column, the lock key
    /// as text in the second, copied into the buffers `out` keeps. Machinery.
    #[doc(hidden)]
    fn fetch_candidates<'c, Id>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        out: &'c mut Candidates<Id>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c
    where
        Id: for<'r> Decode<'r, Self> + Type<Self> + Send + 'c;

    /// Runs a statement and says whether it returned a row, such as the take of a candidate the
    /// service's own fetch reads. Machinery.
    #[doc(hidden)]
    fn fetch_found<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'c;
}

impl<DB> QueueDatabase for DB
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: Decode<'r, DB>,
    for<'a> i64: Encode<'a, DB> + Decode<'a, DB> + Type<DB>,
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

    async fn execute_text(conn: &mut Self::Connection, sql: &'static str) -> Result<(), Error> {
        // A bare text binds nothing, so sqlx sends it as is, without preparing it.
        conn.execute(sql).await?;
        Ok(())
    }

    async fn fetch_rows<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        queue: &'static Queue,
        out: &'c mut Vec<Claimed<Row>>,
    ) -> Result<(), Error>
    where
        Row: Events<Self>,
        Row::Id: for<'r> Decode<'r, Self> + Type<Self>,
    {
        // The decode `query_as` runs, one row at a time, so a row that fails it fails alone.
        let mut rows = sqlx::query_with::<Self, _>(sql, arguments).fetch(conn);
        while let Some(raw) = rows.try_next().await? {
            match Row::from_row(&raw) {
                Ok(row) => out.push(Claimed::Row(row)),
                Err(error) => {
                    // Why the id alone: a row only its id can name is still a row the policy
                    // settles, and nothing settles a row whose id does not decode either. Its
                    // attempt comes along, so the cap spends such a row as any other.
                    let id = match queue.id_at {
                        IdAt::First => raw.try_get::<Row::Id, _>(0_usize),
                        IdAt::Named(name) => raw.try_get::<Row::Id, _>(name),
                    };
                    match id {
                        Ok(id) => out.push(Claimed::Undecodable {
                            id,
                            attempt: Row::read_attempt(&raw, queue),
                            error: Box::new(error),
                        }),
                        Err(_) => return Err(error),
                    }
                }
            }
        }
        Ok(())
    }

    fn column<T>(row: &Self::Row, name: &str) -> Result<T, Error>
    where
        T: for<'r> Decode<'r, Self> + Type<Self>,
    {
        row.try_get::<T, _>(name)
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

    async fn fetch_flag(
        conn: &mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> Result<bool, Error> {
        let flag: i64 = sqlx::query_scalar_with::<Self, i64, _>(sql, arguments)
            .fetch_one(conn)
            .await?;
        Ok(flag != 0)
    }

    async fn fetch_candidates<'c, Id>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        out: &'c mut Candidates<Id>,
    ) -> Result<(), Error>
    where
        Id: for<'r> Decode<'r, Self> + Type<Self> + Send + 'c,
    {
        let mut rows = sqlx::query_with::<Self, _>(sql, arguments).fetch(conn);
        while let Some(row) = rows.try_next().await? {
            // The key is read where the row holds it, and copied once, into a kept buffer.
            let key = row.try_get::<&str, _>(1_usize)?;
            out.push(row.try_get::<Id, _>(0_usize)?, key);
        }
        Ok(())
    }

    async fn fetch_found(
        conn: &mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> Result<bool, Error> {
        // Every row is read, so the statement has run to its end when the answer comes.
        let mut rows = sqlx::query_with::<Self, _>(sql, arguments).fetch(conn);
        let mut found = false;
        while rows.try_next().await?.is_some() {
            found = true;
        }
        Ok(found)
    }
}

/// A database whose dialect is built into this crate, so `SqlxBroker::new` needs no dialect of
/// the service's own.
///
/// Postgres (feature `postgres`), MySQL with MariaDB (feature `mysql`) and SQLite (feature
/// `sqlite`) have one, [`BuiltIn<DB>`](BuiltIn). `Any` (feature `any`) takes the dialect of the
/// database its pool reaches, among those whose features are on, and the broker picks it when it
/// connects. A row on an `AnyPool` holds only the types `sqlx::Any` carries, and no time is among
/// them, so a table with `locked_until`, `retry_after` or `processed_at` is out of its reach.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "any")]
/// # async fn run(pool: sqlx::AnyPool) -> Result<(), sqlx::Error> {
/// use ruststream_sqlx::BuiltInDialect;
/// use ruststream_sqlx::dialect::Dialect;
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
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no dialect built into the crate",
    label = "no built-in dialect for this database",
    note = "build the broker with a dialect of the service's own: \
            `SqlxBroker::with_dialect(pool, dialect)`, a `SqlxBroker<{Self}, YourDialect>`"
)]
pub trait BuiltInDialect: QueueDatabase {
    /// The dialect of the crate's `dialect` module that builds the database's statements.
    /// Machinery; [`BuiltIn`] holds it.
    #[doc(hidden)]
    type Picked: Lease + Advisory + Copy + 'static;

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
    /// use ruststream_sqlx::dialect::Dialect;
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
    fn dialect(conn: &Self::Connection) -> Option<BuiltIn<Self>>;

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
    type Picked = dialect::Postgres;

    fn dialect(_: &PgConnection) -> Option<BuiltIn<Self>> {
        Some(BuiltIn::new(dialect::Postgres))
    }
}

#[cfg(feature = "mysql")]
impl BuiltInDialect for MySql {
    type Picked = dialect::MySql;

    fn dialect(_: &MySqlConnection) -> Option<BuiltIn<Self>> {
        Some(BuiltIn::new(dialect::MySql))
    }
}

#[cfg(feature = "sqlite")]
impl BuiltInDialect for Sqlite {
    type Picked = dialect::Sqlite;

    fn dialect(_: &SqliteConnection) -> Option<BuiltIn<Self>> {
        Some(BuiltIn::new(dialect::Sqlite))
    }
}

#[cfg(feature = "any")]
impl BuiltInDialect for Any {
    type Picked = AnyDialect;

    fn dialect(conn: &AnyConnection) -> Option<BuiltIn<Self>> {
        AnyDialect::of(conn.backend_name()).map(BuiltIn::new)
    }

    fn backend(conn: &AnyConnection) -> &str {
        conn.backend_name()
    }
}

/// The name an `AnyConnection` reports for a Postgres backend: its sqlx driver's
/// `Database::NAME`.
#[cfg(feature = "any")]
pub(crate) const POSTGRES_BACKEND: &str = "PostgreSQL";

/// The name an `AnyConnection` reports for a MySQL or MariaDB backend.
#[cfg(feature = "any")]
pub(crate) const MYSQL_BACKEND: &str = "MySQL";

/// The name an `AnyConnection` reports for a SQLite backend.
#[cfg(feature = "any")]
pub(crate) const SQLITE_BACKEND: &str = "SQLite";

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

    use super::{InsertSql, MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

    #[test]
    fn the_backend_names_are_those_sqlx_reports() {
        assert_eq!(POSTGRES_BACKEND, <sqlx::Postgres as Database>::NAME);
        assert_eq!(MYSQL_BACKEND, <sqlx::MySql as Database>::NAME);
        assert_eq!(SQLITE_BACKEND, <sqlx::Sqlite as Database>::NAME);
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
