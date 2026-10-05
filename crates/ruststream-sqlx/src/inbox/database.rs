//! The databases the inbox runs on.

use std::future::Future;

use futures::TryStreamExt;
use ruststream_sqlx_dialect::Dialect;
use sqlx::{
    Arguments, ColumnIndex, Database, Decode, Encode, Error, Executor, FromRow, IntoArguments,
    SqlStr, Type,
};

use super::InboxRow;
use super::engine::Claimed;

/// A sqlx database the inbox runs on: one that binds the text and the integers the queue's own
/// statements bind, and runs statements on a connection.
///
/// Every database whose sqlx driver does so implements it; Postgres does.
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

    /// Runs a statement. Machinery.
    #[doc(hidden)]
    fn execute<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Runs a claim of whole rows into `out`. Machinery.
    #[doc(hidden)]
    fn fetch_rows<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        out: &'c mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c
    where
        Row: InboxRow + for<'r> FromRow<'r, Self::Row> + Unpin;

    /// Runs a fetch of whole rows. Machinery.
    #[doc(hidden)]
    fn fetch_all<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Vec<Row>, Error>> + Send + 'c
    where
        Row: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c;

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
}

impl<DB> QueueDatabase for DB
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
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
    ) -> Result<(), Error> {
        sqlx::query_with(sql, arguments).execute(conn).await?;
        Ok(())
    }

    async fn fetch_rows<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
        out: &'c mut Vec<Claimed<Row>>,
    ) -> Result<(), Error>
    where
        Row: InboxRow + for<'r> FromRow<'r, Self::Row> + Unpin,
    {
        let mut rows = sqlx::query_as_with::<Self, Row, _>(sql, arguments).fetch(conn);
        while let Some(row) = rows.try_next().await? {
            out.push(Claimed::Row(row));
        }
        Ok(())
    }

    fn fetch_all<'c, Row>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Vec<Row>, Error>> + Send + 'c
    where
        Row: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c,
    {
        sqlx::query_as_with::<Self, Row, _>(sql, arguments).fetch_all(conn)
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
}

/// A database whose dialect is built into this crate, so `SqlxBroker::new` needs no dialect of
/// the service's own. Postgres has one (feature `postgres`).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx::BuiltInDialect;
///
/// assert_eq!(<sqlx::Postgres as BuiltInDialect>::dialect().name(), "postgres");
/// # }
/// ```
pub trait BuiltInDialect: QueueDatabase {
    /// The dialect that builds this database's statements.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx::BuiltInDialect;
    /// use ruststream_sqlx::dialect::{ClaimShape, Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("id"), Form::RowLock);
    /// let claim = <sqlx::Postgres as BuiltInDialect>::dialect().claim(&JOBS, ClaimShape::Ids)?;
    /// assert!(claim.sql().ends_with("FOR UPDATE SKIP LOCKED"));
    /// # }
    /// # Ok::<(), ruststream_sqlx::dialect::StatementError>(())
    /// ```
    fn dialect() -> &'static dyn Dialect;
}

#[cfg(feature = "postgres")]
impl BuiltInDialect for sqlx::Postgres {
    fn dialect() -> &'static dyn Dialect {
        &ruststream_sqlx_dialect::Postgres
    }
}

/// A Postgres connection, for the inserts the derive builds at compile time. Machinery; never
/// named directly.
#[cfg(feature = "postgres")]
#[doc(hidden)]
pub trait OnPostgres: Send {
    /// The connection itself.
    fn connection(&mut self) -> &mut sqlx::PgConnection;
}

#[cfg(feature = "postgres")]
impl OnPostgres for sqlx::PgConnection {
    fn connection(&mut self) -> &mut sqlx::PgConnection {
        self
    }
}
