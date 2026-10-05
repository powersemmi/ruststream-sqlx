//! The databases the inbox runs on.

use std::future::Future;

use futures::TryStreamExt;
use ruststream_sqlx_dialect::Dialect;
use sqlx::Row as _;
use sqlx::any::AnyQueryResult;
use sqlx::{
    Arguments, ColumnIndex, Database, Decode, Encode, Error, Executor, FromRow, IntoArguments,
    SqlStr, Type,
};

use super::QueueRow;
use super::engine::{Claimed, IdAt};

/// A sqlx database the inbox runs on: one that binds the text and the integers the queue's own
/// statements bind, runs statements on a connection, and reports the rows a statement changed.
///
/// Every database whose sqlx driver does so implements it; Postgres does. The rows a statement
/// changed come through sqlx's [`AnyQueryResult`], which every driver in sqlx converts its result
/// into; a driver outside sqlx provides that conversion for its own result type.
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
}

impl<DB> QueueDatabase for DB
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
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
