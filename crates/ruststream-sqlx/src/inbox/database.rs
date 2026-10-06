//! The databases the inbox runs on.

use std::future::Future;

use futures::TryStreamExt;
use sqlx::Row as _;
use sqlx::any::AnyQueryResult;
use sqlx::{
    Arguments, ColumnIndex, Database, Decode, Encode, Error, Executor, IntoArguments, SqlStr, Type,
};

use super::engine::{Claimed, Events, IdAt};
use super::form::advisory::events::Candidates;
use super::queue::Queue;

#[cfg(feature = "any")]
mod any;
pub(crate) mod built_in;
mod insert;

#[cfg(feature = "any")]
pub use any::AnyDialect;
pub use built_in::{BuiltIn, BuiltInDialect};
pub use insert::{InsertSql, OnConnection, no_insert};

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
