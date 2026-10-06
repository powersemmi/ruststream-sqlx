//! The queue rows the suites read, a module per form of the queue table.
//!
//! The modules name their rows alike, over the same tables, so a test body reads the form its
//! module imports. Every row but `Unreadable` writes a published message through its generated
//! insert, on each database that insert serves; `Unreadable` writes a row that never decodes.

use sqlx::{Database, Error, Executor};

/// The priority a published ledger entry takes: a row written by hand with a smaller one goes
/// ahead of it.
pub(crate) const PUBLISHED_PRIORITY: i16 = 1;

/// Writes a job of `unreadable_jobs`, whatever the message: an integer where its struct reads
/// bytes, so the row never decodes.
pub(crate) async fn unreadable<DB>(conn: &mut DB::Connection) -> Result<(), Error>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    sqlx::raw_sql("INSERT INTO unreadable_jobs (payload) VALUES (7)")
        .execute(conn)
        .await?;
    Ok(())
}

/// A fetch of the service's own over `plain_jobs`, per database: the rows of `ids`, read with the
/// columns a row's struct names.
///
/// It refuses a list of no ids, as MySQL refuses an empty `IN ()`, so a claim that hands it none
/// fails.
pub(crate) mod own_fetch {
    use sqlx::{AssertSqlSafe, Error, FromRow};
    #[cfg(feature = "mysql")]
    use sqlx::{MySqlConnection, mysql::MySqlRow};
    #[cfg(feature = "postgres")]
    use sqlx::{PgConnection, postgres::PgRow};
    #[cfg(feature = "sqlite")]
    use sqlx::{SqliteConnection, sqlite::SqliteRow};

    fn refuse_none(ids: &[i64]) -> Result<(), Error> {
        if ids.is_empty() {
            return Err(Error::InvalidArgument(
                "the claim handed the service's fetch no ids".to_owned(),
            ));
        }
        Ok(())
    }

    /// The select of MySQL and SQLite, which bind no list as one parameter: one placeholder per id.
    #[cfg(any(feature = "mysql", feature = "sqlite"))]
    fn listed(columns: &str, ids: &[i64]) -> AssertSqlSafe<String> {
        let placeholders = vec!["?"; ids.len()].join(", ");
        AssertSqlSafe(format!(
            "SELECT {columns} FROM plain_jobs WHERE id IN ({placeholders})"
        ))
    }

    #[cfg(feature = "postgres")]
    pub(crate) async fn postgres<Row>(
        conn: &mut PgConnection,
        columns: &str,
        ids: &[i64],
    ) -> Result<Vec<Row>, Error>
    where
        Row: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        refuse_none(ids)?;
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT {columns} FROM plain_jobs WHERE id = ANY($1)"
        )))
        .bind(ids)
        .fetch_all(conn)
        .await
    }

    #[cfg(feature = "mysql")]
    pub(crate) async fn mysql<Row>(
        conn: &mut MySqlConnection,
        columns: &str,
        ids: &[i64],
    ) -> Result<Vec<Row>, Error>
    where
        Row: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
    {
        refuse_none(ids)?;
        let mut select = sqlx::query_as(listed(columns, ids));
        for id in ids {
            select = select.bind(id);
        }
        select.fetch_all(conn).await
    }

    #[cfg(feature = "sqlite")]
    pub(crate) async fn sqlite<Row>(
        conn: &mut SqliteConnection,
        columns: &str,
        ids: &[i64],
    ) -> Result<Vec<Row>, Error>
    where
        Row: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        refuse_none(ids)?;
        let mut select = sqlx::query_as(listed(columns, ids));
        for id in ids {
            select = select.bind(id);
        }
        select.fetch_all(conn).await
    }
}

/// The rows of the row lock form: a claim locks its rows in a transaction their settlements end.
pub(crate) mod row_lock;

/// The rows of the lease form: a claim writes a lease into `locked_until` and commits, and a
/// settlement passes only while the row still holds it.
pub(crate) mod lease;

/// The rows of the advisory lock form: a claim locks each row's key in the delivery's session and
/// takes the row, counting its attempt, and a settlement releases the lock.
pub(crate) mod advisory;
