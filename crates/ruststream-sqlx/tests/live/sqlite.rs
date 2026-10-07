//! The SQLite stand: an in-memory database of the test's own with `schema/sqlite.sql` applied, and
//! the SQL the suites read and write its tables with. It needs no server, so it never skips.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ruststream_sqlx::{Insert, dialect};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{AssertSqlSafe, Connection, FromRow, Sqlite, SqliteConnection};

use super::Database;

/// The database the stand serves.
pub(crate) type Db = Sqlite;

/// The dialect the broker builds the stand's statements with.
pub(crate) const DIALECT: dialect::Sqlite = dialect::Sqlite;

/// A fresh database in memory, shared by every connection that names it.
///
/// An in-memory database ends with its last connection, so the stand holds one open beside the
/// pool until `finish`, and the pool keeps one of its own: a connection the broker closes (a
/// transaction dropped open) never takes the database with it.
///
/// # Panics
///
/// Panics when SQLite refuses to open the database or to apply the schema.
pub(crate) async fn database() -> Option<Database<Db>> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = format!(
        "rs_sqlx_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
    let options: SqliteConnectOptions = url.parse().expect("the stand's URL parses");
    let keeper = SqliteConnection::connect_with(&options)
        .await
        .expect("the database opens");
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await
        .expect("the test database accepts connections");
    sqlx::raw_sql(include_str!("../schema/sqlite.sql"))
        .execute(&pool)
        .await
        .expect("the test schema applies");
    Some(Database {
        pool,
        name,
        url,
        keeper: Some(keeper),
    })
}

impl Database<Db> {
    /// Closes the pool, then the stand's own connection, and the database with it.
    pub(crate) async fn finish(self) {
        self.pool.close().await;
        if let Some(keeper) = self.keeper {
            let _ = keeper.close().await;
        }
    }

    /// The rows of `table` in id order, as `(group or empty, payload, attempt, finished)`.
    pub(crate) async fn email_rows(
        &self,
        table: &'static str,
    ) -> Vec<(String, Vec<u8>, i16, bool)> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT name, payload, attempt, processed_at IS NOT NULL FROM {table} ORDER BY job_id"
        )))
        .fetch_all(&self.pool)
        .await
        .expect("the table reads")
    }

    /// Whether the first email in id order waits: its `retry_after` lies ahead of the database's
    /// clock, read in the layout the column holds.
    pub(crate) async fn email_waits(&self) -> bool {
        sqlx::query_scalar(
            "SELECT retry_after > strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now') FROM email_jobs \
             ORDER BY job_id LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await
        .expect("the row reads")
    }

    /// What the first email in id order keeps for `header`.
    pub(crate) async fn email_header(&self, header: &str) -> Option<String> {
        sqlx::query_scalar("SELECT json_extract(meta, ?) FROM email_jobs ORDER BY job_id LIMIT 1")
            .bind(format!("$.\"{header}\""))
            .fetch_one(&self.pool)
            .await
            .expect("the headers read")
    }

    /// Writes one plain job per payload.
    pub(crate) async fn plain(&self, payloads: &[impl AsRef<[u8]> + Sync]) {
        for payload in payloads {
            sqlx::query("INSERT INTO plain_jobs (payload) VALUES (?)")
                .bind(payload.as_ref())
                .execute(&self.pool)
                .await
                .expect("the row writes");
        }
    }

    /// Writes `count` plain jobs whose payload is their own id as text, and returns the ids.
    pub(crate) async fn plain_ids(&self, count: usize) -> BTreeSet<i64> {
        let count = i64::try_from(count).expect("a count of rows");
        sqlx::query(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?) \
             INSERT INTO plain_jobs (payload) SELECT x'' FROM n",
        )
        .bind(count)
        .execute(&self.pool)
        .await
        .expect("the rows write");
        sqlx::query("UPDATE plain_jobs SET payload = CAST(CAST(id AS TEXT) AS BLOB)")
            .execute(&self.pool)
            .await
            .expect("the rows name themselves");
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM plain_jobs")
            .fetch_all(&self.pool)
            .await
            .expect("the ids read");
        ids.into_iter().collect()
    }

    /// The first plain job's `attempt`, and whether a lease holds it: `locked_until` is set.
    pub(crate) async fn plain_lease(&self) -> (i16, bool) {
        sqlx::query_as(
            "SELECT attempt, locked_until IS NOT NULL FROM plain_jobs ORDER BY id LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await
        .expect("the row reads")
    }

    /// The payloads of `table`, in id order.
    pub(crate) async fn plain_rows(&self, table: &'static str) -> Vec<Vec<u8>> {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT payload FROM {table} ORDER BY id"
        )))
        .fetch_all(&self.pool)
        .await
        .expect("the table reads")
    }

    /// Drops `plain_jobs` and its dead-letter table.
    pub(crate) async fn drop_plain(&self) {
        sqlx::raw_sql("DROP TABLE plain_jobs_dead; DROP TABLE plain_jobs")
            .execute(&self.pool)
            .await
            .expect("the tables drop");
    }

    /// Writes one fragile job per payload and returns their ids.
    pub(crate) async fn fragile(&self, payloads: &[&str]) -> Vec<i64> {
        let mut ids = Vec::new();
        for payload in payloads {
            let id: i64 =
                sqlx::query_scalar("INSERT INTO fragile_jobs (payload) VALUES (?) RETURNING id")
                    .bind(payload.as_bytes())
                    .fetch_one(&self.pool)
                    .await
                    .expect("the job writes");
            ids.push(id);
        }
        ids
    }

    /// Points a reference at the fragile job `id`, so its acknowledgement fails.
    pub(crate) async fn reference(&self, id: i64) {
        sqlx::query("INSERT INTO fragile_refs (job_id) VALUES (?)")
            .bind(id)
            .execute(&self.pool)
            .await
            .expect("the reference writes");
    }

    /// The payloads of the fragile jobs as text, in id order.
    pub(crate) async fn fragile_rows(&self) -> Vec<String> {
        let rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT payload FROM fragile_jobs ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .expect("the table reads");
        rows.into_iter()
            .map(|row| String::from_utf8(row).expect("text"))
            .collect()
    }

    /// The rows of `table`.
    pub(crate) async fn count(&self, table: &'static str) -> i64 {
        sqlx::query_scalar(AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
            .fetch_one(&self.pool)
            .await
            .expect("the table counts")
    }

    /// Writes `mails` through their generated insert, in order.
    pub(crate) async fn mail<Row: Insert<SqliteConnection>>(&self, mails: &[Row]) {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("the pool lends a connection");
        for mail in mails {
            mail.insert(&mut conn).await.expect("the mail writes");
        }
    }

    /// Writes a mail of the queue `name` whose recipient a struct reading it as text cannot read:
    /// bytes that are not text. A NULL would not do on SQLite, where sqlx reads it as empty text.
    pub(crate) async fn unreadable_mail(&self, name: &str) {
        sqlx::query(
            "INSERT INTO mail_jobs (name, recipient, subject) VALUES (?, X'FF', 'unreadable')",
        )
        .bind(name)
        .execute(&self.pool)
        .await
        .expect("the mail writes");
    }

    /// The mails in id order, read as `Row` reads them.
    pub(crate) async fn mails<Row>(&self) -> Vec<Row>
    where
        Row: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        sqlx::query_as("SELECT * FROM mail_jobs ORDER BY job_id")
            .fetch_all(&self.pool)
            .await
            .expect("the mails read")
    }
}
