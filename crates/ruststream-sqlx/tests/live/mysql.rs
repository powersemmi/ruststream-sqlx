//! The MySQL stand: a database of the test's own with `schema/mysql.sql` applied, and the SQL the
//! suites read and write its tables with. The MariaDB stand reads and writes its tables with the
//! same SQL.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{AssertSqlSafe, Connection, MySql, MySqlConnection};

use super::{Database, url};

/// The database the stand serves.
pub(crate) type Db = MySql;

/// The variable that names the stand: a MySQL URL whose user may create databases.
pub(crate) const URL: &str = "MYSQL_TEST_URL";

/// A fresh database on the stand, or `None` to skip the test.
///
/// # Panics
///
/// Panics when the stand refuses to create or serve the database.
pub(crate) async fn database() -> Option<Database<Db>> {
    database_on(URL).await
}

/// A fresh database on the server `variable` names, or `None` to skip the test.
///
/// # Panics
///
/// Panics when the server refuses to create or serve the database.
pub(crate) async fn database_on(variable: &str) -> Option<Database<Db>> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let url = url(variable)?;
    let name = format!(
        "rs_sqlx_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut admin = MySqlConnection::connect(&url)
        .await
        .expect("the stand accepts a connection");
    sqlx::raw_sql(AssertSqlSafe(format!("CREATE DATABASE `{name}`")))
        .execute(&mut admin)
        .await
        .expect("the stand creates a test database");
    admin.close().await.ok();
    let options: MySqlConnectOptions = url.parse().expect("the stand's URL parses");
    let pool = MySqlPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options.database(&name))
        .await
        .expect("the test database accepts connections");
    sqlx::raw_sql(include_str!("../schema/mysql.sql"))
        .execute(&pool)
        .await
        .expect("the test schema applies");
    Some(Database {
        pool,
        name,
        url,
        keeper: None,
    })
}

impl Database<Db> {
    /// Closes the pool and drops the database.
    pub(crate) async fn finish(self) {
        self.pool.close().await;
        if let Ok(mut admin) = MySqlConnection::connect(&self.url).await {
            let drop = format!("DROP DATABASE `{}`", self.name);
            let _ = sqlx::raw_sql(AssertSqlSafe(drop)).execute(&mut admin).await;
            let _ = admin.close().await;
        }
    }

    /// The rows of `table` in id order, as `(group or empty, payload, attempt, finished)`.
    pub(crate) async fn email_rows(
        &self,
        table: &'static str,
    ) -> Vec<(String, Vec<u8>, i16, bool)> {
        // MySQL has no booleans: `IS NOT NULL` answers an integer, which sqlx reads as a `bool`.
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT name, payload, attempt, processed_at IS NOT NULL FROM {table} ORDER BY job_id"
        )))
        .fetch_all(&self.pool)
        .await
        .expect("the table reads")
    }

    /// Whether the first email in id order waits: its `retry_after` lies ahead of the database's
    /// clock.
    pub(crate) async fn email_waits(&self) -> bool {
        sqlx::query_scalar(
            "SELECT retry_after > UTC_TIMESTAMP(6) FROM email_jobs ORDER BY job_id LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await
        .expect("the row reads")
    }

    /// What the first email in id order keeps for `header`.
    pub(crate) async fn email_header(&self, header: &str) -> Option<String> {
        sqlx::query_scalar(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(meta, ?)) FROM email_jobs ORDER BY job_id LIMIT 1",
        )
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
        let rows = vec!["('')"; count].join(", ");
        sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO plain_jobs (payload) VALUES {rows}"
        )))
        .execute(&self.pool)
        .await
        .expect("the rows write");
        sqlx::query("UPDATE plain_jobs SET payload = CAST(id AS CHAR)")
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
        sqlx::query("DROP TABLE plain_jobs_dead, plain_jobs")
            .execute(&self.pool)
            .await
            .expect("the table drops");
    }

    /// Writes one fragile job per payload and returns their ids.
    pub(crate) async fn fragile(&self, payloads: &[&str]) -> Vec<i64> {
        let mut ids = Vec::new();
        for payload in payloads {
            let written = sqlx::query("INSERT INTO fragile_jobs (payload) VALUES (?)")
                .bind(payload.as_bytes())
                .execute(&self.pool)
                .await
                .expect("the job writes");
            ids.push(i64::try_from(written.last_insert_id()).expect("an id of the table"));
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
}
