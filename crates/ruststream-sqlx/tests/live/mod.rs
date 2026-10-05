//! The gate the live suites share, a database of its own for each test, and the rows they read.
//!
//! A live test skips when `POSTGRES_TEST_URL` is unset, which keeps `cargo test` usable on a
//! laptop with no stand. The same skip in a job that started the stand would be a lie, so
//! `just test-brokers` and CI set `RUSTSTREAM_REQUIRE_LIVE`, and under it a skip fails, naming
//! what it wanted.

// Each live suite is its own test binary and uses the part of this module its topic needs, so
// what one of them leaves alone is not dead code.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::{HeaderColumn, Inbox, Insert, Publish};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::Json;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool, Postgres};

/// The variable a job sets to say it stood a database up, so skipping past it is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// The variable that names the stand: a Postgres URL whose user may create databases.
pub(crate) const URL: &str = "POSTGRES_TEST_URL";

fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The stand's URL, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and [`URL`] is not: a job that started a stand and lost
/// its address is a broken job, and the tests behind it would pass without running.
pub(crate) fn url() -> Option<String> {
    match std::env::var(URL) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {URL} is unset or empty"
            );
            eprintln!("{URL} is not set; skipping the live suite");
            None
        }
    }
}

/// A database of the test's own on the stand, with `tests/schema.sql` applied.
pub(crate) struct Database {
    /// A pool on the database, the one the test's broker takes.
    pub(crate) pool: PgPool,
    name: String,
    url: String,
}

/// A fresh database, or `None` to skip the test.
///
/// # Panics
///
/// Panics when the stand refuses to create or serve the database.
pub(crate) async fn database() -> Option<Database> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let url = url()?;
    let name = format!(
        "rs_sqlx_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut admin = PgConnection::connect(&url)
        .await
        .expect("the stand accepts a connection");
    sqlx::raw_sql(AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
        .execute(&mut admin)
        .await
        .expect("the stand creates a test database");
    admin.close().await.ok();
    let options: PgConnectOptions = url.parse().expect("the stand's URL parses");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options.database(&name))
        .await
        .expect("the test database accepts connections");
    sqlx::raw_sql(include_str!("../schema.sql"))
        .execute(&pool)
        .await
        .expect("the test schema applies");
    Some(Database { pool, name, url })
}

impl Database {
    /// Closes the pool and drops the database.
    pub(crate) async fn finish(self) {
        self.pool.close().await;
        if let Ok(mut admin) = PgConnection::connect(&self.url).await {
            let drop = format!(r#"DROP DATABASE "{}" WITH (FORCE)"#, self.name);
            let _ = sqlx::raw_sql(AssertSqlSafe(drop)).execute(&mut admin).await;
            let _ = admin.close().await;
        }
    }
}

/// The email queue: a group per name and every role of this phase.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
pub(crate) struct SendEmail {
    #[field(id, generated)]
    pub(crate) job_id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(partition_key)]
    pub(crate) customer: Option<String>,
    #[field(retry_after)]
    pub(crate) retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(processed_at)]
    pub(crate) processed_at: Option<DateTime<Utc>>,
    #[field(headers)]
    pub(crate) meta: Option<Json<BTreeMap<String, String>>>,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl Publish<Postgres> for SendEmail {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let job = Self {
            job_id: 0,
            name: message.name().to_owned(),
            customer: message.headers().get_str("customer").map(str::to_owned),
            retry_after: Utc::now(),
            attempt: 1,
            processed_at: None,
            meta: HeaderColumn::from_headers(message.headers()),
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

/// One queue per table: no group, no time; a finished row is deleted.
#[derive(Debug, Inbox, sqlx::FromRow)]
#[inbox(table = "plain_jobs")]
pub(crate) struct Plain {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl Publish<Postgres> for Plain {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO plain_jobs (payload) VALUES ($1)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// The rows of `table` in id order, as `(group or empty, payload, attempt, finished)`.
pub(crate) async fn email_rows(
    pool: &PgPool,
    table: &'static str,
) -> Vec<(String, Vec<u8>, i16, bool)> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT name, payload, attempt, processed_at IS NOT NULL FROM {table} ORDER BY job_id"
    )))
    .fetch_all(pool)
    .await
    .expect("the table reads")
}

/// The payloads of `table`, in id order.
pub(crate) async fn plain_rows(pool: &PgPool, table: &'static str) -> Vec<Vec<u8>> {
    sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT payload FROM {table} ORDER BY id"
    )))
    .fetch_all(pool)
    .await
    .expect("the table reads")
}
