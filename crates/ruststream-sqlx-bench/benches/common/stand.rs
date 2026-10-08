//! The databases a run measures against, the schema each table gets on them, and the fill.
//!
//! Postgres and MySQL are the compose stand the crate's live suites use, named by the same
//! variables. SQLite is a file of its own in WAL mode, recreated for every run: a database in
//! memory cannot be shared by a pool of more than one connection without SQLite's shared cache,
//! whose table locks are not what a service on a file meets.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{AssertSqlSafe, Connection, MySqlConnection, MySqlPool, PgPool, SqlitePool};

use super::{CUSTOMER, QUANTITY, json_body};

/// The variable that names the Postgres server, the one the live suites read.
pub const POSTGRES: &str = "POSTGRES_TEST_URL";
/// The variable that names the MySQL server, the one the live suites read.
pub const MYSQL: &str = "MYSQL_TEST_URL";
/// The variable that names the SQLite file, when the default under the temporary directory does
/// not suit.
pub const SQLITE: &str = "RUSTSTREAM_BENCH_SQLITE";

/// The MySQL database the benchmark keeps its tables in. The URL the stand gives names none.
const MYSQL_DATABASE: &str = "ruststream_bench";

/// How long a run waits for a connection before it is called stuck.
const ACQUIRE: Duration = Duration::from_secs(30);

/// The tables a scenario reads, by the schema they get.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    RowLock,
    Lease,
    Advisory,
    Named,
    RowMode,
    Outbox,
}

impl Table {
    /// The name the table's description gives it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::RowLock => "bench_row_lock",
            Self::Lease => "bench_lease",
            Self::Advisory => "bench_advisory",
            Self::Named => "bench_named",
            Self::RowMode => "bench_row_mode",
            Self::Outbox => "bench_outbox",
        }
    }

    fn postgres(self) -> &'static str {
        match self {
            Self::RowLock | Self::Advisory | Self::Named => {
                "(id BIGSERIAL PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1, \
                 payload BYTEA NOT NULL)"
            }
            Self::Lease => {
                "(id BIGSERIAL PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1, \
                 locked_until TIMESTAMPTZ, payload BYTEA NOT NULL)"
            }
            Self::RowMode => {
                "(id BIGSERIAL PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1, \
                 customer TEXT NOT NULL, quantity INTEGER NOT NULL)"
            }
            Self::Outbox => {
                "(id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, payload BYTEA NOT NULL, \
                 processed_at TIMESTAMPTZ)"
            }
        }
    }

    fn mysql(self) -> &'static str {
        match self {
            Self::RowLock => {
                "(id BIGINT AUTO_INCREMENT PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1, \
                 payload LONGBLOB NOT NULL)"
            }
            other => panic!("{} is measured on Postgres alone", other.name()),
        }
    }

    fn sqlite(self) -> &'static str {
        match self {
            // Times are text in the layout sqlx writes `chrono` times in, which sorts as the
            // times it holds.
            Self::Lease => {
                "(id INTEGER PRIMARY KEY AUTOINCREMENT, attempt INTEGER NOT NULL DEFAULT 1, \
                 locked_until TEXT, payload BLOB NOT NULL)"
            }
            other => panic!("{} is measured on Postgres alone", other.name()),
        }
    }
}

/// The URL the stand's Postgres answers on.
///
/// # Panics
///
/// Panics when the variable is not set, with the recipe that sets it.
pub fn postgres_url() -> String {
    env::var(POSTGRES).unwrap_or_else(|_| {
        panic!("{POSTGRES} names the server to measure against; `just bench` sets it")
    })
}

/// A pool of up to `connections` on the stand's Postgres, with sqlx's defaults otherwise: what a
/// service gets.
///
/// Lazy, so the pool opens its connections on the runtime that first uses it, which in a
/// code-cost scenario is the measured one.
pub fn postgres_pool(connections: u32) -> PgPool {
    let options: PgConnectOptions = postgres_url().parse().expect("the Postgres URL parses");
    PgPoolOptions::new()
        .max_connections(connections)
        .acquire_timeout(ACQUIRE)
        .connect_lazy_with(options)
}

/// Drops `table` and creates it empty on Postgres.
pub async fn postgres_table(pool: &PgPool, table: Table) {
    let name = table.name();
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP TABLE IF EXISTS {name}; CREATE TABLE {name} {}",
        table.postgres()
    )))
    .execute(pool)
    .await
    .expect("the table is recreated");
}

/// Writes `rows` rows into `table` on Postgres in one statement.
pub async fn postgres_fill(pool: &PgPool, table: Table, rows: usize) {
    let rows = i64::try_from(rows).expect("a row count fits a bigint");
    let name = table.name();
    let filled = if table == Table::RowMode {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {name} (customer, quantity) SELECT $1, $2 FROM generate_series(1, $3)"
        )))
        .bind(CUSTOMER)
        .bind(i32::try_from(QUANTITY).expect("the quantity fits"))
        .bind(rows)
        .execute(pool)
        .await
    } else {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {name} (payload) SELECT $1 FROM generate_series(1, $2)"
        )))
        .bind(json_body())
        .bind(rows)
        .execute(pool)
        .await
    };
    filled.expect("the table is filled");
    // Planner statistics for a table that just went from empty to full, so the claim's plan is
    // the one a table that has been in use for a while gets.
    sqlx::raw_sql(AssertSqlSafe(format!("ANALYZE {name}")))
        .execute(pool)
        .await
        .expect("the table is analyzed");
}

/// Creates the benchmark's own database on the stand's MySQL when it is missing, and returns a
/// pool of up to `connections` on it.
pub async fn mysql_pool(connections: u32) -> MySqlPool {
    let url = env::var(MYSQL).unwrap_or_else(|_| {
        panic!("{MYSQL} names the server to measure against; `just bench` sets it")
    });
    let mut admin = MySqlConnection::connect(&url)
        .await
        .expect("the MySQL server accepts a connection");
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`"
    )))
    .execute(&mut admin)
    .await
    .expect("the benchmark's database exists");
    admin.close().await.expect("the admin connection closes");
    let options: MySqlConnectOptions = url.parse().expect("the MySQL URL parses");
    MySqlPoolOptions::new()
        .max_connections(connections)
        .acquire_timeout(ACQUIRE)
        .connect_lazy_with(options.database(MYSQL_DATABASE))
}

/// Drops `table` and creates it empty on MySQL.
pub async fn mysql_table(pool: &MySqlPool, table: Table) {
    let name = table.name();
    sqlx::raw_sql(AssertSqlSafe(format!("DROP TABLE IF EXISTS {name}")))
        .execute(pool)
        .await
        .expect("the table is dropped");
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {name} {}",
        table.mysql()
    )))
    .execute(pool)
    .await
    .expect("the table is created");
}

/// Writes `rows` rows into `table` on MySQL in one statement.
pub async fn mysql_fill(pool: &MySqlPool, table: Table, rows: usize) {
    let mut conn = pool.acquire().await.expect("the pool lends a connection");
    // A recursive row source stops at 1000 rows unless the session allows more.
    sqlx::raw_sql("SET SESSION cte_max_recursion_depth = 100000000")
        .execute(&mut *conn)
        .await
        .expect("the session allows the fill's depth");
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (payload) WITH RECURSIVE n (x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n \
         WHERE x < ?) SELECT ? FROM n",
        table.name()
    )))
    .bind(i64::try_from(rows).expect("a row count fits a bigint"))
    .bind(json_body())
    .execute(&mut *conn)
    .await
    .expect("the table is filled");
    sqlx::raw_sql(AssertSqlSafe(format!("ANALYZE TABLE {}", table.name())))
        .execute(&mut *conn)
        .await
        .expect("the table is analyzed");
}

/// Where the SQLite file lives.
fn sqlite_path() -> PathBuf {
    env::var_os(SQLITE).map_or_else(
        || env::temp_dir().join("ruststream-sqlx-bench.sqlite"),
        PathBuf::from,
    )
}

/// A fresh SQLite file in WAL mode with `table` in it, and a pool of up to `connections` on it.
///
/// `synchronous = NORMAL` is what a service in WAL mode usually runs: a commit is durable once
/// the log is, not once the database file is.
pub async fn sqlite_pool(connections: u32, table: Table) -> SqlitePool {
    let path = sqlite_path();
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.clone().into_os_string();
        file.push(suffix);
        // Absent on the first run, which is not an error here.
        let _ = std::fs::remove_file(file);
    }
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(ACQUIRE);
    let pool = SqlitePoolOptions::new()
        .max_connections(connections)
        .acquire_timeout(ACQUIRE)
        .connect_with(options)
        .await
        .expect("the SQLite file opens");
    sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {} {}",
        table.name(),
        table.sqlite()
    )))
    .execute(&pool)
    .await
    .expect("the table is created");
    pool
}

/// Writes `rows` rows into `table` on SQLite in one statement.
pub async fn sqlite_fill(pool: &SqlitePool, table: Table, rows: usize) {
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO {} (payload) WITH RECURSIVE n (x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n \
         WHERE x < ?) SELECT ? FROM n",
        table.name()
    )))
    .bind(i64::try_from(rows).expect("a row count fits a bigint"))
    .bind(json_body())
    .execute(pool)
    .await
    .expect("the table is filled");
}
