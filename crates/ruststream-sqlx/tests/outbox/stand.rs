//! What the suite needs of a stand's database: its placeholders, the id of the row an insert
//! created, which MySQL reports instead of returning it, and a pool on a database that does not
//! exist.

use std::borrow::Cow;
use std::future::Future;
use std::time::Duration;

use sqlx::mysql::{MySqlArguments, MySqlPoolOptions};
use sqlx::postgres::{PgArguments, PgPoolOptions};
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{AssertSqlSafe, Database, Error, MySql, Pool, Postgres, Row, Sqlite};

/// How long a pool on a database that does not exist tries before it gives up: a test that reaches
/// it fails at once instead of waiting out sqlx's default.
const UNREACHABLE_TIMEOUT: Duration = Duration::from_secs(2);

/// A database the suite runs on.
pub(crate) trait Stand: Database {
    /// `sql`, written with `?` for each parameter in order, in this database's placeholders.
    fn sql(sql: &'static str) -> AssertSqlSafe<Cow<'static, str>>;

    /// `insert`, an `INSERT` of one row written with `?`, as a statement whose run yields the id.
    fn returning_id(insert: &'static str) -> AssertSqlSafe<Cow<'static, str>>;

    /// Runs `insert`, built on [`Stand::returning_id`], and returns the new row's id.
    fn inserted<'c>(
        conn: &'c mut Self::Connection,
        insert: Query<'c, Self, Self::Arguments>,
    ) -> impl Future<Output = Result<i64, Error>> + Send + 'c;

    /// A pool on a database nothing serves, built without I/O: every connection it opens fails.
    fn unreachable() -> Pool<Self>;
}

impl Stand for Postgres {
    fn sql(sql: &'static str) -> AssertSqlSafe<Cow<'static, str>> {
        let mut numbered = String::with_capacity(sql.len() + 8);
        for (parameter, piece) in sql.split('?').enumerate() {
            if parameter > 0 {
                numbered.push('$');
                numbered.push_str(&parameter.to_string());
            }
            numbered.push_str(piece);
        }
        AssertSqlSafe(Cow::Owned(numbered))
    }

    fn returning_id(insert: &'static str) -> AssertSqlSafe<Cow<'static, str>> {
        let AssertSqlSafe(sql) = Self::sql(insert);
        AssertSqlSafe(Cow::Owned(format!("{sql} RETURNING id")))
    }

    async fn inserted<'c>(
        conn: &'c mut Self::Connection,
        insert: Query<'c, Self, PgArguments>,
    ) -> Result<i64, Error> {
        insert.fetch_one(conn).await?.try_get(0)
    }

    fn unreachable() -> Pool<Self> {
        // Port 9 is the discard service, which no stand runs.
        PgPoolOptions::new()
            .acquire_timeout(UNREACHABLE_TIMEOUT)
            .connect_lazy("postgres://ruststream@127.0.0.1:9/ruststream")
            .expect("the URL parses")
    }
}

impl Stand for MySql {
    fn sql(sql: &'static str) -> AssertSqlSafe<Cow<'static, str>> {
        AssertSqlSafe(Cow::Borrowed(sql))
    }

    // MySQL has no `RETURNING`: the insert reports the id it generated instead.
    fn returning_id(insert: &'static str) -> AssertSqlSafe<Cow<'static, str>> {
        Self::sql(insert)
    }

    async fn inserted<'c>(
        conn: &'c mut Self::Connection,
        insert: Query<'c, Self, MySqlArguments>,
    ) -> Result<i64, Error> {
        let id = insert.execute(conn).await?.last_insert_id();
        i64::try_from(id).map_err(|error| Error::Decode(error.into()))
    }

    fn unreachable() -> Pool<Self> {
        MySqlPoolOptions::new()
            .acquire_timeout(UNREACHABLE_TIMEOUT)
            .connect_lazy("mysql://root@127.0.0.1:9")
            .expect("the URL parses")
    }
}

impl Stand for Sqlite {
    fn sql(sql: &'static str) -> AssertSqlSafe<Cow<'static, str>> {
        AssertSqlSafe(Cow::Borrowed(sql))
    }

    fn returning_id(insert: &'static str) -> AssertSqlSafe<Cow<'static, str>> {
        AssertSqlSafe(Cow::Owned(format!("{insert} RETURNING id")))
    }

    async fn inserted<'c>(
        conn: &'c mut Self::Connection,
        insert: Query<'c, Self, SqliteArguments>,
    ) -> Result<i64, Error> {
        insert.fetch_one(conn).await?.try_get(0)
    }

    fn unreachable() -> Pool<Self> {
        // A file in a directory that does not exist, which SQLite cannot create.
        let options = SqliteConnectOptions::new().filename("/nonexistent/ruststream/outbox.db");
        SqlitePoolOptions::new()
            .acquire_timeout(UNREACHABLE_TIMEOUT)
            .connect_lazy_with(options)
    }
}
