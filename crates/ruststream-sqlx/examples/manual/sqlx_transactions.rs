//! The service of `examples/sqlx_transactions.rs` without the macros: the handler's `Handle`
//! impl names `TxContext`, resolves the transaction through `FromContext`, and
//! `.transactional()` is a step of the mount chain.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_transactions --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::TxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};

// --8<-- [start:transactional]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `signup_jobs`.
#[derive(sqlx::FromRow)]
struct OpenAccount {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for OpenAccount {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("signup_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for OpenAccount {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for OpenAccount {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The account and its welcome email, written on the connection of the delivery's transaction.
async fn open(
    conn: &mut SqliteConnection,
    signup: &Signup,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    sqlx::query("INSERT INTO accounts (email) VALUES (?)")
        .bind(&signup.email)
        .execute(&mut *conn)
        .await?;
    sqlx::query("INSERT INTO welcome_jobs (payload) VALUES (?)")
        .bind(serde_json::to_vec(&Welcome { to: &signup.email })?)
        .execute(conn)
        .await?;
    Ok(())
}

struct OpenAccountHandler;

impl Handle<Signup, (), (), TxContext<Sqlite>> for OpenAccountHandler {
    async fn handle(
        &self,
        signup: &Signup,
        _outs: &(),
        ctx: &mut Context<'_, TxContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(mut tx) = Ctx::<keys::Tx<Sqlite>>::from_context(ctx).await?;
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        if open(&mut tx, signup).await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        if !register(signup, attempt).await {
            // The retry rolls the account and its welcome email back first.
            return Err(HandlerOutcome::retry());
        }
        // The commit keeps the account and its welcome email, and finishes the signup.
        Ok(())
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("accounts", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<OpenAccount>("signups"), |b| {
            let open_account = subscriber("signups", OpenAccountHandler)
                .map_source(|_| InboxQueue::<OpenAccount>::new("signups"))
                .transactional();
            b.include(open_account.build());
        })
}
// --8<-- [end:transactional]

/// A stand-in for the CRM, which must know an account before its signup is done: it answers the
/// first call for a signup with a timeout.
async fn register(signup: &Signup, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(email = %signup.email, ?attempt, "registering the account");
    attempt != Some(1)
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:accounts.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS signup_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS welcome_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts (email TEXT PRIMARY KEY)";

impl Publish<Sqlite> for OpenAccount {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO signup_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

#[derive(Serialize)]
struct Welcome<'a> {
    to: &'a str,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Signup, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool it reads the table through.
    async fn database(
        name: &str,
    ) -> Result<(String, SqlitePool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let options: SqliteConnectOptions = url.parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    async fn count(pool: &SqlitePool, table: &str) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
            .fetch_one(pool)
            .await
    }

    // The first attempt writes the account and its welcome email, then the CRM times out: the
    // retry discards both, and the second attempt writes them again and commits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_discards_the_writes_and_the_ack_commits_them() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_transactions").await?;
        let tb = TestApp::start(service(&url)).await?;
        let signup = Signup {
            email: "ada@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&signup)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("signups")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        assert_eq!(count(&pool, "accounts").await?, 1, "one account");
        assert_eq!(count(&pool, "welcome_jobs").await?, 1, "one welcome email");
        Ok(())
    }
}
