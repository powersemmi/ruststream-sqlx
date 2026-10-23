//! The service of `examples/sqlx_wake.rs` without the macros: the table is
//! described with `InboxSpec`, and the two handlers are named types.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_wake --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use ruststream_sqlx::prelude::*;

// --8<-- [start:service]
use ruststream::OutgoingMessage;
use sqlx::{Sqlite, SqliteConnection};

/// One row of `jobs`: the group column holds the name a message was published to.
#[derive(sqlx::FromRow)]
struct Job {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Job {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Job {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The service's own insert: the name goes to the group column, the bytes to the payload.
impl Publish<Sqlite> for Job {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

struct Greet;

impl Handle<Signup, Welcome> for Greet {
    fn handle(
        &self,
        signup: &Signup,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Welcome, HandlerOutcome>> {
        ready(Ok(Welcome {
            email: signup.email.clone(),
        }))
    }
}

struct SendWelcome;

impl Handle<Welcome> for SendWelcome {
    fn handle(
        &self,
        welcome: &Welcome,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %welcome.email, "sending the welcome");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    // The hourly poll reaches a row the service wrote with its own SQL.
    let broker = SqlxBroker::new(pool)
        .poll_interval(Duration::from_secs(3600))
        .route::<Job>("signups");
    RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(broker, |b| {
        let greet = subscriber("signups", Greet).map_source(|_| InboxQueue::<Job>::new("signups"));
        b.include(greet.reply().build())
            .out_reply(Repository::<Job>::default());
        // The welcome's row wakes this subscription: no poll interval in between.
        let send =
            subscriber("welcome", SendWelcome).map_source(|_| InboxQueue::<Job>::new("welcome"));
        b.include(send.build());
    })
}
// --8<-- [end:service]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:signups.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "welcome")]
struct Welcome {
    email: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Signup, Welcome, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool.
    async fn database(name: &str) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_welcome_is_sent_long_before_the_next_poll() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_wake").await?;
        let tb = TestApp::start(service(pool.clone())).await?;
        let signup = Signup {
            email: "ada@example.com".to_owned(),
        };
        // Both subscriptions found the table empty as they opened, and wait their hour.
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&signup)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("signups")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("welcome")
            .assert_called_once()
            .with(&Welcome {
                email: "ada@example.com".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "both rows were acknowledged within the hour");
        Ok(())
    }
}
