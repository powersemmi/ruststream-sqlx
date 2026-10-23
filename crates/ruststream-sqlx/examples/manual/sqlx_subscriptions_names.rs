//! The service of `examples/sqlx_subscriptions_names.rs` without the macros: `subscriber(..)`
//! takes the name itself, and the route leads it into the table described with `InboxSpec`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_subscriptions_names --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:names]
use ruststream_sqlx::prelude::*;
use sqlx::SqlitePool;

/// One row of `jobs`: the group column holds the name a row was published to.
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
        tracing::info!(to = %welcome.email, "sending the welcome email");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    // Both names lead into `jobs`, in both directions: a publish writes a row of the name's
    // group, and a subscription by the name reads that group.
    let broker = SqlxBroker::new(pool)
        .route::<Job>("signups")
        .route::<Job>("welcome");
    RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("signups", Greet).reply().build());
        b.include(subscriber("welcome", SendWelcome).build());
    })
}
// --8<-- [end:names]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:signup.db?mode=rwc").await?;
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
    async fn a_signup_read_by_name_is_answered_by_a_row_of_the_welcome_group()
    -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_subscriptions_names").await?;
        let tb = TestApp::start(service(pool)).await?;
        let signup = Signup {
            email: "ada@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&signup)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("signups")
            .assert_called_once()
            .with(&signup);
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("welcome")
            .assert_called_once()
            .with(&Welcome {
                email: "ada@example.com".to_owned(),
            })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
