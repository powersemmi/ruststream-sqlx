//! Subscriptions by name on SQLite: `#[subscriber("signups")]` reads the table its route leads
//! to, and a reply to `welcome` becomes a row another subscription by name reads.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_subscriptions_names --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:names]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `jobs`: the group column holds the name a row was published to.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct Job {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[subscriber("signups", reply)]
async fn greet(signup: &Signup) -> Welcome {
    Welcome {
        email: signup.email.clone(),
    }
}

#[subscriber("welcome")]
async fn send_welcome(welcome: &Welcome) -> HandlerOutcome {
    tracing::info!(to = %welcome.email, "sending the welcome email");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    // Both names lead into `jobs`, in both directions: a publish writes a row of the name's
    // group, and a subscription by the name reads that group.
    let broker = broker.route::<Job>("signups").route::<Job>("welcome");
    RustStream::new(AppInfo::new("signup", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            b.include(greet);
            b.include(send_welcome);
        })
}
// --8<-- [end:names]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:signup.db?mode=rwc")
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_signup_read_by_name_is_answered_by_a_row_of_the_welcome_group()
    -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("sqlx_subscriptions_names").await?;
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
