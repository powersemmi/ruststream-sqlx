//! A queue on an `AnyPool`: the broker picks the built-in dialect of the database the URL names
//! when it connects, and the table holds the types `sqlx::Any` carries.
//!
//! ```text
//! DATABASE_URL=sqlite:mail.db?mode=rwc \
//!     cargo run -p ruststream-sqlx --example sqlx_databases_any --features inbox,any,sqlite,testing
//! ```

use std::env;

use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

use ruststream_sqlx::prelude::*;

// --8<-- [start:service]
use ruststream::OutgoingMessage;
use ruststream_sqlx::QueueDatabase;
use sqlx::any::AnyPoolOptions;
use sqlx::{Any, AnyConnection, Connection};

/// One row of `email_jobs` in the advisory lock form: integers, text and bytes, no time.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", advisory_lock = "email_jobs-{id}")]
struct SendEmail {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i32,
    #[field(payload)]
    payload: Vec<u8>,
}

/// One insert for every database: the derive's own, on the connection the pool hands out.
impl<DB> Publish<DB> for SendEmail
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let job = Self {
            id: 0,
            name: message.name().to_owned(),
            attempt: 1,
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::<Any>::from_url(url, AnyPoolOptions::new());
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = AnyConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendEmail>("emails"), |b| {
            b.include(send);
        })
}

#[ruststream::app]
fn app() -> impl App {
    // An `AnyPool` reaches its driver by the URL's scheme.
    install_default_drivers();
    let url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:mail.db?mode=rwc".to_owned());
    service(&url)
}
// --8<-- [end:service]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS email_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "emails")]
struct Email {
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::any::install_default_drivers;
    use sqlx::{Any, AnyPool, Connection, SqliteConnection};

    use super::{Email, SCHEMA, service};

    // An in-memory SQLite database every connection naming it shares, alive while one of them is
    // open: the test holds one beside the pool, and the pool reaches it through `sqlx::Any`.
    async fn database(name: &str) -> Result<(String, AnyPool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let keeper = SqliteConnection::connect(&url).await?;
        install_default_drivers();
        let pool = AnyPool::connect(&url).await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_any_pool_on_sqlite_sends_the_email() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_databases_any").await?;
        let tb = TestApp::start(service(&url)).await?;
        let email = Email {
            to: "ops@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Any>>()
            .message(&email)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Any>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&email)
            .settled(HandlerOutcome::ack());
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM email_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(left, 0, "the acknowledgement deleted the row");
        Ok(())
    }
}
