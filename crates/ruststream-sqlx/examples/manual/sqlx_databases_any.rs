//! The service of `examples/sqlx_databases_any.rs` without the macros: the table is described
//! with `InboxSpec`, its `Publish` picks the statement of the backend, and the handler is a named
//! type.
//!
//! ```text
//! DATABASE_URL=sqlite:mail.db?mode=rwc \
//!     cargo run -p ruststream-sqlx --example manual_sqlx_databases_any --features inbox,any,sqlite,testing,asyncapi
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::any::install_default_drivers;

use ruststream_sqlx::prelude::*;

// --8<-- [start:service]
use ruststream::OutgoingMessage;
use ruststream_sqlx::dialect::{Column, KeyPart};
use ruststream_sqlx::spec::{Advisory, Attempt, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use sqlx::any::AnyPoolOptions;
use sqlx::{Any, AnyConnection, Connection, Database, Postgres};

/// One row of `email_jobs` in the advisory lock form: integers, text and bytes, no time.
#[derive(sqlx::FromRow)]
struct SendEmail {
    id: i64,
    attempt: i32,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(Advisory, Attempt, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .advisory(&[KeyPart::Literal("email_jobs-"), KeyPart::Column("id")])
        .attempt(Column::new("attempt").generated())
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SendEmail {
    type Attempt = i32;

    fn attempt(&self) -> &i32 {
        &self.attempt
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// `sqlx::Any` hands a statement to its backend as written, so the insert names the
/// placeholders of the database the connection reaches.
impl Publish<Any> for SendEmail {
    async fn publish(
        conn: &mut AnyConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        let insert = if conn.backend_name() == Postgres::NAME {
            "INSERT INTO email_jobs (name, payload) VALUES ($1, $2)"
        } else {
            "INSERT INTO email_jobs (name, payload) VALUES (?, ?)"
        };
        sqlx::query(insert)
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

struct Mailer;

impl Handle<Email> for Mailer {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %email.to, "sending");
        ready(Ok(()))
    }
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
            let send =
                subscriber("emails", Mailer).map_source(|_| InboxQueue::<SendEmail>::new("emails"));
            b.include(send.build());
        })
}

fn main() -> ExitCode {
    // An `AnyPool` reaches its driver by the URL's scheme.
    install_default_drivers();
    let url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:mail.db?mode=rwc".to_owned());
    run_main(|| service(&url))
}
// --8<-- [end:service]

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS email_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    payload BLOB NOT NULL
)";

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
        let (url, pool, _keeper) = database("manual_sqlx_databases_any").await?;
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
