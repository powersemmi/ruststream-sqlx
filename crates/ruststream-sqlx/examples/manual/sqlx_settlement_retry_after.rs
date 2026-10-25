//! The service of `examples/sqlx_settlement_retry_after.rs` without the macros: `.retry_after(..)`
//! names the column, and `RetryAfter<Time>` goes into the description's type.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_settlement_retry_after --features inbox,sqlite,chrono,testing
//! ```

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::InboxContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:retry_after]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `webhook_jobs`: a claim takes it only once its `retry_after` has come.
#[derive(sqlx::FromRow)]
struct Webhook {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Webhook {
    type Id = i64;
    type Table = InboxSpec<(
        Attempt,
        RetryAfter<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("webhook_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .retry_after(Column::new("retry_after"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for Webhook {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Webhook {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Notify;

impl Handle<Call, (), (), InboxContext> for Notify {
    async fn handle(
        &self,
        call: &Call,
        _outs: &(),
        ctx: &mut Context<'_, InboxContext>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        match post(call, attempt).await {
            Answer::Done => Ok(()),
            // The row stays out of the queue as long as the endpoint asked, then a claim takes it
            // again.
            Answer::TooManyRequests { retry_after } => {
                Err(HandlerOutcome::retry_after(retry_after))
            }
        }
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("webhooks", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Webhook>("webhooks"), |b| {
            let notify = subscriber("webhooks", Notify)
                .map_source(|_| InboxQueue::<Webhook>::new("webhooks"));
            b.include(notify.build());
        })
}
// --8<-- [end:retry_after]

/// What the endpoint answered: done, or "too many requests" with the `Retry-After` it sent.
enum Answer {
    Done,
    TooManyRequests { retry_after: Duration },
}

/// How long the endpoint asks a rate-limited caller to wait.
const RETRY_AFTER: Duration = Duration::from_secs(1);

/// A stand-in for the endpoint: it rate-limits the first attempt of every call.
async fn post(call: &Call, attempt: Option<u64>) -> Answer {
    tokio::task::yield_now().await;
    tracing::info!(url = %call.url, ?attempt, "calling");
    if attempt == Some(1) {
        Answer::TooManyRequests {
            retry_after: RETRY_AFTER,
        }
    } else {
        Answer::Done
    }
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:webhooks.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS webhook_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Webhook {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO webhook_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "webhooks")]
struct Call {
    url: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Call, RETRY_AFTER, SCHEMA, service};

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

    // The test runs on the real clock and lets the endpoint's second pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rate_limited_call_comes_back_when_its_wait_is_over() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_sqlx_settlement_retry_after").await?;
        let tb = TestApp::start_live(service(&url)).await?;
        let call = Call {
            url: "https://hooks.example.com/orders".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&call)
            .publish()
            .await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("webhooks")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY_AFTER));

        tb.advance(RETRY_AFTER).await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("webhooks")
            .assert_called(2)
            .with(&call)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
