//! The service of `examples/sqlx_context_pool.rs` without the macros: the handler's `Handle`
//! impl names `PoolContext`, and the body resolves the pool and the attempt through
//! `FromContext`.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_context_pool --features inbox,sqlite,chrono,testing
//! ```

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::keys::PoolContext;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

// --8<-- [start:pool]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::Sqlite;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `sms_jobs`.
#[derive(sqlx::FromRow)]
struct SendSms {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for SendSms {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("sms_jobs", Column::new("id").generated())
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for SendSms {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for SendSms {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct SendText;

impl Handle<Sms, (), (), PoolContext<Sqlite>> for SendText {
    async fn handle(
        &self,
        sms: &Sms,
        _outs: &(),
        ctx: &mut Context<'_, PoolContext<Sqlite>>,
    ) -> Result<(), HandlerOutcome> {
        let Ctx(pool) = Ctx::<keys::Pool<Sqlite>>::from_context(ctx).await?;
        let Ctx(attempt) = Ctx::<keys::Attempt>::from_context(ctx).await?;
        let sent = deliver(sms, attempt).await;
        // The line commits on its own: a retry leaves it in place.
        let _ = sqlx::query("INSERT INTO delivery_log (phone, sent) VALUES (?, ?)")
            .bind(&sms.phone)
            .bind(sent)
            .execute(&pool)
            .await;
        if sent {
            Ok(())
        } else {
            Err(HandlerOutcome::retry())
        }
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("sms", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<SendSms>("sms"), |b| {
            let send =
                subscriber("sms", SendText).map_source(|_| InboxQueue::<SendSms>::new("sms"));
            b.include(send.build());
        })
}
// --8<-- [end:pool]

/// A stand-in for the SMS gateway: it is busy on the first attempt of every message.
async fn deliver(sms: &Sms, attempt: Option<u64>) -> bool {
    tokio::task::yield_now().await;
    tracing::info!(phone = %sms.phone, ?attempt, "sending a text message");
    attempt != Some(1)
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:sms.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS sms_jobs (
    id INTEGER PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_log (phone TEXT NOT NULL, sent BOOLEAN NOT NULL)";

impl Publish<Sqlite> for SendSms {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO sms_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "sms")]
struct Sms {
    phone: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{SCHEMA, Sms, service};

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
    async fn the_log_keeps_the_attempt_the_retry_answered() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_context_pool").await?;
        let tb = TestApp::start(service(&url)).await?;
        let sms = Sms {
            phone: "+15550100".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&sms)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("sms")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        let log: Vec<bool> = sqlx::query_scalar("SELECT sent FROM delivery_log ORDER BY rowid")
            .fetch_all(&pool)
            .await?;
        assert_eq!(log, [false, true], "both attempts are in the log");
        Ok(())
    }
}
