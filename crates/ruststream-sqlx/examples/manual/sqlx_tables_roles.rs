//! The service of `examples/sqlx_tables_roles.rs` without the macros: each role is a setter of
//! the `InboxSpec` chain, and the roles a delivery reads off the row are small traits.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_tables_roles --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::future::{Future, ready};

use ruststream::schemars::JsonSchema;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Key, Lease, Payload, ProcessedAt, RetryAfter};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, KeyRow, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

// --8<-- [start:table]
#[derive(sqlx::FromRow)]
struct Ticket {
    id: i64,
    customer: String,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Ticket {
    type Id = i64;
    type Table = InboxSpec<(
        Key,
        RetryAfter<DateTime<Utc>>,
        Attempt,
        ProcessedAt<DateTime<Utc>>,
        Lease<DateTime<Utc>>,
        Payload,
    )>;
    const TABLE: Self::Table = InboxSpec::new("support_tickets", Column::new("id").generated())
        .group(Column::new("queue"))
        .priority(Column::new("urgency"))
        .partition_key(Column::new("customer"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt").generated())
        .processed_at(Column::new("processed_at"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl KeyRow for Ticket {
    type Key = String;

    fn partition_key(&self) -> &String {
        &self.customer
    }
}

impl AttemptRow for Ticket {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Ticket {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Triage;

impl Handle<Question> for Triage {
    fn handle(
        &self,
        question: &Question,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if answer(question) {
            return ready(Ok(()));
        }
        ready(Err(HandlerOutcome::retry_after(Duration::from_secs(300))))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("support", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker.route::<Ticket>("billing"), |b| {
            let triage = subscriber("billing", Triage)
                .map_source(|_| InboxQueue::<Ticket>::new("billing"))
                .workers_by_key(nonzero!(4));
            b.include(triage.build());
        })
}
// --8<-- [end:table]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:support.db?mode=rwc")
}

// The service's own insert fills `urgency` and `customer`; a publish takes their defaults.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS support_tickets (
    id INTEGER PRIMARY KEY,
    queue TEXT NOT NULL,
    urgency INTEGER NOT NULL DEFAULT 5,
    customer TEXT NOT NULL DEFAULT '',
    retry_after TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish writes a ticket into the queue it names.
impl Publish<Sqlite> for Ticket {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO support_tickets (queue, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

fn answer(question: &Question) -> bool {
    question.text.contains("invoice")
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "billing")]
struct Question {
    text: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Question, SCHEMA, service};

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

    /// The ticket's `(attempt, processed)`.
    async fn ticket(pool: &SqlitePool) -> Result<(i64, bool), Box<dyn Error>> {
        Ok(
            sqlx::query_as("SELECT attempt, processed_at IS NOT NULL FROM support_tickets")
                .fetch_one(pool)
                .await?,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_answered_ticket_stays_marked_processed() -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_tables_roles_answered").await?;
        let tb = TestApp::start(service(&url)).await?;
        let question = Question {
            text: "where is my invoice".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&question)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("billing")
            .assert_called_once()
            .with(&question)
            .settled(HandlerOutcome::ack());
        assert_eq!(
            ticket(&pool).await?,
            (2, true),
            "the ticket stays, marked processed"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_open_ticket_waits_five_minutes_with_its_attempt_counted()
    -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("manual_sqlx_tables_roles_open").await?;
        let tb = TestApp::start(service(&url)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&Question {
                text: "the app crashes".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("billing")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(300)));
        assert_eq!(
            ticket(&pool).await?,
            (2, false),
            "the ticket waits, its attempt counted"
        );
        Ok(())
    }
}
