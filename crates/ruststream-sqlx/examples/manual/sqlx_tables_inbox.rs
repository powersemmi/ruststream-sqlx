//! The service of `examples/sqlx_tables_inbox.rs` without the macros: the table implements
//! `InboxTable` with an `InboxSpec` chain, and one handler type serves both groups.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_tables_inbox --features inbox,sqlite,chrono,testing,asyncapi
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::{SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:table]
use chrono::{DateTime, Utc};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Attempt, Lease, Payload};
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
use sqlx::Sqlite;

/// One row of `notifications`, described by hand: the columns only the queue reads, such as
/// `channel` and `locked_until`, need no field.
#[derive(sqlx::FromRow)]
struct Notification {
    id: i64,
    attempt: i16,
    payload: Vec<u8>,
}

impl InboxTable for Notification {
    type Id = i64;
    type Table = InboxSpec<(Attempt, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("notifications", Column::new("id").generated())
        .group(Column::new("channel"))
        .attempt(Column::new("attempt").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl AttemptRow for Notification {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

impl PayloadRow for Notification {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct Deliver;

impl Handle<Note> for Deliver {
    fn handle(
        &self,
        note: &Note,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %note.to, channel = ctx.name(), "delivering");
        ready(Ok(()))
    }
}

fn service(pool: SqlitePool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<Notification>("email")
        .route::<Notification>("sms");
    RustStream::new(AppInfo::new("notifier", "0.1.0")).with_broker(broker, |b| {
        for channel in ["email", "sms"] {
            let deliver = subscriber(channel, Deliver)
                .map_source(move |_| InboxQueue::<Notification>::new(channel));
            b.include(deliver.build());
        }
    })
}
// --8<-- [end:table]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:notifications.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY,
    channel TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish writes a row into the group it names.
impl Publish<Sqlite> for Notification {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO notifications (channel, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Note {
    to: String,
    text: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Note, SCHEMA, service};

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
    async fn a_note_reaches_the_subscription_of_its_group() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("manual_sqlx_tables_inbox").await?;
        let tb = TestApp::start(service(pool)).await?;
        let note = Note {
            to: "ops@example.com".to_owned(),
            text: "disk full".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&note)
            .to("email")
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("email")
            .assert_called_once()
            .with(&note)
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("sms")
            .assert_not_called();
        Ok(())
    }
}
