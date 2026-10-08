# Testing a service on the inbox

```no_run
# #[cfg(all(feature = "sqlite", feature = "chrono", feature = "testing"))]
# mod demo {
# use chrono::{DateTime, Utc};
# use ruststream::OutgoingMessage;
# use ruststream_sqlx::prelude::*;
# use serde::{Deserialize, Serialize};
# use sqlx::SqlitePool;
# #[derive(Inbox, sqlx::FromRow)]
# #[inbox(table = "thumbnail_jobs")]
# pub struct MakeThumbnail { #[field(id, generated)] id: i64, #[field(attempt, generated)] attempt: i16, #[field(locked_until)] locked_until: Option<DateTime<Utc>>, #[field(payload)] payload: Vec<u8> }
# impl Publish<Sqlite> for MakeThumbnail {
#     async fn publish(conn: &mut SqliteConnection, message: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> {
#         sqlx::query("INSERT INTO thumbnail_jobs (payload) VALUES (?)").bind(message.payload()).execute(conn).await?;
#         Ok(())
#     }
# }
# #[derive(Serialize, Deserialize, Outgoing)]
# pub struct Image { path: String }
# #[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
# async fn thumbnail(_: &Image) -> HandlerOutcome { HandlerOutcome::ack() }
# pub fn app(pool: SqlitePool) -> RustStream {
#     RustStream::new(AppInfo::new("thumbnails", "0.1.0"))
#         .with_broker(SqlxBroker::new(pool).route::<MakeThumbnail>("thumbnails"), |b| { b.include(thumbnail); })
# }
use std::error::Error;

use ruststream::testing::TestApp;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Connection, Sqlite, SqliteConnection};

const SCHEMA: &str = "CREATE TABLE thumbnail_jobs (id INTEGER PRIMARY KEY, \
    attempt INTEGER NOT NULL DEFAULT 1, locked_until TEXT, payload BLOB NOT NULL)";

pub async fn a_thumbnail_is_made() -> Result<(), Box<dyn Error + Send + Sync>> {
    // An in-memory database that every connection naming it shares, alive while one of them
    // is open: the test holds one beside the pool.
    let options: SqliteConnectOptions =
        "sqlite:file:thumbnails?mode=memory&cache=shared".parse()?;
    let keeper = SqliteConnection::connect_with(&options).await?;
    let pool = SqlitePoolOptions::new().connect_with(options).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;

    let tb = TestApp::start_live(app(pool)).await?;
    tb.broker::<SqlxBroker<Sqlite>>()
        .message(&Image { path: "cat.png".to_owned() })
        .to("thumbnails")
        .publish()
        .await?;
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("thumbnails")
        .assert_called_once();
    tb.shutdown().await?;
    keeper.close().await?;
    Ok(())
}
# }
# fn main() {}
```

A test runs the service against the database it runs on, because the service's SQL is part of
what the test checks. The message goes in through the route and the service's [`Publish`], as in
production. On Postgres, MySQL and MariaDB a test takes a database of its own on a server, with
the service's migrations applied. SQLite needs no server: a named in-memory database serves every
connection that names it and lives while one of them is open. The clock is real as well: a paused
tokio clock jumps to the next timer while a database reply is in flight, so a test starts with
`TestApp::start_live`, and `tb.advance(by)` lets that much real time pass.

A test build of a service that also tracks its publishes with the outbox leaves the outbox off,
so such a test needs no outbox table. `RUSTSTREAM_SQLX_OUTBOX=on` in the environment turns it on
for the whole test process.
