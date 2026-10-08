<h1 align="center">ruststream-sqlx</h1>

<p align="center">
  <i>SQL databases for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework through sqlx: a transactional outbox over any RustStream broker, and task queues in Postgres, MySQL/MariaDB and SQLite tables.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sqlx/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sqlx/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-sqlx"><img src="https://img.shields.io/crates/v/ruststream-sqlx.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-sqlx"><img src="https://img.shields.io/crates/dr/ruststream-sqlx" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-sqlx"><img src="https://img.shields.io/docsrs/ruststream-sqlx" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-sqlx/">Documentation</a></b>
</p>

---

`ruststream-sqlx` connects a RustStream service to SQL databases over
[`sqlx`](https://crates.io/crates/sqlx). Handlers, routing, codecs and middleware come from the
framework; this crate brings two components: the inbox, a broker that serves task queues from the
service's own tables, and the transactional outbox, which tracks what a service publishes on any
RustStream broker.

## Features

- **Task queues in the service's own tables** on Postgres, MySQL 8.0.1 and later, MariaDB 10.6
  and later, and SQLite, or through an `AnyPool`.
- **Three claim forms:** a row lock held in a transaction while the handler runs, a lease
  committed at once and extended while the handler works, or an advisory lock on the row's key.
  SQLite tables take the lease or the advisory lock form.
- **Transactional mode:** `.transactional()` lets a handler write through its delivery's
  transaction, and the acknowledgement commits those writes together with the row.
- **Column roles** that each turn on one behaviour: groups, FIFO keys, delayed retries, attempt
  caps with dead letters, a processed mark, and the database's clock in place of the host's.
- **Row mode, headers layouts and batches:** a handler takes the decoded payload, the row itself,
  a struct joined from other tables, or the rows of one claim as a slice.
- **A derive or the manual API:** `#[derive(Inbox)]` or a trait with a typed builder, checked as
  strictly by the compiler and running the same statements.
- **SQL checked at compile time:** `checked` hands the derive's statements to sqlx's macros, so
  `cargo sqlx prepare` checks them with the service's own queries.
- **The service's own SQL** for any queue event, and a dialect of its own for another database.
- **Waking on publish:** a publish wakes the subscriptions of its table in the same process, and
  `LISTEN/NOTIFY` on Postgres wakes them from other processes.
- **The transactional outbox over any RustStream broker:** `#[derive(Outbox)]` describes the
  service's outbox table, and `outbox!` registers it under the names it tracks. A publish
  middleware records each tracked message before it is sent, with the record's id in a header. A
  subscription middleware takes the record into work by that id and marks it processed when the
  handler acknowledges. Unprocessed records are published again at startup, so each message is
  delivered at least once.
- **Tests on the production app:** `TestApp` runs the service's own app against a real database;
  SQLite needs no server.

## Inbox or outbox

The inbox fits work that belongs to the service's data: a task written in the same transaction as
the data it serves, a queue in the database the service already runs. The outbox fits messages a
service publishes on another broker and must not lose. A service may use both: the outbox's
middlewares wrap inbox handlers as they wrap any other.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "outbox", "postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }

[dev-dependencies]
ruststream-sqlx = { version = "0.7", features = ["testing"] }
```

`inbox` and `outbox` turn the components on, each independently of the other. A driver feature
picks the database: `postgres`, `mysql` (MySQL and MariaDB), `sqlite`, or `any`. Optional
features: `chrono` or `time` for time columns, `json` for a headers column, and `asyncapi`.

## Write a service

A queue in a Postgres table, served by the inbox:

```rust
use std::error::Error;

use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for SendEmail {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Outgoing)]
struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    println!("sending to {}", email.to);
    HandlerOutcome::ack()
}

fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        SqlxBroker::new(pool).route::<SendEmail>("emails"),
        |b| {
            b.include(send);
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
    app(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}
```

sqlx builds a pool only inside a Tokio runtime, so `main` builds it and hands it to the app.

Messages published on any broker, tracked by the outbox:

```rust
use std::convert::Infallible;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream_sqlx::{Outbox, outbox};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};

#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "outbox")]
struct OrderOutbox {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
}

impl outbox::Publish<Postgres> for OrderOutbox {
    async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

#[derive(Deserialize)]
struct PlaceOrder {
    id: u64,
}

#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    id: u64,
}

#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: cmd.id }
}

#[subscriber("orders")]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    println!("fulfilling order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let tracking = outbox! {
        "orders" => OrderOutbox,
    };
    let registry = tracking.clone();

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| {
            let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
            registry
                .set_pool(pool.clone())
                .expect("the outbox is deferred and gets one pool");
            Ok::<_, sqlx::Error>(pool)
        })
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .after_shutdown(async move |pool| {
            pool.close().await;
            Ok::<_, Infallible>(())
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            b.include(fulfil);
            b.after_startup(Publish, tracking.republish());
        })
}
```

`#[ruststream::app]` generates `main` and builds the app before the runtime starts, so the outbox
starts without a pool and `on_startup` hands it one with `set_pool`. Both services are in
[`crates/ruststream-sqlx/examples`](./crates/ruststream-sqlx/examples), and `examples/outbox`
runs the outbox over Redis Pub/Sub.

## Test it

`TestApp` runs the service's own app against a real database, because the service's SQL is part
of what the test checks.

```rust
use ruststream::testing::TestApp;

let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
let tb = TestApp::start_live(app(pool)).await?;

tb.broker::<SqlxBroker<Postgres>>()
    .message(&Email { to: "ann@example.com".to_owned() })
    .to("emails")
    .publish()
    .await?;

tb.broker::<SqlxBroker<Postgres>>()
    .subscriber("emails")
    .assert_called_once()
    .settled(HandlerOutcome::ack());
```

A test build leaves the outbox off, so a test needs no outbox table;
`RUSTSTREAM_SQLX_OUTBOX=on` in the environment turns it on.

## Documentation

- This crate: <https://docs.rs/ruststream-sqlx>
- The site, with the benchmarks: <https://powersemmi.github.io/ruststream-sqlx/>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
