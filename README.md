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
  <b><a href="https://powersemmi.github.io/ruststream-sqlx/latest/">Documentation</a></b>
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
- **Tests on the production app:** `TestApp::start` runs the service's own app against a real
  database, with a clock the test moves; SQLite in memory needs no server.

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
use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPoolOptions, Postgres};

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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

fn service(options: PgConnectOptions) -> RustStream {
    let broker = SqlxBroker::from_connect_options(options, PgPoolOptions::new());
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
        broker.route::<SendEmail>("emails"),
        |b| {
            b.include(send);
        },
    )
}

#[ruststream::app]
fn app() -> impl App {
    // The options read the `PG*` environment variables.
    service(PgConnectOptions::new())
}
```

`#[ruststream::app]` builds the app before the runtime starts, so the broker records the pool's
settings there and builds the pool when it connects. A service that shares one pool with code of
its own hands it to `SqlxBroker::new(pool)` instead. The whole service is
[`examples/inbox.rs`](./crates/ruststream-sqlx/examples/inbox.rs).

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
    tracing::info!(order = order.id, "fulfilling the order");
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
starts without a pool and `on_startup` hands it one with `set_pool`. The whole service is
[`examples/outbox.rs`](./crates/ruststream-sqlx/examples/outbox.rs), and
[`examples/outbox`](./examples/outbox) runs the outbox over Redis Pub/Sub.

## Test it

`TestApp::start` runs the service's own app in process against a real database, because the
service's SQL is part of what the test checks. SQLite in memory needs no server; on Postgres the
test takes a database of its own.

```rust
use ruststream::testing::TestApp;

let options: PgConnectOptions = std::env::var("DATABASE_URL")?.parse()?;
let tb = TestApp::start(service(options)).await?;

tb.broker::<SqlxBroker<Postgres>>()
    .message(&Email { to: "ann@example.com".to_owned() })
    .to("emails")
    .publish()
    .await?;

tb.broker::<SqlxBroker<Postgres>>()
    .subscriber("emails")
    .assert_called_once()
    .with(&Email { to: "ann@example.com".to_owned() })
    .settled(HandlerOutcome::ack());
```

The harness waits for every publish, and after `tokio::time::pause()` `tb.advance(d)` moves the
queue's clock past a delayed retry or a lease ([testing](https://powersemmi.github.io/ruststream-sqlx/latest/components/testing/)).

A test build leaves the outbox off, so a test needs no outbox table;
`RUSTSTREAM_SQLX_OUTBOX=on` in the environment turns it on.

## Documentation

- [Getting started](https://powersemmi.github.io/ruststream-sqlx/latest/guides/getting-started/) - a queue table, a first handler
  and its test on SQLite, with no server.
- [Guides](https://powersemmi.github.io/ruststream-sqlx/latest/) - enqueueing, retries and dead letters, claim forms, ordering,
  transactional handlers, several instances, relaying, the outbox, testing and production.
- [Components](https://powersemmi.github.io/ruststream-sqlx/latest/components/) - one page per part of the crate: the broker,
  tables, subscriptions, forms, settlement, transactions, time, databases and the outbox.
- [Architecture](https://powersemmi.github.io/ruststream-sqlx/latest/architecture/) - how a queue in a SQL table works and where
  the crate sits between the database and the runtime.
- [Benchmarks](https://powersemmi.github.io/ruststream-sqlx/latest/architecture/benchmarks/) - what the crate costs over a raw sqlx
  loop, and the outbox's cost to an app.
- [API reference](https://docs.rs/ruststream-sqlx) - every item on docs.rs.
- The framework: <https://powersemmi.github.io/ruststream/latest/>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
