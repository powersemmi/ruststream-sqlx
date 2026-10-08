# The transactional outbox

```
# #[cfg(all(feature = "outbox", feature = "postgres"))]
# mod demo {
use std::convert::Infallible;
use std::io;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream_sqlx::{Outbox, outbox};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnection, PgPool, Postgres};

// outbox: id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, payload BYTEA NOT NULL,
// processed_at TIMESTAMPTZ
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

/// What `#[ruststream::app]` runs.
pub fn app() -> impl App {
    let tracking = outbox! {
        "orders" => OrderOutbox,
    };
    let registry = tracking.clone();

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| {
            let pool = PgPool::connect("postgres://localhost/orders")
                .await
                .map_err(io::Error::other)?;
            registry.set_pool(pool.clone()).map_err(io::Error::other)?;
            Ok::<_, io::Error>(pool)
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
# }
# fn main() {}
```

The outbox keeps what a service publishes in a table of its own until a consumer has processed
it. The publish middleware writes a record of each message published under a registered name,
then sends the message with the record's id in the
[`x-ruststream-outbox-id`](outbox::OUTBOX_ID_HEADER) header. The subscription middleware takes a
delivery that carries an id into work, runs the handler, and marks the record processed when the
handler acknowledges. At startup the republish sends every unprocessed record again. The outbox
works over any RustStream broker and needs no broker of its own: the example above runs on
`MemoryBroker`, and `examples/outbox` in the repository runs the same service over Redis Pub/Sub.

The `outbox` feature turns it on, beside a driver feature (`postgres`, `mysql`, `sqlite` or
`any`). It is independent of the inbox; a service may enable both.

## The guarantee

A message is delivered at least once. The record is written before the message is sent. A send
that fails, a consumer that stops before its handler finishes, or a handler that asks for the
message again leaves the record unprocessed, and the next startup sends it again. The consumer
then takes the record by its id, so a copy whose record is processed already is acknowledged
without running the handler. Two instances of a service that start together may both send the
same record again; a consumer that must not process a message twice checks that itself.

## The record and the registry

[`#[derive(Outbox)]`](derive@Outbox) describes the table: `id`, `name` and `payload` are required
roles, `headers` and `processed_at` are optional. The service writes the record itself, in
[`outbox::Publish`], because its statement decides how the name, the payload and the headers are
laid out. The registry runs the other events from statements it builds when the record type is
registered, from the table's description and the database's built-in dialect.

The same record may be described by hand, and registered under a name that is a type:

```
# #[cfg(all(feature = "outbox", feature = "postgres"))]
# mod demo {
# use ruststream::OutgoingMessage;
# use ruststream::memory::prelude::*;
# use serde::{Deserialize, Serialize};
# use sqlx::postgres::{PgConnection, PgPool, Postgres};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{self, Outbox, OutboxSpec, OutboxTable, TrackedName};

#[derive(sqlx::FromRow)]
struct OrderOutbox {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for OrderOutbox {
    type Id = i64;
    type Table = OutboxSpec<(ProcessedAt,)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .processed_at(Column::new("processed_at"));

    fn id(&self) -> &i64 {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}
# impl outbox::Publish<Postgres> for OrderOutbox {
#     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
#         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
#             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
#     }
# }
# #[derive(Deserialize)] struct PlaceOrder { id: u64 }
# #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] struct OrderPlaced { id: u64 }
# #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
# #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }

/// The name `OrderPlaced` is published under.
struct Orders;

impl TrackedName for Orders {
    const NAME: &'static str = "orders";
}

pub fn app(pool: PgPool) -> impl App {
    let tracking = Outbox::new(pool).track::<OrderOutbox, Orders>();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place).out_reply(Publish);
            b.include(fulfil);
            b.after_startup(Publish, tracking.republish());
        })
}
# }
# fn main() {}
```

A record described by hand implements [`OutboxTable`]. [`OutboxSpec::new`] takes the table and
its three required columns, and `type Table` lists the typed settings in the order the chain sets
them: [`Headers`](outbox::spec::Headers), [`ProcessedAt`](outbox::spec::ProcessedAt) and the
events of [`outbox::spec::own`]. A record with `Headers` implements [`HeaderRow`] for the field
that holds the column. `processed_at` takes no time type, because the outbox writes it from the
database's clock.

[`outbox!`] registers record types under the names they track, and a name written twice does not
compile. [`Outbox::track`] registers one under a name that is a type, a
[`TrackedName`](outbox::TrackedName), and a name tracked twice stops the build. [`Outbox::register`]
takes the name as a string, for a name read from configuration, and panics on a repeated name when
the app is built. One record type may track several names; each registration republishes only its
own records.

## The two middlewares

[`Outbox::layer`] is mounted with `RustStream::layer` and [`Outbox::publish_layer`] with
`RustStream::publish_layer`. A message whose name is not registered passes both after a
comparison of names. A handler's outcome settles a tracked delivery's record:

| Outcome | Event | Default statement |
| --- | --- | --- |
| acknowledged | [`outbox::Ack`] | marks the record processed |
| asked again (`retry`, `retry_after`) | [`outbox::Retry`] | none: the record waits for the next startup |
| dropped | [`outbox::Discard`] | marks the record processed |

A record whose fetch finds it processed already acknowledges the delivery without the handler. A
fetch that fails retries the delivery without the handler. A settlement that fails logs a warning
and keeps the handler's outcome; the record stays for the next startup.

## The republish

`b.after_startup(policy, tracking.republish())` publishes every unprocessed record through the
broker's publisher once the subscriptions are open, each with its id.
[`Outbox::republish_names`] republishes only the names it lists, for a service whose names live on
several brokers. A record the republish cannot read or send fails startup.

## Publishing outside a handler

```
# #[cfg(all(feature = "outbox", feature = "postgres"))]
# mod demo {
# use ruststream::OutgoingMessage;
# use ruststream::memory::prelude::*;
# use ruststream_sqlx::{Outbox, outbox};
# use serde::{Deserialize, Serialize};
# use sqlx::postgres::{PgConnection, PgPool, Postgres};
# #[derive(Outbox, sqlx::FromRow)]
# #[outbox(table = "outbox")]
# struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
# impl outbox::Publish<Postgres> for OrderOutbox {
#     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
#         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
#             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
#     }
# }
#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct OrderPlaced {
    id: u64,
}

pub async fn serve(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    let broker = MemoryBroker::new().bindable();
    let egress = broker.bind(Publish);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .publish_layer(tracking.publish_layer())
        .with_broker(broker, |_b| {});
    let running = app.start().await?;

    // An HTTP endpoint publishes through this one: the record is written before the send.
    let publisher = tracking.wrap(running.publisher(egress).await?);
    publisher.message(&OrderPlaced { id: 7 }).publish().await?;

    running.shutdown().await?;
    Ok(())
}
# }
# fn main() {}
```

The publish pipeline reaches what handlers publish. A publisher from `RunningApp::publisher` or an
`after_startup` hook runs outside it, so [`Outbox::wrap`] gives it the same tracking.

## The pool

Inside `#[ruststream::app]` the app is built before the Tokio runtime exists, and sqlx builds a
pool only inside one. There the registry starts without a pool (`outbox!` without `pool:`, or
[`Outbox::deferred`]), and `on_startup` builds the pool and hands it over with
[`Outbox::set_pool`], as the first example does. An app built inside a runtime passes the pool
directly: `outbox! { pool: pool, .. }` or [`Outbox::new`]. Every handle the registry gave out
reads the same pool. The service closes the pool in `after_shutdown`, which runs once the
handlers have finished and the brokers have stopped.

## Events of the service's own

A record names the events it writes itself: `#[outbox(custom(fetch, ack, retry, discard,
recover))]` on the derive, or `.own::<own::Ack>()` and the others by hand, each with its marker of
[`outbox::spec::own`] in the type. It implements each named event's trait, and the registry runs
it in place of the default. The example service in `examples/outbox` takes its records with a
fetch of its own that also stamps `taken_at`. A record type that names an event and lacks its
trait does not register, and the error names the missing trait.

## Beside the inbox

The outbox's middlewares wrap the handlers of an inbox subscription as they wrap any other. A
tracked publish takes a connection of its own for the record's insert, so a record written while a
[transactional](#transactional-mode) handler runs commits on its own, whatever the delivery's
outcome. A message tracked into an inbox table carries its record id where the table keeps
headers: a `headers` column that the service's `Publish` impl writes.

## Testing

A test build of the service (the `testing` feature) leaves the outbox off: the middlewares pass
every message untouched and the republish sends nothing, so a test needs no outbox table and no
database for it. `RUSTSTREAM_SQLX_OUTBOX=on` in the environment turns it on for the whole test
process; the variable is read once, the first time the outbox runs. A production build always
tracks, and the switch is compiled out.

## Costs

A message whose name is not registered costs one comparison per registered name and nothing else:
no allocation and no database call. A tracked publish takes a connection for the record's insert
and allocates the id header's value. A tracked delivery takes a connection for the fetch, returns
it before the handler runs, so the handler can publish tracked messages of its own, and takes one
again for the mark. Under sqlx's default `test_before_acquire`, each of these acquires pings the
connection first, a round trip a pool built with `test_before_acquire(false)` skips. The default
`Retry` takes no connection. The republish allocates its body once per startup.
