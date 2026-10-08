//! Orders tracked by a transactional outbox, over the in-memory broker.
//!
//! The service owns the table:
//!
//! ```sql
//! CREATE TABLE outbox (
//!     id           BIGSERIAL PRIMARY KEY,
//!     name         TEXT NOT NULL,
//!     payload      BYTEA NOT NULL,
//!     processed_at TIMESTAMPTZ
//! );
//! ```
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables. `examples/outbox` in the repository runs the same service
//! over Redis Pub/Sub.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example outbox --features outbox,postgres
//! ```

// --8<-- [start:service]
use std::convert::Infallible;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream_sqlx::{Outbox, outbox};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};

/// One record of the `outbox` table.
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

/// The service writes the record; the outbox sends its id with the message.
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

/// The reply to `orders` is recorded before it is sent.
#[subscriber("checkout", reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: cmd.id }
}

/// After the ack, the record is marked processed.
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
        // sqlx builds a pool only inside the Tokio runtime, which starts after this builder.
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
            // Once the subscriptions are open, the unprocessed records are sent again.
            b.after_startup(Publish, tracking.republish());
        })
}
// --8<-- [end:service]
