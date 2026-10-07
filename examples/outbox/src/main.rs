//! Orders and refunds over Redis Pub/Sub, tracked by a transactional outbox.
//!
//! `place` and `cancel` answer commands on `checkout` and `cancellations` with messages to
//! `orders` and `refunds`. The publish middleware records each of them in the `outbox` table
//! before it is sent, and the message carries its record's id in a header. `fulfil` and `refund`
//! consume them: the subscription middleware takes the record into work by that id and marks it
//! processed once the handler acknowledged it. At startup every record without a mark is published
//! again, so a message whose consumer never finished is not lost.
//!
//! The service owns the table: `migrations/0001_outbox.sql` creates it, and `sqlx::migrate!` runs
//! it in `on_startup`, where the pool is built. The pool reads its settings from the `PGHOST`,
//! `PGPORT`, `PGUSER`, `PGPASSWORD` and `PGDATABASE` environment variables, and the broker connects
//! to `REDIS_URL` (`redis://localhost:6379` when it is not set).
//!
//! ```text
//! PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=postgres \
//! REDIS_URL=redis://localhost:6379 cargo run -p outbox-example
//!
//! redis-cli PUBLISH checkout '{"id":1}'
//! redis-cli PUBLISH cancellations '{"id":1}'
//! ```

use std::convert::Infallible;
use std::env;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_fred::pubsub::prelude::*;
use ruststream_sqlx::{Outbox, outbox};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};
use uuid::Uuid;

/// One outbox record for both channels: they differ by the `name` column.
///
/// The derive writes the mark after an ack and the selection of unmarked records for the
/// republish. Taking a record into work is the service's own statement, hence `custom(fetch)`.
#[derive(Outbox, sqlx::FromRow)]
#[outbox(table = "outbox", custom(fetch))]
struct OrderOutbox {
    #[field(id)]
    id: Uuid,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    // FIXME(ruststream-sqlx 0.7.0, `derive(Outbox)`): the derive names this column in its
    // statements but never reads the field, so rustc reports it unread. Goes when the derive
    // reads the `processed_at` field.
    #[expect(dead_code, reason = "the database writes the mark")]
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
}

/// The record is written when the message is published, and its id rides a header.
impl outbox::Publish<Postgres> for OrderOutbox {
    async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<Uuid> {
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO outbox (id, name, payload) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(msg.name())
            .bind(msg.payload())
            .execute(conn)
            .await?;
        Ok(id)
    }
}

/// A record is taken into work only while it is unprocessed, and the service notes when.
/// `None` means it was processed already, and the handler does not run.
impl outbox::Fetch<Postgres> for OrderOutbox {
    async fn fetch(conn: &mut PgConnection, id: &Uuid) -> sqlx::Result<Option<Self>> {
        sqlx::query_as(
            "UPDATE outbox SET taken_at = now()
             WHERE id = $1 AND processed_at IS NULL
             RETURNING id, name, payload, processed_at",
        )
        .bind(id)
        .fetch_optional(conn)
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

#[derive(Deserialize)]
struct CancelOrder {
    id: u64,
}

#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "refunds")]
struct RefundRequested {
    id: u64,
}

/// Accepts an order; the reply to `orders` is recorded in the outbox.
#[subscriber(RedisPubSub::new("checkout"), reply)]
async fn place(cmd: &PlaceOrder) -> OrderPlaced {
    OrderPlaced { id: cmd.id }
}

/// Accepts a cancellation; the reply to `refunds` is recorded too.
#[subscriber(RedisPubSub::new("cancellations"), reply)]
async fn cancel(cmd: &CancelOrder) -> RefundRequested {
    RefundRequested { id: cmd.id }
}

/// Fulfils an order; after the ack its record is marked processed.
#[subscriber(RedisPubSub::new("orders"))]
async fn fulfil(order: &OrderPlaced) -> HandlerOutcome {
    println!("fulfilling order {}", order.id);
    HandlerOutcome::ack()
}

/// Refunds an order; its record is marked the same way.
#[subscriber(RedisPubSub::new("refunds"))]
async fn refund(request: &RefundRequested) -> HandlerOutcome {
    println!("refunding order {}", request.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let redis = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_owned());
    // No pool yet: a sqlx pool needs the async runtime, which starts after this builder.
    // `on_startup` builds it and hands it over, and every handle taken from `tracking` sees it.
    let tracking = outbox! {
        "orders" => OrderOutbox,
        "refunds" => OrderOutbox,
    };
    let registry = tracking.clone();

    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| {
            let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
            sqlx::migrate!().run(&pool).await?;
            registry
                .set_pool(pool.clone())
                .expect("the outbox is deferred and gets one pool");
            Ok::<_, sqlx::Error>(pool)
        })
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        // The brokers are down and the handlers drained by now, so nothing uses the pool.
        .after_shutdown(async move |pool| {
            pool.close().await;
            Ok::<_, Infallible>(())
        })
        .with_broker(RedisBroker::standalone(redis), |b| {
            b.include(place).out_reply(Publish::default());
            b.include(cancel).out_reply(Publish::default());
            b.include(fulfil);
            b.include(refund);
            // The subscriptions are open by now: the unmarked records of both names go out again.
            b.after_startup(Publish::default(), tracking.republish());
        })
}
