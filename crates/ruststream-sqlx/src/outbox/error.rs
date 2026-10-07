//! What can go wrong while the outbox tracks a message.

use std::error::Error as StdError;

use sqlx::Error as SqlError;
use thiserror::Error;

/// A failure of the outbox: a record it could not write or read, or a republish that did not
/// reach the broker.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use ruststream_sqlx::{Outbox, outbox};
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Outbox, sqlx::FromRow)]
/// # #[outbox(table = "outbox")]
/// # pub struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream::runtime::PublishError;
/// use ruststream_sqlx::outbox::{OutboxError, TrackedPublishError};
///
/// pub async fn serve(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     let broker = MemoryBroker::new().bindable();
///     let egress = broker.bind(Publish);
///     let running = RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(broker, |b| {
///             b.include(fulfil);
///         })
///         .start()
///         .await?;
///     let publisher = tracking.wrap(running.publisher(egress).await?);
///
///     // The status an HTTP endpoint answers an order with.
///     let status = match publisher.message(&OrderPlaced { id: 7 }).publish().await {
///         Ok(()) => 202,
///         // Recorded but not sent: the next startup sends it.
///         Err(PublishError::Publish(TrackedPublishError::Publish(_))) => 202,
///         // Neither recorded nor sent: the client sends the order again.
///         Err(PublishError::Publish(TrackedPublishError::Outbox(OutboxError::Record { .. }))) => 503,
///         Err(_) => 500,
///     };
///     tracing::info!(status, "order answered");
///
///     running.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OutboxError {
    /// A message under a registered name was published before the outbox had a pool.
    #[error(
        "the outbox has no pool, so a message under `{name}` cannot be recorded: give it one with \
         `Outbox::new` or with `set_pool` in `on_startup`"
    )]
    NoPool {
        /// The registered name.
        name: &'static str,
    },
    /// The record of a published message was not written; the message was not sent.
    #[error("the outbox did not record a message published under `{name}` in `{record}`")]
    Record {
        /// The registered name.
        name: &'static str,
        /// The record type.
        record: &'static str,
        /// The database's error.
        #[source]
        source: SqlError,
    },
    /// The unprocessed records of a name were not read at startup.
    #[error("the outbox did not read the unprocessed records of `{name}` from `{record}`")]
    Recover {
        /// The registered name.
        name: &'static str,
        /// The record type.
        record: &'static str,
        /// The database's error.
        #[source]
        source: SqlError,
    },
    /// A record read at startup was not published again.
    #[error("the outbox did not publish record {id} of `{name}` in `{record}` again")]
    Republish {
        /// The registered name.
        name: &'static str,
        /// The record type.
        record: &'static str,
        /// The record's id.
        id: String,
        /// The publisher's error.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

/// The outbox's pool was set already: by [`Outbox::new`](super::Outbox::new) or by an earlier
/// [`set_pool`](super::Outbox::set_pool).
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use ruststream_sqlx::{Outbox, outbox};
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Outbox, sqlx::FromRow)]
/// # #[outbox(table = "outbox")]
/// # pub struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use std::io;
///
/// pub fn app() -> impl App {
///     let tracking = outbox! { "orders" => OrderOutbox };
///     let registry = tracking.clone();
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .on_startup(async move |()| {
///             let pool = PgPool::connect("postgres://localhost/orders")
///                 .await
///                 .map_err(io::Error::other)?;
///             // A deferred registry takes this one pool; a second one would fail startup here.
///             registry.set_pool(pool.clone()).map_err(io::Error::other)?;
///             Ok::<_, io::Error>(pool)
///         })
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Error)]
#[error("the outbox's pool is set already; `set_pool` takes one pool, once")]
#[non_exhaustive]
pub struct PoolAlreadySet;

/// A publish through [`wrap`](super::Outbox::wrap) that failed: the outbox did not record the
/// message, or the publisher it wraps did not send it.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use ruststream_sqlx::{Outbox, outbox};
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Outbox, sqlx::FromRow)]
/// # #[outbox(table = "outbox")]
/// # pub struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream::runtime::PublishError;
/// use ruststream_sqlx::outbox::{OutboxError, TrackedPublishError};
///
/// pub async fn serve(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     let broker = MemoryBroker::new().bindable();
///     let egress = broker.bind(Publish);
///     let running = RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(broker, |b| {
///             b.include(fulfil);
///         })
///         .start()
///         .await?;
///     let publisher = tracking.wrap(running.publisher(egress).await?);
///
///     // The status an HTTP endpoint answers an order with.
///     let status = match publisher.message(&OrderPlaced { id: 7 }).publish().await {
///         Ok(()) => 202,
///         // Recorded but not sent: the next startup sends it.
///         Err(PublishError::Publish(TrackedPublishError::Publish(_))) => 202,
///         // Neither recorded nor sent: the client sends the order again.
///         Err(PublishError::Publish(TrackedPublishError::Outbox(OutboxError::Record { .. }))) => 503,
///         Err(_) => 500,
///     };
///     tracing::info!(status, "order answered");
///
///     running.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TrackedPublishError<PublishError> {
    /// The record was not written, so the message was not sent.
    #[error(transparent)]
    Outbox(OutboxError),
    /// The wrapped publisher's error. The record stays unprocessed, and the next startup
    /// publishes it again.
    #[error(transparent)]
    Publish(PublishError),
}
