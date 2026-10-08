//! The transactional outbox: what a service publishes is recorded in its own table until a
//! consumer has processed it, and what was not processed is published again at startup.
//!
//! A struct deriving [`Outbox`](derive@crate::Outbox), or implementing [`OutboxTable`] by hand,
//! describes the table; `outbox!`, [`Outbox::register`] or [`Outbox::track`] registers it under the
//! names it tracks, and the registry hands out the two middlewares and the republish. The events
//! below are what the record's statements do; a service implements the ones it names as its own
//! (`#[outbox(custom(..))]`, or [`OutboxSpec::own`]), and always [`Publish`], which has no
//! default.
//!
//! The crate overview's [transactional outbox](crate#the-transactional-outbox) section shows a
//! whole service and states the guarantee, the test switch and the costs.

mod database;
mod dispatch;
mod error;
mod events;
mod layer;
mod publish;
mod registry;
mod republish;
mod row;
pub mod spec;
mod store;
mod switch;
mod wrap;

pub use database::OutboxDatabase;
#[doc(hidden)]
pub use database::{Defaults, Statements};
#[doc(hidden)]
pub use dispatch::{AckBy, DiscardBy, FetchBy, HeadersOf, RecoverBy, RetryBy, Slot};
pub use error::{OutboxError, PoolAlreadySet, TrackedPublishError};
pub use events::{Ack, Discard, Fetch, Publish, Recover, Retry, Tracked};
pub use layer::TrackingLayer;
pub use publish::TrackingPublishLayer;
pub use registry::{Checked, Nil, Outbox, Registered, TrackedName};
#[doc(hidden)]
pub use registry::{Lacks, RecordList, RecordNames};
pub use republish::Republishing;
pub use row::OutboxRow;
pub use spec::{OutboxSpec, OutboxTable};
#[doc(hidden)]
pub use store::Store;
pub use wrap::TrackedPublisher;

/// The header a tracked message carries its record's id in: written with the id's `Display`,
/// read back with its `FromStr`.
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
/// use ruststream::runtime::{BlanketLayer, Context, Handler};
/// use ruststream_sqlx::outbox::OUTBOX_ID_HEADER;
///
/// /// A layer of the service's own that logs the record id of every tracked delivery.
/// #[derive(Clone, Copy)]
/// pub struct RecordIds;
///
/// pub struct Logged<H>(H);
///
/// impl BlanketLayer for RecordIds {
///     fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
///     where
///         M: Send + Sync + 'static,
///         C: Send + 'static,
///         S: Send + Sync + 'static,
///         H: Handler<M, C, S> + 'static,
///     {
///         Logged(handler)
///     }
/// }
///
/// impl<M: Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S> for Logged<H> {
///     async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
///         if let Some(id) = ctx.headers().get(OUTBOX_ID_HEADER) {
///             let id = String::from_utf8_lossy(id);
///             tracing::info!(subscription = ctx.name(), %id, "a tracked delivery");
///         }
///         self.0.handle(msg, ctx).await
///     }
/// }
///
/// pub fn app(pool: PgPool) -> impl App {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(RecordIds)
///         .layer(tracking.layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(fulfil);
///         })
/// }
/// # }
/// # fn main() {}
/// ```
pub const OUTBOX_ID_HEADER: &str = "x-ruststream-outbox-id";
