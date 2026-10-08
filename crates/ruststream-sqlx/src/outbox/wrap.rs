//! A publisher with the publish middleware's tracking, for publishes outside the handlers.

use std::fmt;
use std::sync::Arc;

use ruststream::{OutgoingFor, OutgoingMessage, Publisher};
use sqlx::Database;

use super::OUTBOX_ID_HEADER;
use super::error::TrackedPublishError;
use super::registry::RecordList;
use super::switch::enabled;
use crate::outbox::store::Store;

/// A publisher that records what it publishes under a registered name, from
/// [`wrap`](super::Outbox::wrap).
///
/// It publishes as the publisher it wraps does, with the same payload form and options. A message
/// under a registered name is recorded first and carries [`OUTBOX_ID_HEADER`]; one under any
/// other name goes out as it is.
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
///
///     let publisher = tracking.wrap(running.publisher(egress).await?);
///     publisher.message(&OrderPlaced { id: 7 }).publish().await?;
///
///     running.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct TrackedPublisher<Live, DB: Database, Records> {
    live: Live,
    store: Arc<Store<DB>>,
    records: Records,
}

impl<Live, DB: Database, Records> TrackedPublisher<Live, DB, Records> {
    pub(super) const fn new(live: Live, store: Arc<Store<DB>>, records: Records) -> Self {
        Self {
            live,
            store,
            records,
        }
    }
}

impl<Live: Clone, DB: Database, Records: Copy> Clone for TrackedPublisher<Live, DB, Records> {
    fn clone(&self) -> Self {
        Self {
            live: self.live.clone(),
            store: Arc::clone(&self.store),
            records: self.records,
        }
    }
}

impl<Live: fmt::Debug, DB: Database, Records: fmt::Debug> fmt::Debug
    for TrackedPublisher<Live, DB, Records>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackedPublisher")
            .field("live", &self.live)
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}

impl<Live, DB, Records> Publisher for TrackedPublisher<Live, DB, Records>
where
    Live: Publisher,
    DB: Database,
    Records: RecordList<DB>,
{
    type Payload = Live::Payload;
    type Error = TrackedPublishError<Live::Error>;
    type Options = Live::Options;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let msg = if enabled() && self.records.contains(msg.name()) {
            let (name, payload, headers) = msg.into_parts();
            // The record reads a view that borrows the payload in whatever form the publisher
            // takes it, so the payload is neither copied nor converted.
            let view = OutgoingMessage::with_payload(name, payload.as_ref()).with_headers(headers);
            let recorded = self.records.record(&self.store, &view).await;
            let (_, _, mut headers) = view.into_parts();
            if let Some(id) = recorded {
                headers.insert(OUTBOX_ID_HEADER, id.map_err(TrackedPublishError::Outbox)?);
            }
            OutgoingMessage::with_payload(name, payload).with_headers(headers)
        } else {
            msg
        };
        self.live
            .publish(msg, options)
            .await
            .map_err(TrackedPublishError::Publish)
    }
}
