//! A publisher with the publish middleware's tracking, for publishes outside the handlers.

use std::fmt;
use std::sync::{Arc, OnceLock};

use ruststream::{OutgoingFor, OutgoingMessage, Publisher};
use sqlx::{Database, Pool};

use super::OUTBOX_ID_HEADER;
use super::error::TrackedPublishError;
use super::registry::RecordList;
use super::switch::enabled;

/// A publisher that records what it publishes under a registered name, from
/// [`wrap`](super::Outbox::wrap).
///
/// It publishes as the publisher it wraps does, with the same payload form and options. A message
/// under a registered name is recorded first and carries [`OUTBOX_ID_HEADER`]; one under any
/// other name goes out as it is.
pub struct TrackedPublisher<Live, DB: Database, Records> {
    live: Live,
    pool: Arc<OnceLock<Pool<DB>>>,
    records: Records,
}

impl<Live, DB: Database, Records> TrackedPublisher<Live, DB, Records> {
    pub(super) const fn new(live: Live, pool: Arc<OnceLock<Pool<DB>>>, records: Records) -> Self {
        Self {
            live,
            pool,
            records,
        }
    }
}

impl<Live: Clone, DB: Database, Records: Copy> Clone for TrackedPublisher<Live, DB, Records> {
    fn clone(&self) -> Self {
        Self {
            live: self.live.clone(),
            pool: Arc::clone(&self.pool),
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
            let recorded = self.records.record(&self.pool, &view).await;
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
