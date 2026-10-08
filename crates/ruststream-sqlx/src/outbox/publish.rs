//! The publish middleware: a message published under a registered name is recorded before it is
//! sent, and carries its record's id.

use std::any::type_name;
use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::io::Write;
use std::mem;
use std::str::{self, FromStr};
use std::sync::Arc;

use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use ruststream::{Bytes, HeaderMap, OutgoingMessage, Publisher};
use sqlx::Database;

use super::OUTBOX_ID_HEADER;
use super::error::OutboxError;
use super::events::Publish;
use super::registry::RecordList;
use super::switch::enabled;
use crate::outbox::store::Store;

/// The publish middleware of an [`Outbox`](super::Outbox), from
/// [`publish_layer`](super::Outbox::publish_layer).
///
/// A message under a registered name is recorded through its record type's `Publish`, then sent
/// with [`OUTBOX_ID_HEADER`]; a record that fails fails the publish, and the message is not sent.
/// A message under any other name is sent as it is.
pub struct TrackingPublishLayer<DB: Database, Records> {
    store: Arc<Store<DB>>,
    records: Records,
}

impl<DB: Database, Records> TrackingPublishLayer<DB, Records> {
    pub(super) const fn new(store: Arc<Store<DB>>, records: Records) -> Self {
        Self { store, records }
    }
}

impl<DB: Database, Records: Copy> Clone for TrackingPublishLayer<DB, Records> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            records: self.records,
        }
    }
}

impl<DB: Database, Records: fmt::Debug> fmt::Debug for TrackingPublishLayer<DB, Records> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackingPublishLayer")
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}

impl<DB: Database, Records: RecordList<DB>> PublishLayer for TrackingPublishLayer<DB, Records> {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        if enabled() && self.records.contains(out.name()) {
            // The map moves out and back: the record reads it through an `OutgoingMessage`
            // that borrows the name and the payload, so nothing is copied.
            let headers = mem::take(out.headers_mut());
            let msg =
                OutgoingMessage::with_payload(out.name(), out.payload()).with_headers(headers);
            let recorded = self.records.record(&self.store, &msg).await;
            let (_, _, headers) = msg.into_parts();
            *out.headers_mut() = headers;
            if let Some(id) = recorded {
                out.headers_mut().insert(OUTBOX_ID_HEADER, id?);
            }
        }
        next.run(out).await
    }
}

/// Records `msg` with `Record`, and returns the id header's value.
pub(super) async fn record<DB: Database, Record: Publish<DB>>(
    name: &'static str,
    store: &Store<DB>,
    msg: &OutgoingMessage<'_>,
) -> Result<Bytes, OutboxError> {
    let pool = store.get().ok_or(OutboxError::NoPool { name })?;
    let failed = |source| OutboxError::Record {
        name,
        record: type_name::<Record>(),
        source,
    };
    let mut conn = store.acquire(pool).await.map_err(failed)?;
    let id = Record::publish(&mut conn, msg).await.map_err(failed)?;
    Ok(id_value(&id))
}

/// The longest id text [`id_value`] formats on the stack: a UUID's 36 characters fit.
const ID_ON_STACK: usize = 64;

/// The id header's value: the id's `Display` text in a buffer of its own length.
///
/// One allocation. `to_string` would take two: its `String` reserves more than an integer's
/// digits, and a `Bytes` made from a vector with spare capacity allocates a block to share it.
pub(super) fn id_value(id: &impl Display) -> Bytes {
    let mut text = [0_u8; ID_ON_STACK];
    let mut rest = &mut text[..];
    if write!(rest, "{id}").is_ok() {
        let len = ID_ON_STACK - rest.len();
        return Bytes::copy_from_slice(&text[..len]);
    }
    Bytes::copy_from_slice(id.to_string().as_bytes())
}

/// The id a delivery carries in [`OUTBOX_ID_HEADER`]: `None` without the header, the header's
/// text when it does not parse as an `Id`.
pub(super) fn carried_id<Id: FromStr>(headers: &HeaderMap) -> Option<Result<Id, String>> {
    let value = headers.get(OUTBOX_ID_HEADER)?;
    Some(
        str::from_utf8(value)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| String::from_utf8_lossy(value).into_owned()),
    )
}

#[cfg(test)]
mod tests {
    use ruststream::HeaderMap;

    use super::{ID_ON_STACK, OUTBOX_ID_HEADER, carried_id, id_value};

    #[test]
    fn an_id_survives_the_header() {
        let mut headers = HeaderMap::new();
        headers.insert(OUTBOX_ID_HEADER, 42_i64.to_string());
        assert_eq!(carried_id::<i64>(&headers), Some(Ok(42)));
        headers.insert(OUTBOX_ID_HEADER, "7f3a".to_owned());
        assert_eq!(carried_id::<String>(&headers), Some(Ok("7f3a".to_owned())));
    }

    #[test]
    fn an_id_value_holds_the_ids_text_whatever_its_length() {
        assert_eq!(&id_value(&42_i64)[..], b"42");
        assert_eq!(&id_value(&i64::MIN)[..], i64::MIN.to_string().as_bytes());
        let long = "7".repeat(ID_ON_STACK + 1);
        assert_eq!(&id_value(&long)[..], long.as_bytes());
    }

    #[test]
    fn a_missing_or_foreign_header_is_told_apart() {
        let mut headers = HeaderMap::new();
        assert_eq!(carried_id::<i64>(&headers), None);
        headers.insert(OUTBOX_ID_HEADER, "seven");
        assert_eq!(carried_id::<i64>(&headers), Some(Err("seven".to_owned())));
        headers.insert(OUTBOX_ID_HEADER, &b"\xff7"[..]);
        assert_eq!(
            carried_id::<i64>(&headers),
            Some(Err("\u{fffd}7".to_owned()))
        );
    }
}
