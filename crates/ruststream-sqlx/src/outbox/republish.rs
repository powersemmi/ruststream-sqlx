//! The startup republish: every unprocessed record of a registered name is published again, with
//! its id.

use std::any::type_name;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ruststream::{OutgoingMessage, PayloadForm, Publisher};
use sqlx::Database;

use super::OUTBOX_ID_HEADER;
use super::database::Defaults;
use super::error::OutboxError;
use super::events::Tracked;
use super::publish::id_value;
use super::switch::enabled;
use crate::outbox::store::Store;

/// The body of the startup republish, which [`republish`](super::Outbox::republish) hands to
/// `after_startup`: it resolves once every unprocessed record went out again, and fails startup
/// with the first record that did not.
#[must_use = "a republish does nothing until it is awaited"]
pub struct Republishing(
    // Why boxed: the hook's return type has to be named, and stable Rust cannot name an async
    // block's type in a closure's return. One allocation and one dynamic poll, once per startup.
    Pin<Box<dyn Future<Output = Result<(), OutboxError>> + Send>>,
);

impl Republishing {
    pub(super) fn new(
        body: impl Future<Output = Result<(), OutboxError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(async move {
            if enabled() { body.await } else { Ok(()) }
        }))
    }
}

impl Future for Republishing {
    type Output = Result<(), OutboxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl fmt::Debug for Republishing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Republishing").finish_non_exhaustive()
    }
}

/// Publishes every unprocessed record `Record` of `name` through `publisher`.
pub(super) async fn recover_and_publish<DB, Record, Live>(
    name: &'static str,
    defaults: &Defaults,
    pool: &Store<DB>,
    publisher: &Live,
) -> Result<(), OutboxError>
where
    DB: Database,
    Record: Tracked<DB>,
    Live: Publisher,
{
    let pool = pool.get().ok_or(OutboxError::NoPool { name })?;
    let recovered = async {
        let mut conn = pool.acquire().await?;
        Record::recover_records(&mut conn, name, defaults).await
    }
    .await
    .map_err(|source| OutboxError::Recover {
        name,
        record: type_name::<Record>(),
        source,
    })?;
    for mut record in recovered {
        let mut headers = record.take_headers();
        headers.insert(OUTBOX_ID_HEADER, id_value(record.id()));
        let payload = <Live::Payload as PayloadForm>::Form::from(record.payload());
        let msg = OutgoingMessage::with_payload(record.name(), payload).with_headers(headers);
        if let Err(source) = publisher.publish(msg, None).await {
            return Err(OutboxError::Republish {
                name,
                record: type_name::<Record>(),
                id: record.id().to_string(),
                source: Box::new(source),
            });
        }
    }
    Ok(())
}
