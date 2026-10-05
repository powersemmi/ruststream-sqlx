//! The broker's place in the test harness: the in-process transition and the view the harness
//! drives.

use std::sync::{Mutex, OnceLock, PoisonError};

use ruststream::testing::{Backlog, Coordinator, InProcess, TestableBroker};
use ruststream::{Broker, OutgoingMessage, RawMessage};

use super::broker::{ConnectedSqlxBroker, SqlxBroker};
use super::database::QueueDatabase;
use super::error::SqlxBrokerError;

/// The harness's books of one connection: the coordinator a test installed, and every message the
/// broker's own publishers and the test's injections wrote.
#[derive(Debug, Default)]
pub(crate) struct Harness {
    pub(crate) coordinator: OnceLock<Coordinator>,
    pub(crate) log: Mutex<Vec<RawMessage>>,
}

impl Harness {
    pub(crate) fn record(&self, message: &OutgoingMessage<'_>) {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(
                RawMessage::new(message.name().to_owned(), message.payload().to_vec())
                    .with_headers(message.headers().clone()),
            );
    }
}

impl<DB: QueueDatabase> InProcess for SqlxBroker<DB> {
    /// Connects to the database the test's pool reaches, as [`connect`](Broker::connect) does:
    /// the inbox has no model of a database to run instead.
    async fn connect_in_process(self) -> Result<ConnectedSqlxBroker<DB>, SqlxBrokerError> {
        self.connect().await
    }
}

impl<DB: QueueDatabase> TestableBroker for ConnectedSqlxBroker<DB> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.shared.harness.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.shared.harness.record(&message);
        let shared = std::sync::Arc::clone(&self.shared);
        let (name, payload, headers) = message.into_parts();
        let owned = (name.to_owned(), payload.to_vec(), headers);
        tokio::spawn(async move {
            let (name, payload, headers) = owned;
            let message = OutgoingMessage::new(&name, &payload).with_headers(headers);
            if let Some(route) = shared.routes.find(&name)
                && let Err(error) = route.publish(&shared, &message).await
            {
                tracing::warn!(target: "ruststream_sqlx", %error, "an injected message was refused");
            }
        });
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.shared
            .harness
            .log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|message| message.name() == name)
            .cloned()
            .collect()
    }

    fn backlog(&self) -> Backlog {
        // A queue table keeps what is written before a subscription opens.
        Backlog::Delivered
    }
}

#[cfg(feature = "postgres")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Postgres>);
