//! The broker's place in the test harness: the in-process transition, the view the harness
//! drives, and what an in-process connection does differently.
//!
//! The inbox has no model of a database to run in process: its in-process mode reaches the
//! database the test's pool names, as `connect` does. Every database call of an in-process
//! connection runs through [`off_clock`], so a test on a paused tokio clock does not see its
//! timers fire while a reply is on its way, and "now" follows the tokio clock.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::panic::resume_unwind;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, SystemTime};

use ruststream::testing::{Backlog, Coordinator, InProcess, TestableBroker};
use ruststream::{HeaderMap, OutgoingMessage, RawMessage};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::time::Instant;

use super::broker::{ConnectedSqlxBroker, Shared, SqlxBroker};
use super::database::QueueDatabase;
use super::engine::Now;
use super::error::SqlxBrokerError;
use super::publish::insert_routed;

/// Where an in-process connection reads "now": the tokio clock, anchored to the wall clock when
/// the broker connected, so a test that moves a paused clock moves the queue's time with it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TestClock {
    system: SystemTime,
    instant: Instant,
}

impl TestClock {
    fn start() -> Self {
        Self {
            system: SystemTime::now(),
            instant: Instant::now(),
        }
    }

    pub(crate) fn now(self) -> SystemTime {
        self.system + self.instant.elapsed()
    }
}

/// Runs `work` where a paused tokio clock stands still while it waits on the database.
///
/// Tokio does not auto-advance a paused clock while a blocking task runs, and the database's reply
/// arrives in real time. `None` when the runtime shut down before the work finished.
pub(crate) async fn off_clock<T, Work>(work: Work) -> Option<T>
where
    T: Send + 'static,
    Work: Future<Output = T> + Send + 'static,
{
    let runtime = Handle::current();
    match spawn_blocking(move || runtime.block_on(work)).await {
        Ok(output) => Some(output),
        Err(join) if join.is_panic() => resume_unwind(join.into_panic()),
        Err(_) => None,
    }
}

/// The error of database work the runtime dropped before it finished.
pub(crate) fn cancelled() -> sqlx::Error {
    sqlx::Error::Io(io::Error::other(
        "the runtime shut down during a database call",
    ))
}

/// The harness's books of one connection: the coordinator a test installed, and every message the
/// broker's own publishers and the test's injections wrote.
#[derive(Debug, Default)]
pub(crate) struct Harness {
    coordinator: OnceLock<Coordinator>,
    log: Mutex<Vec<RawMessage>>,
    /// The clock of an in-process connection; unset on a connection to the server.
    clock: OnceLock<TestClock>,
    /// The connection's open subscriptions by name, with the messages counted ahead of a claim.
    credits: Mutex<HashMap<String, usize>>,
    /// The last injection's write, which the next one waits for.
    writes: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Harness {
    /// Whether the connection runs in process.
    pub(crate) fn in_process(&self) -> bool {
        self.clock.get().is_some()
    }

    /// Where "now" comes from for the connection's statements.
    pub(crate) fn now(&self) -> Now {
        Now::test(self.clock.get().copied())
    }

    /// The coordinator, on an in-process connection: only that one keeps the books.
    fn books(&self) -> Option<&Coordinator> {
        self.clock.get().and_then(|_| self.coordinator.get())
    }

    fn credits(&self) -> MutexGuard<'_, HashMap<String, usize>> {
        self.credits.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A subscription to `name` opened on the connection.
    pub(crate) fn opened(&self, name: &str) {
        self.credits().entry(name.to_owned()).or_insert(0);
    }

    /// The subscription to `name` closed.
    pub(crate) fn closed(&self, name: &str) {
        self.credits().remove(name);
    }

    /// A message reached the queue `name`: counted now when a subscription of the connection
    /// reads it, so the harness waits for its delivery.
    pub(crate) fn expect(&self, name: &str) {
        let Some(coordinator) = self.books() else {
            return;
        };
        if let Some(credit) = self.credits().get_mut(name) {
            *credit += 1;
            coordinator.enqueued();
        }
    }

    /// A message counted for `name` never reached the table.
    pub(crate) fn refused(&self, name: &str) {
        let Some(coordinator) = self.books() else {
            return;
        };
        if let Some(credit) = self.credits().get_mut(name)
            && *credit > 0
        {
            *credit -= 1;
            coordinator.consumed();
        }
    }

    /// A claim of `name` took `count` rows: each was counted ahead, or is counted now.
    pub(crate) fn claimed(&self, name: &str, count: usize) {
        let Some(coordinator) = self.books() else {
            return;
        };
        let covered = self.credits().get_mut(name).map_or(0, |credit| {
            let covered = (*credit).min(count);
            *credit -= covered;
            covered
        });
        for _ in covered..count {
            coordinator.enqueued();
        }
    }

    /// A delivery settled.
    pub(crate) fn released(&self) {
        if let Some(coordinator) = self.books() {
            coordinator.consumed();
        }
    }

    /// A delivery of `name` dropped unsettled: its row returns, and the delivery leaves the books.
    pub(crate) fn returned(&self, name: &str) {
        self.expect(name);
        self.released();
    }

    /// A message one of the broker's own publishers wrote; it was counted before its insert, so
    /// a claim that lands right after the insert finds it counted.
    pub(crate) fn published(&self, message: &OutgoingMessage<'_>) {
        self.record(message);
    }

    fn record(&self, message: &OutgoingMessage<'_>) {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(
                RawMessage::new(message.name().to_owned(), message.payload().to_vec())
                    .with_headers(message.headers().clone()),
            );
    }
}

/// The row of an in-process delivery of `name` comes back after `delay`: it is counted when the
/// delay runs out, as `TestApp::advance` fires the timer.
pub(crate) fn returns_after<DB: QueueDatabase>(
    connection: &Arc<Shared<DB>>,
    name: &'static str,
    delay: Duration,
) {
    let Some(coordinator) = connection.harness.books() else {
        return;
    };
    let connection = Arc::clone(connection);
    coordinator.schedule_redelivery(delay, move || connection.harness.expect(name));
}

impl<DB: QueueDatabase> InProcess for SqlxBroker<DB> {
    /// Connects to the database the test's pool reaches, as [`connect`](ruststream::Broker::connect)
    /// does, with every database call of the connection kept off a paused clock.
    async fn connect_in_process(self) -> Result<ConnectedSqlxBroker<DB>, SqlxBrokerError> {
        let (pool, choice) = (self.pool.clone(), self.dialect.clone());
        let dialect = off_clock(async move {
            let conn = pool
                .acquire()
                .await
                .map_err(|source| SqlxBrokerError::Connect { source })?;
            choice.resolve(&conn)
        })
        .await
        .unwrap_or_else(|| {
            Err(SqlxBrokerError::Connect {
                source: cancelled(),
            })
        })?;
        let connected = ConnectedSqlxBroker::new(self, dialect, Handle::current());
        let _ = connected.shared.harness.clock.set(TestClock::start());
        Ok(connected)
    }
}

impl<DB: QueueDatabase> TestableBroker for ConnectedSqlxBroker<DB> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.shared.harness.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let harness = &self.shared.harness;
        harness.record(&message);
        harness.expect(message.name());
        let (name, payload, headers) = message.into_parts();
        let (name, payload) = (name.to_owned(), payload.to_vec());
        // Each injection waits for the one before it, so they reach the table in the order the
        // test made them.
        let (written, next) = oneshot::channel();
        let previous = harness
            .writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(next);
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            inject(&shared, &name, &payload, headers).await;
            let _ = written.send(());
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

/// Writes an injected message into the table its route leads to, as an external producer would.
async fn inject<DB: QueueDatabase>(
    shared: &Shared<DB>,
    name: &str,
    payload: &[u8],
    headers: HeaderMap,
) {
    let message = OutgoingMessage::new(name, payload).with_headers(headers);
    let written = match shared.routes.find(name) {
        Some(route) => insert_routed(shared, route, &message).await,
        None => Err(SqlxBrokerError::NoRoute {
            name: name.to_owned(),
        }),
    };
    if let Err(error) = written {
        shared.harness.refused(name);
        tracing::warn!(target: "ruststream_sqlx", %error, name, "an injected message was refused");
    }
}

#[cfg(feature = "postgres")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Postgres>);
#[cfg(feature = "mysql")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::MySql>);
#[cfg(feature = "sqlite")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Sqlite>);
#[cfg(feature = "any")]
ruststream::register_testable_broker!(SqlxBroker<sqlx::Any>);
