//! `InboxSubscriber`: the claim loop a subscription's stream runs.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use ruststream::Subscriber;
use sqlx::Transaction;

use super::PayloadRow;
use super::broker::Shared;
use super::database::QueueDatabase;
use super::delivery::InboxDelivery;
use super::engine::{Claimed, Claiming, Events, Now};
use super::error::SqlxBrokerError;
use super::queue::{Queue, Registration};

/// How long a subscription waits after a claim failed, so a persistent failure cannot spin the
/// loop.
const CLAIM_RETRY: Duration = Duration::from_secs(1);

/// The subscriber an [`InboxQueue`](crate::InboxQueue) opens: a stream of deliveries claimed from
/// the table.
///
/// The stream claims when it is polled: up to one row for a single-message handler (one
/// connection per message in work), up to the batch size for a batch handler. After a claim that
/// filled its limit the next one runs at once; after one that found fewer rows it waits the poll
/// interval. A failed claim reaches the stream as an error item and the next claim waits one
/// second. After `shutdown` the stream ends.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use ruststream_sqlx::Inbox;
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
/// use futures::StreamExt;
/// use ruststream::{Broker, IncomingMessage, Subscriber, SubscriptionSource};
/// use ruststream_sqlx::{InboxQueue, SqlxBroker};
///
/// // What the runtime does for a mounted handler, written out.
/// pub async fn drain(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
///     let connected = SqlxBroker::new(pool).connect().await?;
///     let mut subscriber = InboxQueue::<Job>::new("jobs").subscribe(&connected).await?;
///     let mut deliveries = std::pin::pin!(subscriber.stream());
///     while let Some(delivery) = deliveries.next().await {
///         delivery?.ack().await?;
///     }
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxSubscriber<DB: QueueDatabase, Row: Events<DB>> {
    shared: Arc<Shared<DB>>,
    queue: &'static Queue,
    /// The claim's rows, reused from claim to claim.
    rows: Vec<Claimed<Row>>,
    /// What the next claim waits for first.
    wait: Option<Duration>,
    _registration: Registration<DB>,
}

impl<DB: QueueDatabase, Row: Events<DB>> fmt::Debug for InboxSubscriber<DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxSubscriber")
            .field("subscription", &self.queue.name)
            .field("table", &self.queue.table)
            .field("row", &self.queue.row)
            .finish_non_exhaustive()
    }
}

impl<DB: QueueDatabase, Row: Events<DB> + PayloadRow> InboxSubscriber<DB, Row> {
    pub(crate) const fn new(
        shared: Arc<Shared<DB>>,
        queue: &'static Queue,
        registration: Registration<DB>,
    ) -> Self {
        Self {
            shared,
            queue,
            rows: Vec::new(),
            wait: None,
            _registration: registration,
        }
    }

    fn failed(&self, statement: &'static str, source: sqlx::Error) -> SqlxBrokerError {
        SqlxBrokerError::Sqlx {
            subscription: self.queue.name.to_owned(),
            table: self.queue.table.to_owned(),
            row: self.queue.row,
            statement,
            source: Box::new(source),
        }
    }

    /// Waits for what the last claim asked, then claims up to `limit` rows in one transaction.
    /// `None` once the broker is shut down.
    pub(crate) async fn claim(
        &mut self,
        limit: usize,
    ) -> Option<Result<(Transaction<'static, DB>, usize), SqlxBrokerError>> {
        loop {
            if let Some(wait) = self.wait.take() {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = self.shared.closed.cancelled() => return None,
                }
            }
            if self.shared.closed.is_cancelled() {
                return None;
            }
            let claimed = self.claim_once(limit).await;
            match claimed {
                Ok((tx, 0)) => {
                    // Dropping the empty claim's transaction queues its rollback.
                    drop(tx);
                    self.wait = Some(self.queue.poll_interval);
                }
                Ok((tx, count)) => {
                    self.wait = (count < limit).then_some(self.queue.poll_interval);
                    return Some(Ok((tx, count)));
                }
                Err(error) => {
                    self.wait = Some(CLAIM_RETRY);
                    return Some(Err(error));
                }
            }
        }
    }

    async fn claim_once(
        &mut self,
        limit: usize,
    ) -> Result<(Transaction<'static, DB>, usize), SqlxBrokerError> {
        let mut tx = self
            .shared
            .pool
            .begin()
            .await
            .map_err(|source| self.failed("BEGIN", source))?;
        let statement = self.queue.prepared.claim.map_or("claim", |claim| claim.sql);
        let cx = Claiming {
            queue: self.queue.name,
            limit: i64::try_from(limit).unwrap_or(i64::MAX),
            prepared: &self.queue.prepared,
            now: Now::default(),
        };
        self.rows.clear();
        let claimed = Row::claim(&mut tx, &cx, &mut self.rows).await;
        claimed.map_err(|source| self.failed(statement, source))?;
        Ok((tx, self.rows.len()))
    }

    async fn next_one(&mut self) -> Option<Result<InboxDelivery<DB, Row>, SqlxBrokerError>> {
        let (tx, _) = match self.claim(1).await? {
            Ok(claimed) => claimed,
            Err(error) => return Some(Err(error)),
        };
        let queue = self.queue;
        let claimed = self.rows.pop()?;
        Some(Ok(InboxDelivery::own(claimed, tx, queue)))
    }
}

impl<DB, Row> Subscriber for InboxSubscriber<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    type Message = InboxDelivery<DB, Row>;
    type Error = SqlxBrokerError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, |subscriber| async move {
            let next = subscriber.next_one().await?;
            Some((next, subscriber))
        })
    }
}
