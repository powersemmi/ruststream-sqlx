//! `InboxSubscriber`: the claim loop a subscription's stream runs.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use ruststream::{BatchSubscriber, Subscriber};
use sqlx::{Pool, Transaction};

use super::PayloadRow;
use super::broker::Shared;
use super::database::QueueDatabase;
use super::delivery::{BatchTx, InboxDelivery};
use super::engine::{Claimed, Claiming, Events, Now};
use super::error::SqlxBrokerError;
use super::queue::{Queue, Registration};
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};

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
/// A batch's settlements take effect together, when the last of its deliveries finishes: a
/// settlement whose statement fails rolls the whole batch back, and its rows return.
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
                    () = self.shared.stopping.cancelled() => return None,
                }
            }
            if self.shared.is_closed() {
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
        #[cfg(feature = "testing")]
        if self.shared.harness.in_process() {
            return self.claim_in_process(limit).await;
        }
        let tx = claim_rows(
            &self.shared.pool,
            self.queue,
            limit,
            Now::default(),
            &mut self.rows,
        )
        .await
        .map_err(|(statement, source)| self.failed(statement, source))?;
        Ok((tx, self.rows.len()))
    }

    /// The claim of an in-process connection: on the test's clock, off a paused one, and in the
    /// harness's books.
    #[cfg(feature = "testing")]
    async fn claim_in_process(
        &mut self,
        limit: usize,
    ) -> Result<(Transaction<'static, DB>, usize), SqlxBrokerError> {
        let pool = self.shared.pool.clone();
        let queue = self.queue;
        let now = self.shared.harness.now();
        let mut rows = std::mem::take(&mut self.rows);
        let (claimed, rows) = off_clock(async move {
            let claimed = claim_rows(&pool, queue, limit, now, &mut rows).await;
            (claimed, rows)
        })
        .await
        .unwrap_or_else(|| (Err(("BEGIN", cancelled())), Vec::new()));
        self.rows = rows;
        let tx = claimed.map_err(|(statement, source)| self.failed(statement, source))?;
        self.shared.harness.claimed(queue.name, self.rows.len());
        Ok((tx, self.rows.len()))
    }

    /// The deliveries of the last claim, oldest first.
    pub(crate) fn take_rows(&mut self) -> impl Iterator<Item = Claimed<Row>> + '_ {
        self.rows.drain(..)
    }

    pub(crate) const fn queue(&self) -> &'static Queue {
        self.queue
    }

    async fn next_one(&mut self) -> Option<Result<InboxDelivery<DB, Row>, SqlxBrokerError>> {
        let (tx, _) = match self.claim(1).await? {
            Ok(claimed) => claimed,
            Err(error) => return Some(Err(error)),
        };
        let queue = self.queue;
        let claimed = self.rows.pop()?;
        let delivery = InboxDelivery::own(claimed, tx, queue);
        #[cfg(feature = "testing")]
        let delivery = delivery.on(&self.shared);
        Some(Ok(delivery))
    }

    /// The subscription's deliveries, as a stream that owns it.
    pub(crate) fn into_stream(
        self,
    ) -> impl Stream<Item = Result<InboxDelivery<DB, Row>, SqlxBrokerError>> + Send + 'static {
        futures::stream::unfold(self, |mut subscriber| async move {
            let next = subscriber.next_one().await?;
            Some((next, subscriber))
        })
    }
}

/// A statement that failed, and why.
type Failed = (&'static str, sqlx::Error);

/// Claims up to `limit` rows of `queue` into `rows`, in a transaction of `pool` it returns open.
async fn claim_rows<DB, Row>(
    pool: &Pool<DB>,
    queue: &'static Queue,
    limit: usize,
    now: Now,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<Transaction<'static, DB>, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let mut tx = pool.begin().await.map_err(|source| ("BEGIN", source))?;
    let cx = Claiming {
        queue: queue.name,
        limit: i64::try_from(limit).unwrap_or(i64::MAX),
        prepared: &queue.prepared,
        now,
    };
    rows.clear();
    Row::claim(&mut tx, &cx, rows).await.map_err(|source| {
        let statement = queue.prepared.claim.map_or("claim", |claim| claim.sql);
        (statement, source)
    })?;
    Ok(tx)
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

impl<DB, Row> BatchSubscriber for InboxSubscriber<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    type Batch = Vec<InboxDelivery<DB, Row>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, Self::Error>> + Send + '_ {
        futures::stream::unfold(self, move |subscriber| async move {
            let (tx, count) = match subscriber.claim(size.get()).await? {
                Ok(claimed) => claimed,
                Err(error) => return Some((Err(error), subscriber)),
            };
            let queue = subscriber.queue();
            let batch = BatchTx::new(tx, count);
            #[cfg(feature = "testing")]
            let shared = Arc::clone(&subscriber.shared);
            let deliveries = subscriber
                .take_rows()
                .map(|claimed| {
                    let delivery = InboxDelivery::batched(claimed, Arc::clone(&batch), queue);
                    #[cfg(feature = "testing")]
                    let delivery = delivery.on(&shared);
                    delivery
                })
                .collect();
            Some((Ok(deliveries), subscriber))
        })
    }
}
