//! `InboxDelivery`: one claimed row in a handler's hands, and how it settles.

use std::any::type_name;
use std::fmt;
use std::time::Duration;

use ruststream::{AckError, HeaderMap, IncomingMessage};
use sqlx::Transaction;
use sync_wrapper::SyncWrapper;

use super::PayloadRow;
use super::database::QueueDatabase;
use super::engine::{Claimed, Events, Now, Released, Settling};
use super::error::SqlxBrokerError;
use super::queue::Queue;

/// What a handler's outcome asks of the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ack,
    Discard,
    Retry,
    RetryAfter(Duration),
}

/// The statement an outcome runs, once the cap is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Ack,
    Discard,
    Retry,
    RetryAfter(Duration),
    DeadLetter(&'static str),
}

impl Step {
    const fn event(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::Discard => "discard",
            Self::Retry => "retry",
            Self::RetryAfter(_) => "retry_after",
            Self::DeadLetter(_) => "dead_letter",
        }
    }
}

/// Where a delivery's transaction lives.
enum Hold<DB: QueueDatabase> {
    /// The delivery's own: one claim, one row.
    Own(SyncWrapper<Transaction<'static, DB>>),
}

/// One claimed row in a handler's hands.
///
/// The payload is lent from the row, without a copy. The delivery holds the claim's transaction;
/// settling it runs one statement and commits, and dropping it unsettled rolls the transaction
/// back, which returns the row to the queue at once.
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
/// use ruststream::IncomingMessage;
/// use ruststream_sqlx::InboxDelivery;
///
/// // A delivery whose payload is not JSON goes back to the queue for a later attempt.
/// pub async fn settle(delivery: InboxDelivery<sqlx::Postgres, Job>) -> Result<(), ruststream::AckError> {
///     if serde_json::from_slice::<serde_json::Value>(delivery.payload()).is_ok() {
///         delivery.ack().await
///     } else {
///         delivery.nack(true).await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxDelivery<DB: QueueDatabase, Row: Events<DB>> {
    claimed: Claimed<Row>,
    headers: HeaderMap,
    hold: Option<Hold<DB>>,
    queue: &'static Queue,
}

impl<DB: QueueDatabase, Row: Events<DB>> fmt::Debug for InboxDelivery<DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxDelivery")
            .field("subscription", &self.queue.name)
            .field("id", self.claimed.id::<DB>())
            .finish_non_exhaustive()
    }
}

impl<DB: QueueDatabase, Row: Events<DB> + PayloadRow> InboxDelivery<DB, Row> {
    /// A delivery that owns its claim's transaction.
    pub(crate) fn own(
        claimed: Claimed<Row>,
        tx: Transaction<'static, DB>,
        queue: &'static Queue,
    ) -> Self {
        Self::held(claimed, Hold::Own(SyncWrapper::new(tx)), queue)
    }

    fn held(claimed: Claimed<Row>, hold: Hold<DB>, queue: &'static Queue) -> Self {
        let headers = match &claimed {
            Claimed::Row(row) => Row::headers(row),
            Claimed::Missing(id) => {
                tracing::warn!(
                    target: "ruststream_sqlx",
                    subscription = queue.name,
                    table = queue.table,
                    row = queue.row,
                    ?id,
                    "the fetch returned no row for a claimed id; its delivery carries no payload \
                     and fails to decode",
                );
                HeaderMap::new()
            }
        };
        Self {
            claimed,
            headers,
            hold: Some(hold),
            queue,
        }
    }

    /// What `outcome` runs: its own statement, or the declared move once the attempts are spent.
    fn step(&self, outcome: Outcome) -> Step {
        let asked = match outcome {
            Outcome::Ack => return Step::Ack,
            Outcome::Discard => return Step::Discard,
            Outcome::Retry => Step::Retry,
            Outcome::RetryAfter(delay) => Step::RetryAfter(delay),
        };
        let attempt = self.redelivery_count();
        let spent = self
            .queue
            .max_attempts
            .is_some_and(|cap| attempt.is_some_and(|attempt| attempt >= u64::from(cap.get())));
        let instead = match (self.queue.dead_letter, self.queue.max_attempts) {
            (Some(destination), None) => Some(Step::DeadLetter(destination)),
            (Some(destination), Some(_)) if spent => Some(Step::DeadLetter(destination)),
            (None, Some(_)) if spent => Some(Step::Discard),
            _ => None,
        };
        if spent {
            tracing::warn!(
                target: "ruststream_sqlx",
                subscription = self.queue.name,
                table = self.queue.table,
                row = self.queue.row,
                id = ?self.claimed.id::<DB>(),
                attempt,
                dead_letter = self.queue.dead_letter,
                "the row's attempts are spent",
            );
        }
        instead.unwrap_or(asked)
    }

    async fn settle(mut self, outcome: Outcome) -> Result<(), AckError> {
        let step = self.step(outcome);
        // A delivery holds its transaction until it settles, and settling consumes it.
        let Some(Hold::Own(tx)) = self.hold.take() else {
            return Ok(());
        };
        let mut tx = tx.into_inner();
        let queue = self.queue;
        let failed = |source: sqlx::Error| {
            AckError::Broker(Box::new(SqlxBrokerError::Sqlx {
                subscription: queue.name.to_owned(),
                table: queue.table.to_owned(),
                row: queue.row,
                statement: step.event(),
                source: Box::new(source),
            }))
        };
        let cx = Settling {
            queue: queue.name,
            prepared: &queue.prepared,
            now: Now::default(),
        };
        let id = self.claimed.id::<DB>();
        let released = match step {
            Step::Ack => Row::ack(&mut tx, &cx, id).await.map(|()| Released::Written),
            Step::Discard => Row::discard(&mut tx, &cx, id)
                .await
                .map(|()| Released::Written),
            Step::Retry => Row::retry(&mut tx, &cx, id).await,
            Step::RetryAfter(delay) => Row::retry_after(&mut tx, &cx, id, delay)
                .await
                .map(|()| Released::Written),
            Step::DeadLetter(destination) => Row::dead_letter(&mut tx, &cx, id, destination)
                .await
                .map(|()| Released::Written),
        }
        .map_err(failed)?;
        match released {
            Released::Written => tx.commit().await.map_err(failed),
            Released::Untouched => tx.rollback().await.map_err(failed),
        }
    }
}

impl<DB, Row> IncomingMessage for InboxDelivery<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    fn payload(&self) -> &[u8] {
        match &self.claimed {
            Claimed::Row(row) => row.payload(),
            Claimed::Missing(_) => &[],
        }
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn partition_key(&self) -> Option<&[u8]> {
        match &self.claimed {
            Claimed::Row(row) => Row::partition_key(row),
            Claimed::Missing(_) => None,
        }
    }

    fn redelivery_count(&self) -> Option<u64> {
        match &self.claimed {
            Claimed::Row(row) => Row::attempt(row),
            Claimed::Missing(_) => None,
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle(Outcome::Ack).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.settle(if requeue {
            Outcome::Retry
        } else {
            Outcome::Discard
        })
        .await
    }

    fn supports_nack_after(&self) -> bool {
        Row::SHAPE.native_retry_after()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        if !Row::SHAPE.native_retry_after() {
            return Err(AckError::Unsupported);
        }
        self.settle(Outcome::RetryAfter(delay)).await
    }
}

impl<DB: QueueDatabase, Row: Events<DB>> Drop for InboxDelivery<DB, Row> {
    fn drop(&mut self) {
        // An unsettled delivery's transaction rolls back as it drops, which returns the row to
        // the queue at once.
        let _ = self.hold.take();
        let _ = type_name::<Row>();
    }
}
