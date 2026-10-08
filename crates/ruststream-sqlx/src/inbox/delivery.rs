//! `InboxDelivery`: one claimed row in a handler's hands, and how it settles.

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use ruststream::codec::CodecError;
use ruststream::{AckError, Carries, HeaderMap, IncomingMessage};
use sync_wrapper::SyncWrapper;
use thiserror::Error;
use tokio::runtime::Handle;

use super::broker::Shared;
use super::claims::InWork;
use super::database::QueueDatabase;
use super::engine::{Claimed, Events, Now};
use super::form::advisory::LockHold;
use super::form::lease::settle::release;
#[cfg(feature = "testing")]
use super::form::lease::settle::release_in_process;
use super::form::lease::{LeaseBook, Slot};
use super::form::row_lock::BatchTx;
use super::headers::HeaderCell;
use super::queue::Queue;
#[cfg(feature = "testing")]
use super::testing::off_clock;
use super::transactional::{InboxMode, Plain, TxHold};
use super::tx::PoolTx;
use super::{Lane, QueueRow, RowLane};
#[cfg(feature = "testing")]
use crate::inbox::pools::ServicePool;

pub(crate) mod settle;

use settle::Outcome;

/// What holds a delivery's row until it settles.
pub(super) enum Hold<DB: QueueDatabase, Row: Events<DB>> {
    /// The delivery's own transaction: one claim, one row.
    Own(SyncWrapper<PoolTx<DB>>),
    /// A batch's transaction: one claim, its rows sharing the transaction.
    Batch(Arc<BatchTx<DB>>),
    /// The lease the claim wrote, in the subscription's book; in transactional mode with the
    /// delivery's own transaction, in the subscription's book of transactions while the handler
    /// does not borrow it.
    Lease {
        book: &'static LeaseBook<DB, Row>,
        slot: Slot,
        tx: Option<TxHold<DB>>,
    },
    /// The lock on the row's key, held by the delivery's own session in the subscription's book.
    Advisory(LockHold<DB>),
    /// The delivery's own transaction, in the subscription's book while the handler does not
    /// borrow it: one claim, one row, in transactional mode.
    Lent(TxHold<DB>),
}

/// One claimed row in a handler's hands.
///
/// In payload mode the payload is lent from the row, without a copy. In row mode the delivery
/// lends its handler the row itself, as the driver read it, with no codec in between. A `headers`
/// column becomes the delivery's headers, and the row it lends holds that column empty. A claimed
/// id without a row, and a row the driver could not read, lend no row: the runtime settles them by
/// the decode policy.
///
/// In the row lock form the delivery holds the claim's
/// transaction: settling it runs one statement and commits, and dropping it unsettled rolls the
/// transaction back, which returns the row to the queue at once. In the lease form the delivery
/// holds the lease its claim wrote, which its subscription extends each half lease: settling it
/// runs one statement on a connection of its own, which takes effect only while the row still
/// holds that lease. A lease delivery dropped unsettled releases its row at once, on the runtime
/// the broker connected on; with that runtime gone, the row returns once the lease runs out.
///
/// In the advisory lock form the delivery holds a connection whose session holds the lock on the
/// row's key: settling it runs one statement on that connection, then releases the lock and
/// returns the connection to the pool. A delivery dropped unsettled closes its connection on the
/// runtime the broker connected on, after releasing the lock, and its row returns at once; with
/// that runtime gone the connection closes as it drops, and the server ends its session and its
/// lock. A settlement after [`shutdown`](ruststream::ConnectedBroker::shutdown) released its lock
/// fails with [`SqlxBrokerError::Closed`](crate::SqlxBrokerError::Closed).
///
/// In transactional mode the delivery lends its handler the transaction it settles in, and the
/// handler's writes go with the settlement: acknowledgement runs its statement and commits them
/// together; every other settlement first discards what the handler wrote, then settles as outside
/// transactional mode. A statement that fails rolls the whole transaction back, and the row
/// returns at once. In the lease form the acknowledgement runs in the delivery's own transaction by
/// the lease it holds: a lease lost meanwhile rolls everything back and the settlement fails with
/// [`SqlxBrokerError::LeaseLost`](crate::SqlxBrokerError::LeaseLost). In the advisory lock form
/// the transaction runs on the session that holds the key, and ends before the key's release. A
/// delivery dropped unsettled closes its transaction's connection, and the server rolls the
/// transaction back.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// # enum Sent { Yes, Later, Never }
/// # async fn deliver(_: &Email) -> Sent { Sent::Yes }
/// // Each claimed row reaches `send` as an `InboxDelivery<Postgres, SendEmail>`, and what the
/// // handler returns settles it.
/// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     match deliver(email).await {
///         Sent::Yes => HandlerOutcome::ack(),
///         // The mail server asked to come back later: the row returns once the minute passed.
///         Sent::Later => HandlerOutcome::retry_after(Duration::from_secs(60)),
///         // The address does not exist: the row is finished without a send.
///         Sent::Never => HandlerOutcome::drop(),
///     }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// In row mode the handler borrows the row the delivery holds, beside what it reads off the
/// delivery:
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::PgPool;
///
/// /// A mail to send: the table has no payload column, so a handler takes the row itself.
/// #[derive(Inbox, sqlx::FromRow, Clone)]
/// #[inbox(table = "mail_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(attempt, generated)]
///     attempt: i16,
///     to: String,
/// }
///
/// # async fn deliver(_: &str) -> bool { true }
/// // The delivery lends `send` its row and reports the attempt the table counted.
/// #[subscriber(InboxQueue::<SendEmail>::new("mail"))]
/// async fn send(email: &SendEmail, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
///     if deliver(&email.to).await {
///         HandlerOutcome::ack()
///     } else if attempt < Some(3) {
///         HandlerOutcome::retry()
///     } else {
///         HandlerOutcome::drop()
///     }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxDelivery<DB: QueueDatabase, Row: Events<DB>, Mode = Plain> {
    claimed: Claimed<Row>,
    headers: Row::Headers,
    pub(super) hold: Option<Hold<DB, Row>>,
    queue: &'static Queue,
    /// The subscription's handle on the pool, which the delivery lends its handler.
    pub(super) pool: &'static ServicePool<DB>,
    /// In row mode, whether the handler borrowed the row; nothing in payload mode.
    row_lent: <Row::Lane as Lane<Row>>::Lent,
    /// The connection of a delivery claimed in process: its settlement keeps the harness's books.
    #[cfg(feature = "testing")]
    in_process: Option<Arc<Shared<DB>>>,
    /// Counts the delivery settled in its subscription's books when it drops; a delivery of a
    /// batch counts nothing.
    in_work: Option<InWork>,
    _mode: PhantomData<fn() -> Mode>,
}

/// What a delivery of a claimed id without a row reports: the row is gone.
static ROW_GONE: LazyLock<CodecError> = LazyLock::new(|| CodecError::Decode(Box::new(RowGone)));

/// The row of a claimed id was gone when the claim read it: the fetch returned no row for the id.
#[derive(Debug, Error)]
#[error("the claimed row is gone: the fetch returned no row for its id")]
struct RowGone;

/// Logs a claimed id of `queue` the fetch returned no row for, naming the id.
fn row_gone(queue: &Queue, id: &impl Debug) {
    tracing::warn!(
        target: "ruststream_sqlx",
        subscription = queue.name,
        table = queue.table,
        row = queue.row,
        ?id,
        "the fetch returned no row for a claimed id; the decode-failure policy settles its \
         delivery",
    );
}

/// What a subscription to a table in row mode logs when a handler reads a payload from a delivery
/// whose row it never borrowed: a handler that decodes a payload, mounted where `row`, the struct
/// that describes the table, is what a handler takes.
fn unlent_payload(row: &str) -> String {
    format!(
        "the table is in row mode: its deliveries lend the row itself and carry no payload, so a \
         handler that decodes one fails each delivery by the decode policy; take `&{row}` as the \
         handler's input, `&[{row}]` in a batch"
    )
}

/// The subscriptions that logged [`unlent_payload`], by their interned queue: each logs it once.
static UNLENT_LOGGED: Mutex<Vec<&'static Queue>> = Mutex::new(Vec::new());

/// Logs [`unlent_payload`], once per subscription.
// Why a runtime warning: the core's codec lane accepts a handler on any subscription, so a handler
// that decodes a payload mounts on a table in row mode; refusing it at compile time needs a hook
// in the core. Out of line and cold, as only such a handler reaches it, and the lock with it.
#[cold]
#[inline(never)]
fn read_unlent(queue: &'static Queue) {
    let mut logged = UNLENT_LOGGED.lock().unwrap_or_else(PoisonError::into_inner);
    if logged.iter().any(|seen| ptr::eq(*seen, queue)) {
        return;
    }
    logged.push(queue);
    drop(logged);
    tracing::warn!(
        target: "ruststream_sqlx",
        subscription = queue.name,
        table = queue.table,
        row = queue.row,
        "{}",
        unlent_payload(queue.row),
    );
}

impl<DB: QueueDatabase, Row: Events<DB>, Mode> Debug for InboxDelivery<DB, Row, Mode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxDelivery")
            .field("subscription", &self.queue.name)
            .field("id", self.claimed.id::<DB>())
            .finish_non_exhaustive()
    }
}

impl<DB, Row, Mode> InboxDelivery<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
{
    /// A delivery that owns its claim's transaction.
    pub(crate) fn own(
        claimed: Claimed<Row>,
        tx: PoolTx<DB>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Own(SyncWrapper::new(tx)), queue, pool)
    }

    /// A delivery whose claim's transaction waits at `hold` in its subscription's book, for its
    /// handler to borrow: transactional mode.
    pub(crate) fn lent(
        claimed: Claimed<Row>,
        hold: TxHold<DB>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Lent(hold), queue, pool)
    }

    /// A delivery of a batch, sharing its claim's transaction.
    pub(crate) fn batched(
        claimed: Claimed<Row>,
        batch: Arc<BatchTx<DB>>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Batch(batch), queue, pool)
    }

    /// A delivery that holds the lease its claim wrote, entered in its subscription's `book`; in
    /// transactional mode with its own transaction, which waits at `tx` for its handler to borrow.
    pub(crate) fn leased(
        claimed: Claimed<Row>,
        book: &'static LeaseBook<DB, Row>,
        lease: Row::Token,
        tx: Option<TxHold<DB>>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        let slot = book.enter(claimed.id::<DB>(), lease);
        Self::held(claimed, Hold::Lease { book, slot, tx }, queue, pool)
    }

    /// A delivery whose session holds the lock on its row's key, at `hold` in its subscription's
    /// book.
    pub(crate) fn advised(
        claimed: Claimed<Row>,
        hold: LockHold<DB>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        Self::held(claimed, Hold::Advisory(hold), queue, pool)
    }

    /// The subscription's handle on the pool, which the delivery lends its handler.
    pub(crate) const fn pool(&self) -> &'static ServicePool<DB> {
        self.pool
    }

    fn held(
        mut claimed: Claimed<Row>,
        hold: Hold<DB, Row>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        // A row's headers column moves into the delivery in both modes: in row mode the row a
        // handler borrows holds that column empty, and middleware reads the headers off the
        // delivery. A message assembled from a headers struct builds its map on the first read.
        let headers = match &mut claimed {
            Claimed::Row(row) => HeaderCell::take(row),
            Claimed::Missing(id) => {
                row_gone(queue, id);
                Row::Headers::default()
            }
            Claimed::Undecodable { id, attempt, error } => {
                tracing::warn!(
                    target: "ruststream_sqlx",
                    subscription = queue.name,
                    table = queue.table,
                    row = queue.row,
                    ?id,
                    attempt,
                    %error,
                    "the row does not decode into its struct; the decode-failure policy settles \
                     its delivery",
                );
                Row::Headers::default()
            }
        };
        Self {
            claimed,
            headers,
            hold: Some(hold),
            queue,
            pool,
            row_lent: Default::default(),
            #[cfg(feature = "testing")]
            in_process: None,
            in_work: None,
            _mode: PhantomData,
        }
    }

    /// The delivery counted in work until it drops.
    pub(crate) fn counted(mut self, in_work: InWork) -> Self {
        self.in_work = Some(in_work);
        self
    }

    /// The delivery of `connection`, which keeps it when the connection runs in process.
    #[cfg(feature = "testing")]
    pub(crate) fn on(mut self, connection: &Arc<Shared<DB>>) -> Self {
        self.in_process = connection
            .harness
            .in_process()
            .then(|| Arc::clone(connection));
        self
    }
}

impl<DB, Row> InboxDelivery<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
{
    /// The delivery of a row a [`RowBatch`](crate::RowBatch) lent its handler, held by `hold`:
    /// `headers` were taken from the row when the batch was built, and the row counts as lent.
    pub(super) fn of_batch(
        row: Row,
        headers: Row::Headers,
        hold: Hold<DB, Row>,
        queue: &'static Queue,
        pool: &'static ServicePool<DB>,
    ) -> Self {
        Self {
            claimed: Claimed::Row(row),
            headers,
            hold: Some(hold),
            queue,
            pool,
            row_lent: AtomicBool::new(true),
            #[cfg(feature = "testing")]
            in_process: None,
            in_work: None,
            _mode: PhantomData,
        }
    }
}

impl<DB, Row, Mode> Carries<Row> for InboxDelivery<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB> + QueueRow<Lane = RowLane>,
    Mode: InboxMode,
{
    fn carried(&self) -> Option<&Row> {
        match &self.claimed {
            Claimed::Row(row) => {
                // Relaxed: one task handles a delivery, and its own `payload` alone reads the flag.
                self.row_lent.store(true, Ordering::Relaxed);
                Some(row)
            }
            Claimed::Missing(_) | Claimed::Undecodable { .. } => None,
        }
    }
}

impl<DB, Row, Mode> IncomingMessage for InboxDelivery<DB, Row, Mode>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
{
    fn payload(&self) -> &[u8] {
        match &self.claimed {
            Claimed::Row(row) => {
                // A constant `false` in payload mode, where the check folds away.
                if <Row::Lane as Lane<Row>>::unlent(&self.row_lent) {
                    read_unlent(self.queue);
                }
                <Row::Lane as Lane<Row>>::payload(row)
            }
            Claimed::Missing(_) | Claimed::Undecodable { .. } => &[],
        }
    }

    fn headers(&self) -> &HeaderMap {
        let row = match &self.claimed {
            Claimed::Row(row) => Some(row),
            Claimed::Missing(_) | Claimed::Undecodable { .. } => None,
        };
        self.headers.read(row)
    }

    fn decode_error(&self) -> Option<&CodecError> {
        match &self.claimed {
            Claimed::Row(_) => None,
            Claimed::Missing(_) => Some(LazyLock::force(&ROW_GONE)),
            Claimed::Undecodable { error, .. } => Some(error),
        }
    }

    fn partition_key(&self) -> Option<&[u8]> {
        match &self.claimed {
            Claimed::Row(row) => Row::partition_key(row),
            Claimed::Missing(_) | Claimed::Undecodable { .. } => None,
        }
    }

    fn redelivery_count(&self) -> Option<u64> {
        let carried = match &self.claimed {
            Claimed::Row(row) => Row::attempt(row),
            Claimed::Undecodable { attempt, .. } => *attempt,
            Claimed::Missing(_) => None,
        };
        // A claim that returns its rows after counting reports the attempt before its count.
        if self.queue.counted_attempt {
            carried.map(|attempt| attempt.saturating_sub(1))
        } else {
            carried
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
        // A delivery whose attempts are spent has no delayed redelivery to offer: the runtime then
        // settles it with `nack(true)`, which moves the row, and expects nothing back.
        self.queue.native_retry_after && self.spent().is_none()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        if !self.supports_nack_after() {
            return Err(AckError::Unsupported);
        }
        self.settle(Outcome::RetryAfter(delay)).await
    }
}

impl<DB: QueueDatabase, Row: Events<DB>, Mode> Drop for InboxDelivery<DB, Row, Mode> {
    fn drop(&mut self) {
        let Some(hold) = self.hold.take() else {
            return;
        };
        #[cfg(feature = "testing")]
        let in_process = self.in_process.take();
        match hold {
            // In process the rollback runs off a paused clock too.
            #[cfg(feature = "testing")]
            Hold::Own(tx) if in_process.is_some() => {
                if let Ok(runtime) = Handle::try_current() {
                    drop(runtime.spawn(off_clock(tx.into_inner().rollback())));
                }
            }
            // An unsettled delivery's transaction rolls back on the runtime, which returns the
            // row to the queue at once. Without a runtime the transaction drops here: its
            // connection closes, and the server rolls it back.
            Hold::Own(tx) => {
                if let Ok(runtime) = Handle::try_current() {
                    drop(runtime.spawn(tx.into_inner().rollback()));
                }
            }
            Hold::Batch(batch) => batch.release(self.queue),
            // Nothing async runs in `drop`: the transaction ends as its hold drops, its connection
            // closed rather than rolled back, as the handler may have left a statement midway on
            // it; the server rolls the transaction back and the row returns at once.
            Hold::Lent(hold) => drop(hold),
            Hold::Advisory(hold) => {
                #[cfg(feature = "testing")]
                if let Some(connection) = in_process {
                    // The row comes back once its session ends; it is counted first, so a claim
                    // that takes it at once finds it counted.
                    connection.harness.expect(self.queue.name);
                    drop(hold);
                    connection.harness.released();
                    return;
                }
                // Nothing async runs in `drop`: the session closes on the runtime the broker
                // connected on, after releasing the lock, and the row returns at once.
                drop(hold);
            }
            Hold::Lease { book, slot, tx } => {
                // The delivery's own transaction ends first: its connection closes, and the server
                // rolls back what the handler wrote, so the release below waits for nothing of it.
                drop(tx);
                let id = self.claimed.id::<DB>().clone();
                #[cfg(feature = "testing")]
                if let Some(connection) = in_process {
                    release_in_process(connection, book, slot, self.queue, id);
                    return;
                }
                // Nothing async runs in `drop`: the release runs on the runtime the broker
                // connected on, and returns the row to the queue at once. With that runtime gone
                // the task never runs, and the row returns once its lease runs out.
                drop(book.runtime().spawn(release::<DB, Row>(
                    book,
                    slot,
                    self.queue,
                    id,
                    Now::default(),
                )));
            }
        }
        #[cfg(feature = "testing")]
        if let Some(connection) = in_process {
            connection.harness.returned(self.queue.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::mem;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

    use super::{row_gone, unlent_payload};
    use crate::inbox::engine::{IdAt, Prepared};
    use crate::inbox::queue::Queue;

    /// The fields of each event logged while it is the default subscriber, by name, with its level.
    #[derive(Default)]
    struct Logged(Mutex<Vec<(Level, BTreeMap<String, String>)>>);

    struct Fields<'a>(&'a mut BTreeMap<String, String>);

    impl Visit for Fields<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    impl Subscriber for Logged {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _: &Id, _: &Record<'_>) {}

        fn record_follows_from(&self, _: &Id, _: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = BTreeMap::new();
            event.record(&mut Fields(&mut fields));
            self.0
                .lock()
                .expect("the log is not poisoned")
                .push((*event.metadata().level(), fields));
        }

        fn enter(&self, _: &Id) {}

        fn exit(&self, _: &Id) {}
    }

    #[test]
    fn a_claimed_id_without_a_row_is_logged_with_its_id() {
        let queue = Queue {
            name: "mail",
            table: "mail_jobs",
            row: "app::FetchedMail",
            spec: TableSpec::new("mail_jobs", Column::new("job_id"), Form::RowLock),
            id_at: IdAt::First,
            native_retry_after: false,
            kinds: None,
            prepared: Prepared::default(),
            begin_claim: None,
            counted_attempt: false,
            one_writer: false,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        };
        let logged = Arc::new(Logged::default());
        tracing::subscriber::with_default(Arc::clone(&logged), || row_gone(&queue, &42_i64));
        let logged = mem::take(&mut *logged.0.lock().expect("the log is not poisoned"));
        let [(level, fields)] = logged.as_slice() else {
            panic!("one event, not {}", logged.len());
        };
        assert_eq!(*level, Level::WARN);
        assert_eq!(fields.get("id").map(String::as_str), Some("42"));
        assert_eq!(fields.get("subscription").map(String::as_str), Some("mail"));
        assert_eq!(fields.get("table").map(String::as_str), Some("mail_jobs"));
        assert_eq!(
            fields.get("row").map(String::as_str),
            Some("app::FetchedMail")
        );
    }

    #[test]
    fn a_payload_read_from_a_row_mode_delivery_names_the_row_to_take() {
        assert_eq!(
            unlent_payload("app::SendEmail"),
            "the table is in row mode: its deliveries lend the row itself and carry no payload, so \
             a handler that decodes one fails each delivery by the decode policy; take \
             `&app::SendEmail` as the handler's input, `&[app::SendEmail]` in a batch",
        );
    }
}
