//! Transactional mode: a handler that writes through its delivery's transaction, which
//! acknowledgement commits; the step that switches a registration to it, the transaction the
//! handler borrows, and the book a subscription lends its deliveries' transactions from.

use std::fmt;
use std::mem::{self, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::thread;

use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};
use sqlx::Database;

use super::database::QueueDatabase;
use super::queue::InboxQueue;
use super::tx::PoolTx;

/// A subscription whose handler leaves the delivery's transaction alone: the default mode.
///
/// [`InboxQueue::new`] opens a subscription in it.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx::{Inbox, InboxQueue, Plain};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "report_jobs")]
/// pub struct Report {
///     #[field(id)]
///     id: i64,
///     #[field(group)]
///     month: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// /// The subscription to one month's reports, a group of its own.
/// fn month(name: &'static str) -> InboxQueue<Report, Plain> {
///     InboxQueue::new(name)
/// }
/// # let _ = month("2026-10");
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Plain;

/// A subscription whose handler writes through the delivery's transaction, which acknowledgement
/// commits.
///
/// The mount-site step [`transactional`](InboxSettings::transactional) switches a registration to
/// it: the handler then takes the transaction with `Ctx<keys::Tx<DB>>`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::{PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "signup_jobs")]
/// pub struct Signup {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(serde::Deserialize)]
/// struct Account {
///     email: String,
/// }
///
/// #[subscriber(InboxQueue::<Signup>::new("signups"))]
/// async fn open_account(account: &Account, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
///     let opened = sqlx::query("INSERT INTO accounts (email) VALUES ($1)")
///         .bind(&account.email)
///         .execute(&mut *tx)
///         .await;
///     if opened.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("accounts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // The registration's `InboxQueue<Signup, Plain>` becomes
///         // `InboxQueue<Signup, Transactional>`.
///         b.include(open_account.transactional());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Transactional;

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::Plain {}

    impl Sealed for super::Transactional {}
}

/// The mode of a subscription as a type: [`Plain`] or [`Transactional`]. Machinery; a descriptor,
/// its subscriber and its deliveries carry it.
#[doc(hidden)]
pub trait InboxMode: sealed::Sealed + Send + Sync + 'static {
    /// Whether the handler writes through the delivery's transaction.
    const TRANSACTIONAL: bool;
}

impl InboxMode for Plain {
    const TRANSACTIONAL: bool = false;
}

impl InboxMode for Transactional {
    const TRANSACTIONAL: bool = true;
}

/// The inbox's own mount-site steps, on a `#[subscriber]` definition and on its settings builder.
///
/// The prelude brings it in, beside the framework's own settings.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::{PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "invoice_jobs")]
/// pub struct InvoiceJob {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(serde::Deserialize)]
/// struct Invoice {
///     number: i64,
/// }
///
/// #[subscriber(InboxQueue::<InvoiceJob>::new("invoices"))]
/// async fn book(invoice: &Invoice, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
///     let booked = sqlx::query("INSERT INTO booked (number) VALUES ($1)")
///         .bind(invoice.number)
///         .execute(&mut *tx)
///         .await;
///     if booked.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // The inbox's step chains with the framework's.
///         b.include(book.transactional().workers(nonzero!(4)));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait InboxSettings: Declared {
    /// Switches an `InboxQueue` subscription to transactional mode: the handler takes the
    /// delivery's transaction (`Ctx<keys::Tx<DB>>`), and acknowledgement commits what it wrote.
    ///
    /// The handler writes through [`Tx`], which dereferences to the connection inside the
    /// delivery's transaction. Acknowledgement commits the handler's writes together with the
    /// settlement of the row. A retry, a delayed retry, a drop and a dead-letter move roll the
    /// handler's writes back first, then settle the row as outside transactional mode, and so
    /// does an acknowledgement after the handler panicked. A delivery dropped unsettled ends its
    /// transaction, and the row returns at once.
    ///
    /// Transactional mode runs in the row lock form and lends the claim's transaction: the
    /// subscription sets a savepoint right after each claim, one more statement per delivery, so a
    /// retry discards the handler's writes and still counts the attempt. A subscription in the
    /// lease or advisory lock form refuses it when it starts. Each delivery in work holds one pool
    /// connection, so a handler that takes another connection of the same pool needs a pool larger
    /// than its `workers(n)`. Transactional mode serves single deliveries: a batch handler mounted
    /// with it does not compile.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::prelude::*;
    /// use sqlx::{PgPool, Postgres};
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "stock_jobs")]
    /// pub struct StockJob {
    ///     #[field(id)]
    ///     id: i64,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Restock {
    ///     sku: String,
    ///     count: i32,
    /// }
    ///
    /// #[subscriber(InboxQueue::<StockJob>::new("restocks"))]
    /// async fn restock(order: &Restock, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
    ///     let raised = sqlx::query("UPDATE stock SET count = count + $1 WHERE sku = $2")
    ///         .bind(order.count)
    ///         .bind(&order.sku)
    ///         .execute(&mut *tx)
    ///         .await;
    ///     // The count rises exactly when the job is finished: both commit together.
    ///     if raised.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     RustStream::new(AppInfo::new("stock", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
    ///         b.include(restock.transactional());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn transactional(self) -> <Self::Settings as TransactionalStep>::Out
    where
        Self::Settings: TransactionalStep,
    {
        self.declare().apply_transactional()
    }
}

impl<Def: Declared> InboxSettings for Def {}

/// Switching a registration to transactional mode. Machinery behind
/// [`InboxSettings::transactional`]; never named in a service.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.transactional()` switches an `InboxQueue` subscription to transactional mode",
    label = "this registration does not subscribe through `InboxQueue`",
    note = "a subscription by name has no transactional mode: subscribe with \
            `InboxQueue::<Row>::new(..)`"
)]
pub trait TransactionalStep {
    /// The registration in transactional mode.
    type Out;

    /// Switches the registration's descriptor to transactional mode.
    fn apply_transactional(self) -> Self::Out;
}

impl<Def, Row, State, DefCodec> TransactionalStep
    for SubscriberBuilder<Def, InboxQueue<Row, Plain>, State, DefCodec>
where
    Def: Declared,
{
    type Out = SubscriberBuilder<Def, InboxQueue<Row, Transactional>, State, DefCodec>;

    fn apply_transactional(self) -> Self::Out {
        self.map_source(InboxQueue::into_transactional)
    }
}

/// The delivery's transaction, lent to the handler: it dereferences to the connection the
/// transaction runs on, and goes back to the delivery when the handler ends.
///
/// A handler takes it as its first `Ctx` parameter, `Ctx(mut tx): Ctx<keys::Tx<DB>>`, on a
/// registration mounted with [`transactional`](InboxSettings::transactional). It writes through
/// `&mut *tx`, as through the connection of a sqlx transaction, and `(&mut *tx).begin()` nests a
/// savepoint of its own. The delivery's settlement ends the transaction: acknowledgement commits
/// it, every other outcome rolls the handler's writes back first. Dropping `Tx` gives the
/// transaction back and ends nothing. A `Tx` dropped by a panic marks the handler's writes for
/// the rollback, so a delivery acknowledged after its handler panicked, as the `skip` panic policy
/// does, commits none of them.
///
/// `Tx` is an owned value, so a handler may move it into a task of its own. The transaction must
/// be back when the delivery settles: a settlement that finds it still lent fails with
/// [`SqlxBrokerError::TransactionHeld`](crate::SqlxBrokerError::TransactionHeld), and the
/// transaction rolls back when that `Tx` drops.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::{PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "payment_jobs")]
/// pub struct PaymentJob {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(serde::Deserialize)]
/// struct Payment {
///     account: String,
///     cents: i64,
/// }
///
/// #[subscriber(InboxQueue::<PaymentJob>::new("payments"))]
/// async fn post(payment: &Payment, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
///     let posted = sqlx::query("INSERT INTO ledger (account, cents) VALUES ($1, $2)")
///         .bind(&payment.account)
///         .bind(payment.cents)
///         .execute(&mut *tx)
///         .await;
///     match posted {
///         // The commit keeps the ledger row and finishes the job, together.
///         Ok(_) => HandlerOutcome::ack(),
///         // The retry discards the ledger row and returns the job to the queue.
///         Err(error) => {
///             tracing::warn!(%error, "the payment was not posted");
///             HandlerOutcome::retry()
///         }
///     }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(post.transactional());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct Tx<DB: QueueDatabase> {
    /// The transaction, until `Tx` drops and gives it back.
    lent: Option<Lent<DB>>,
    lender: Lender<DB>,
}

impl<DB: QueueDatabase> Tx<DB> {
    /// The transaction `lender` lends, out of its book until the handler ends; `None` when the book
    /// does not hold it for the delivery, as once it is lent.
    pub(crate) fn lent_by(lender: Lender<DB>) -> Option<Self> {
        Some(Self {
            lent: Some(lender.lend()?),
            lender,
        })
    }

    fn lent(&self) -> &Lent<DB> {
        self.lent
            .as_ref()
            .expect("a `Tx` holds its transaction until it drops")
    }

    fn lent_mut(&mut self) -> &mut Lent<DB> {
        self.lent
            .as_mut()
            .expect("a `Tx` holds its transaction until it drops")
    }
}

impl<DB: QueueDatabase> Deref for Tx<DB> {
    type Target = DB::Connection;

    /// The connection inside the delivery's transaction.
    ///
    /// # Panics
    ///
    /// Never: `Tx` holds the transaction until it drops.
    fn deref(&self) -> &DB::Connection {
        self.lent().conn()
    }
}

impl<DB: QueueDatabase> DerefMut for Tx<DB> {
    /// The connection inside the delivery's transaction, to run a statement on.
    ///
    /// # Panics
    ///
    /// Never: `Tx` holds the transaction until it drops.
    fn deref_mut(&mut self) -> &mut DB::Connection {
        self.lent_mut().conn_mut()
    }
}

impl<DB: QueueDatabase> AsMut<DB::Connection> for Tx<DB> {
    fn as_mut(&mut self) -> &mut DB::Connection {
        self
    }
}

impl<DB: QueueDatabase> Drop for Tx<DB> {
    fn drop(&mut self) {
        if let Some(lent) = self.lent.take() {
            // A handler that panicked half way through its writes gives them back marked, so no
            // outcome commits them.
            self.lender.give_back(lent, thread::panicking());
        }
    }
}

impl<DB: QueueDatabase> fmt::Debug for Tx<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx").finish_non_exhaustive()
    }
}

/// What a delivery lends its handler: the transaction it settles in.
pub(crate) enum Lent<DB: Database> {
    /// A transaction of the crate's own on a pool connection: the claim's in the row lock form.
    Tx(PoolTx<DB>),
}

impl<DB: Database> Lent<DB> {
    fn conn(&self) -> &DB::Connection {
        match self {
            Self::Tx(tx) => tx,
        }
    }

    fn conn_mut(&mut self) -> &mut DB::Connection {
        match self {
            Self::Tx(tx) => tx,
        }
    }
}

/// Where a delivery's handler borrows the delivery's transaction from, and gives it back to: its
/// slot in the subscription's book. Copied into the handler's context: a reference, the slot's
/// index and its generation.
pub(crate) enum Lender<DB: Database> {
    /// The delivery's slot in a [`TxBook`].
    Tx(&'static TxBook<DB>, TxSlot),
}

impl<DB: Database> Clone for Lender<DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: Database> Copy for Lender<DB> {}

impl<DB: Database> fmt::Debug for Lender<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tx(_, slot) => f.debug_tuple("Tx").field(slot).finish(),
        }
    }
}

impl<DB: Database> Lender<DB> {
    /// The delivery's transaction, out of its book for the handler: one lock. `None` when the book
    /// does not hold it for this delivery, as once it is lent.
    pub(crate) fn lend(self) -> Option<Lent<DB>> {
        match self {
            Self::Tx(book, slot) => book.lend(slot).map(Lent::Tx),
        }
    }

    /// `lent` back into the delivery's slot after the handler, `panicked` when a panic dropped
    /// it: one lock. A slot that no longer waits for it, its delivery settled or dropped
    /// meanwhile, leaves the transaction to end here: its connection closes, and the server rolls
    /// it back.
    pub(crate) fn give_back(self, lent: Lent<DB>, panicked: bool) {
        match (self, lent) {
            (Self::Tx(book, slot), Lent::Tx(tx)) => book.give_back(slot, tx, panicked),
        }
    }
}

/// A delivery's place in a [`TxBook`]: the slot, and which of the deliveries that held the slot it
/// is, so a lender of an earlier one never reaches a later one's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxSlot {
    index: usize,
    generation: u64,
}

/// The transactions of one transactional subscription's deliveries in work, each in the slot its
/// delivery holds, from the claim until the settlement; the handler borrows it from there.
///
/// A subscription leaks its book once, so a delivery and its handler's [`Tx`] reach the book
/// through a `'static` reference, with no reference count per message. Entering, lending, giving
/// back and leaving each take the book's lock once; a delivery allocates only when the
/// subscription has more deliveries in work than ever before.
pub(crate) struct TxBook<DB: Database> {
    slots: Mutex<TxSlots<DB>>,
}

/// Every slot the book has held, and which of them are free.
struct TxSlots<DB: Database> {
    entries: Vec<TxEntry<DB>>,
    /// The free slots, by index; a delivery takes one of them before it adds a slot.
    free: Vec<usize>,
}

/// One slot: which delivery holds it, and where its transaction is.
struct TxEntry<DB: Database> {
    /// Counts the deliveries that held the slot; the current one's lender carries it.
    generation: u64,
    standing: TxStanding<DB>,
}

/// Where a slot's transaction is.
enum TxStanding<DB: Database> {
    /// No delivery holds the slot.
    Free,
    /// The transaction waits in the slot while the handler does not hold it.
    Held(Returned<DB>),
    /// The handler holds the transaction.
    Lent,
}

/// A delivery's transaction as its settlement finds it.
pub(crate) struct Returned<DB: Database> {
    /// The transaction.
    pub(crate) tx: PoolTx<DB>,
    /// Whether a panic of the handler gave it back: what the handler wrote is rolled back,
    /// whatever the outcome.
    pub(crate) panicked: bool,
}

impl<DB: Database> TxBook<DB> {
    /// A book for a subscription, for the life of the process.
    pub(crate) fn leak() -> &'static Self {
        Box::leak(Box::new(Self {
            slots: Mutex::new(TxSlots {
                entries: Vec::new(),
                free: Vec::new(),
            }),
        }))
    }

    /// Records a delivery's transaction `tx` until the delivery settles.
    pub(crate) fn enter(&'static self, tx: PoolTx<DB>) -> TxHold<DB> {
        let mut slots = self.slots();
        let TxSlots { entries, free } = &mut *slots;
        let reused = free
            .pop()
            .and_then(|index| Some((index, entries.get_mut(index)?)));
        let held = TxStanding::Held(Returned {
            tx,
            panicked: false,
        });
        let slot = if let Some((index, entry)) = reused {
            entry.generation = entry.generation.wrapping_add(1);
            entry.standing = held;
            TxSlot {
                index,
                generation: entry.generation,
            }
        } else {
            entries.push(TxEntry {
                generation: 0,
                standing: held,
            });
            TxSlot {
                index: entries.len() - 1,
                generation: 0,
            }
        };
        drop(slots);
        TxHold { book: self, slot }
    }

    /// The transaction in `slot`, out of the book for the handler; `None` unless it waits there for
    /// the delivery `slot` names.
    fn lend(&self, slot: TxSlot) -> Option<PoolTx<DB>> {
        let mut slots = self.slots();
        let lent = slots
            .entries
            .get_mut(slot.index)
            .filter(|entry| entry.generation == slot.generation)
            .and_then(
                |entry| match mem::replace(&mut entry.standing, TxStanding::Lent) {
                    TxStanding::Held(held) => Some(held.tx),
                    other => {
                        entry.standing = other;
                        None
                    }
                },
            );
        drop(slots);
        lent
    }

    /// `tx` back into `slot` after the handler, `panicked` when a panic dropped it; a slot that no
    /// longer waits for it leaves it to end here.
    fn give_back(&self, slot: TxSlot, tx: PoolTx<DB>, panicked: bool) {
        let mut slots = self.slots();
        let refused = match slots.entries.get_mut(slot.index) {
            Some(entry)
                if entry.generation == slot.generation
                    && matches!(entry.standing, TxStanding::Lent) =>
            {
                entry.standing = TxStanding::Held(Returned { tx, panicked });
                None
            }
            _ => Some(tx),
        };
        drop(slots);
        // The transaction ends outside the book's lock: its connection closes.
        drop(refused);
    }

    /// Frees `slot` as its delivery settles or drops, and hands over the transaction where it waits
    /// in the slot; `None` while the handler holds it.
    fn leave(&self, slot: TxSlot) -> Option<Returned<DB>> {
        let mut slots = self.slots();
        let TxSlots { entries, free } = &mut *slots;
        let entry = entries
            .get_mut(slot.index)
            .filter(|entry| entry.generation == slot.generation)?;
        let left = mem::replace(&mut entry.standing, TxStanding::Free);
        if matches!(left, TxStanding::Free) {
            return None;
        }
        free.push(slot.index);
        drop(slots);
        match left {
            TxStanding::Held(held) => Some(held),
            TxStanding::Free | TxStanding::Lent => None,
        }
    }

    fn slots(&self) -> MutexGuard<'_, TxSlots<DB>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A transactional delivery's place in its subscription's book: entered at the claim, left once,
/// when the delivery settles. Dropped unconsumed, the unsettled drop, it ends the transaction
/// waiting in the slot: the connection closes, the server rolls the transaction back, and the row
/// returns at once.
pub(crate) struct TxHold<DB: Database> {
    book: &'static TxBook<DB>,
    slot: TxSlot,
}

impl<DB: Database> TxHold<DB> {
    /// Where the delivery's handler borrows the transaction from.
    pub(crate) const fn lender(&self) -> Lender<DB> {
        Lender::Tx(self.book, self.slot)
    }

    /// The transaction for the delivery's settlement, its slot freed; `None` while the handler
    /// still holds it, which then gives it back to no one and ends it.
    pub(crate) fn settle(self) -> Option<Returned<DB>> {
        let hold = ManuallyDrop::new(self);
        hold.book.leave(hold.slot)
    }
}

impl<DB: Database> Drop for TxHold<DB> {
    fn drop(&mut self) {
        // Nothing async runs here: a transaction still open closes its connection as it drops.
        drop(self.book.leave(self.slot));
    }
}

impl<DB: Database> fmt::Debug for TxHold<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxHold")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Error, Sqlite, SqlitePool};

    use super::{Lender, TxBook, TxSlot};
    use crate::inbox::tx::PoolTx;

    async fn pool() -> Result<SqlitePool, Error> {
        SqlitePoolOptions::new()
            .max_connections(4)
            .connect("sqlite::memory:")
            .await
    }

    fn slot(lender: Lender<Sqlite>) -> TxSlot {
        let Lender::Tx(_, slot) = lender;
        slot
    }

    #[tokio::test]
    async fn the_handler_borrows_the_transaction_once_and_gives_it_back() -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let hold = book.enter(PoolTx::begin(&pool, None).await?);
        let lender = hold.lender();
        let lent = lender
            .lend()
            .expect("the book lends the delivery's transaction");
        assert!(lender.lend().is_none(), "a second borrower finds it lent");
        lender.give_back(lent, false);
        let returned = hold.settle().expect("the settlement finds it back");
        assert!(!returned.panicked);
        returned.tx.rollback().await
    }

    #[tokio::test]
    async fn a_panic_marks_the_transaction_it_gives_back() -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let hold = book.enter(PoolTx::begin(&pool, None).await?);
        let lent = hold.lender().lend().expect("lends");
        hold.lender().give_back(lent, true);
        let returned = hold.settle().expect("the settlement finds it back");
        assert!(
            returned.panicked,
            "the settlement discards what the handler wrote"
        );
        returned.tx.rollback().await
    }

    #[tokio::test]
    async fn a_transaction_kept_past_its_settlement_never_reaches_the_next_delivery()
    -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let first = book.enter(PoolTx::begin(&pool, None).await?);
        let stale = first.lender();
        let kept = stale.lend().expect("lends");
        assert!(
            first.settle().is_none(),
            "a settlement finds the transaction still lent"
        );
        // The next delivery takes the freed slot, and its handler borrows its own transaction.
        let second = book.enter(PoolTx::begin(&pool, None).await?);
        assert_eq!(slot(second.lender()).index, slot(stale).index);
        let borrowed = second.lender().lend().expect("lends");
        // The first transaction comes back late: the slot it left refuses it, so it ends there.
        stale.give_back(kept, false);
        assert!(
            second.lender().lend().is_none(),
            "the slot still waits for the second delivery's own transaction"
        );
        second.lender().give_back(borrowed, false);
        let returned = second
            .settle()
            .expect("the settlement finds its own transaction");
        returned.tx.rollback().await
    }

    #[tokio::test]
    async fn a_hold_dropped_unsettled_frees_its_slot() -> Result<(), Error> {
        let pool = pool().await?;
        let book = TxBook::<Sqlite>::leak();
        let dropped = book.enter(PoolTx::begin(&pool, None).await?);
        let stale = dropped.lender();
        drop(dropped);
        assert!(
            stale.lend().is_none(),
            "the dropped delivery's transaction ended"
        );
        let next = book.enter(PoolTx::begin(&pool, None).await?);
        assert_eq!(
            slot(next.lender()),
            TxSlot {
                index: slot(stale).index,
                generation: slot(stale).generation + 1,
            },
            "the next delivery takes the slot"
        );
        let returned = next.settle().expect("held");
        returned.tx.rollback().await
    }
}
