//! The transaction a transactional delivery lends its handler, and where the lender finds it: in a
//! book of transactions, or on the session of an advisory lock.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::thread;

use sqlx::Database;

use super::book::{TxBook, TxSlot};
use crate::inbox::database::QueueDatabase;
use crate::inbox::form::advisory::{LockBook, LockSlot, Locked};
use crate::inbox::tx::PoolTx;

/// The delivery's transaction, lent to the handler: it dereferences to the connection the
/// transaction runs on, and goes back to the delivery when the handler ends.
///
/// A handler takes it as its first `Ctx` parameter, `Ctx(mut tx): Ctx<keys::Tx<DB>>`, on a
/// registration mounted with [`transactional`](super::InboxSettings::transactional). It writes
/// through `&mut *tx`, as through the connection of a sqlx transaction, and `(&mut *tx).begin()`
/// nests a savepoint of its own. The delivery's settlement ends the transaction: acknowledgement
/// commits it, every other outcome rolls the handler's writes back first. Dropping `Tx` gives the
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
}

impl<DB: QueueDatabase> Tx<DB> {
    /// The transaction `lender` lends, out of its book until the handler ends; `None` when the book
    /// does not hold it for the delivery, as once it is lent.
    pub(crate) fn lent_by(lender: Lender<DB>) -> Option<Self> {
        Some(Self {
            lent: Some(lender.lend()?),
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
            lent.give_back(thread::panicking());
        }
    }
}

impl<DB: QueueDatabase> fmt::Debug for Tx<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx").finish_non_exhaustive()
    }
}

/// What a delivery lends its handler: the transaction it settles in, with the slot it goes back
/// to.
pub(crate) enum Lent<DB: Database> {
    /// A transaction of the crate's own on a pool connection, from its slot in a [`TxBook`]: the
    /// claim's in the row lock form, the delivery's own in the lease form.
    Tx {
        book: &'static TxBook<DB>,
        slot: TxSlot,
        tx: PoolTx<DB>,
    },
    /// The session that holds the lock on the row's key, its transaction open, from its slot in a
    /// [`LockBook`]: the advisory lock form.
    Session {
        book: &'static LockBook<DB>,
        slot: LockSlot,
        locked: Locked<'static, DB>,
    },
}

impl<DB: Database> Lent<DB> {
    fn conn(&self) -> &DB::Connection {
        match self {
            Self::Tx { tx, .. } => tx,
            Self::Session { locked, .. } => locked.conn_ref(),
        }
    }

    fn conn_mut(&mut self) -> &mut DB::Connection {
        match self {
            Self::Tx { tx, .. } => tx,
            Self::Session { locked, .. } => locked.conn(),
        }
    }

    /// Back into the delivery's slot after the handler, `panicked` when a panic dropped it: one
    /// lock. A slot that no longer waits for it, its delivery settled or dropped meanwhile, leaves
    /// it to end: a transaction's connection closes and the server rolls it back; a session closes
    /// after the unlock of its key, which ends its transaction too.
    pub(super) fn give_back(self, panicked: bool) {
        match self {
            Self::Tx { book, slot, tx } => book.give_back(slot, tx, panicked),
            Self::Session { book, slot, locked } => book.give_back(slot, locked, panicked),
        }
    }
}

/// Where a delivery's handler borrows the delivery's transaction from: its slot in the
/// subscription's book. Copied into the handler's context: a reference, the slot's index and its
/// generation.
pub(crate) enum Lender<DB: Database> {
    /// The delivery's slot in a [`TxBook`].
    Tx(&'static TxBook<DB>, TxSlot),
    /// The delivery's slot in a [`LockBook`], whose session holds the transaction.
    Lock(&'static LockBook<DB>, LockSlot),
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
            Self::Lock(_, slot) => f.debug_tuple("Lock").field(slot).finish(),
        }
    }
}

impl<DB: Database> Lender<DB> {
    /// The delivery's transaction, out of its book for the handler: one lock. `None` when the book
    /// does not hold it for this delivery, as once it is lent.
    pub(crate) fn lend(self) -> Option<Lent<DB>> {
        match self {
            Self::Tx(book, slot) => book.lend(slot).map(|tx| Lent::Tx { book, slot, tx }),
            Self::Lock(book, slot) => {
                book.lend(slot)
                    .map(|locked| Lent::Session { book, slot, locked })
            }
        }
    }
}
