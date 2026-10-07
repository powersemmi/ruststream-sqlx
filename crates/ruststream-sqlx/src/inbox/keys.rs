//! What a handler reads off the delivery it handles, through `Ctx<Key>`.
//!
//! A handler's first `Ctx` names its context: [`Tx`] reads [`TxContext`], which a transactional
//! delivery lends; [`Pool`] reads [`PoolContext`], which every delivery of an `InboxQueue`
//! subscription lends; [`Attempt`] reads [`InboxContext`], which any delivery lends. A key after
//! the first reads any context that carries it, so a handler takes them in the order `Tx`, then
//! `Pool`, then `Attempt`, and leaves out the ones it does not need.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use ruststream::runtime::{Context, Ctx, FromContext};
use ruststream::{BuildContext, ContextField, Field, IncomingMessage};
use sqlx::Pool as DbPool;

use super::database::QueueDatabase;
use super::delivery::InboxDelivery;
use super::engine::Events;
use super::transactional::{InboxMode, Lender};

/// A delivery's context: what the inbox lends a handler besides the message.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream_sqlx::keys::Attempt;
/// # use ruststream_sqlx::{Inbox, InboxQueue};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(attempt)] attempt: i16, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Report { id: u64 }
///
/// #[subscriber(InboxQueue::<Job>::new("reports"))]
/// async fn render(report: &Report, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
///     tracing::info!(report.id, ?attempt, "rendering");
///     HandlerOutcome::ack()
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct InboxContext {
    attempt: Option<u64>,
}

impl<M: IncomingMessage> BuildContext<M> for InboxContext {
    fn build(msg: &M) -> Self {
        Self {
            attempt: msg.redelivery_count(),
        }
    }
}

/// What every delivery of an `InboxQueue` subscription on `DB` lends: the context
/// `Ctx<keys::Pool<DB>>` reads, and `Ctx<keys::Attempt>` after it.
///
/// It is built only for a handler whose first `Ctx` reads it, and copies the pool's reference
/// and the attempt.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::Postgres;
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(attempt)] attempt: i16, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Upload { name: String }
///
/// // `keys::Pool` comes first, so the handler's context is `PoolContext<Postgres>`.
/// #[subscriber(InboxQueue::<Job>::new("uploads"))]
/// async fn scan(
///     upload: &Upload,
///     Ctx(pool): Ctx<keys::Pool<Postgres>>,
///     Ctx(attempt): Ctx<keys::Attempt>,
/// ) -> HandlerOutcome {
///     let logged = sqlx::query("INSERT INTO scans (name, attempt) VALUES ($1, $2)")
///         .bind(&upload.name)
///         .bind(attempt.and_then(|attempt| i64::try_from(attempt).ok()))
///         .execute(&pool)
///         .await;
///     if logged.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
/// # }
/// # fn main() {}
/// ```
pub struct PoolContext<DB: QueueDatabase> {
    pool: &'static DbPool<DB>,
    attempt: Option<u64>,
}

impl<DB: QueueDatabase> Clone for PoolContext<DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: QueueDatabase> Copy for PoolContext<DB> {}

impl<DB: QueueDatabase> fmt::Debug for PoolContext<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolContext")
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

impl<DB, Row, Mode> BuildContext<InboxDelivery<DB, Row, Mode>> for PoolContext<DB>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
{
    fn build(msg: &InboxDelivery<DB, Row, Mode>) -> Self {
        Self {
            pool: msg.pool(),
            attempt: msg.redelivery_count(),
        }
    }
}

/// What a transactional delivery on `DB` lends its handler: the context `Ctx<keys::Tx<DB>>`
/// reads, and `Ctx<keys::Pool<DB>>` and `Ctx<keys::Attempt>` after it.
///
/// It is built only for a handler whose first `Ctx` reads it, and copies two references, where
/// the delivery's transaction waits and the pool, with the transaction's slot and the attempt.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::{PgPool, Postgres};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(attempt)] attempt: i16, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Transfer { from: String, to: String, cents: i64 }
///
/// // `keys::Tx` comes first, so the handler's context is `TxContext<Postgres>`, and the keys
/// // after it read the same context.
/// #[subscriber(InboxQueue::<Job>::new("transfers"))]
/// async fn transfer(
///     order: &Transfer,
///     Ctx(mut tx): Ctx<keys::Tx<Postgres>>,
///     Ctx(pool): Ctx<keys::Pool<Postgres>>,
///     Ctx(attempt): Ctx<keys::Attempt>,
/// ) -> HandlerOutcome {
///     // Through the pool: the attempt stays logged whatever the outcome.
///     let _ = sqlx::query("INSERT INTO transfer_log (sender, attempt) VALUES ($1, $2)")
///         .bind(&order.from)
///         .bind(attempt.and_then(|attempt| i64::try_from(attempt).ok()))
///         .execute(&pool)
///         .await;
///     // Through the transaction: both legs commit with the acknowledgement, or neither does.
///     let debited = sqlx::query("UPDATE accounts SET cents = cents - $1 WHERE name = $2")
///         .bind(order.cents)
///         .bind(&order.from)
///         .execute(&mut *tx)
///         .await;
///     if debited.is_err() {
///         return HandlerOutcome::retry();
///     }
///     let credited = sqlx::query("UPDATE accounts SET cents = cents + $1 WHERE name = $2")
///         .bind(order.cents)
///         .bind(&order.to)
///         .execute(&mut *tx)
///         .await;
///     if credited.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("bank", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(transfer.transactional());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct TxContext<DB: QueueDatabase> {
    lender: Lender<DB>,
    pool: &'static DbPool<DB>,
    attempt: Option<u64>,
}

impl<DB: QueueDatabase> TxContext<DB> {
    /// The context of a delivery whose transaction `lender` lends, on `pool`, at `attempt`.
    pub(crate) const fn new(
        lender: Lender<DB>,
        pool: &'static DbPool<DB>,
        attempt: Option<u64>,
    ) -> Self {
        Self {
            lender,
            pool,
            attempt,
        }
    }
}

impl<DB: QueueDatabase> Clone for TxContext<DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: QueueDatabase> Copy for TxContext<DB> {}

impl<DB: QueueDatabase> fmt::Debug for TxContext<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxContext")
            .field("lender", &self.lender)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

/// A delivery that lends its transaction to its handler: one of a transactional subscription.
/// Machinery; it gates the [`TxContext`] a handler taking `Ctx<keys::Tx<..>>` reads, so mounting
/// that handler without transactional mode does not compile.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "the handler takes `Ctx<keys::Tx<..>>`, and `{Self}` comes from a subscription \
               without transactional mode",
    label = "the delivery's transaction is lent only in transactional mode",
    note = "mount the handler with `.transactional()`: `b.include(handler.transactional())`"
)]
pub trait TransactionalDelivery<DB: QueueDatabase> {
    /// The context the delivery lends its handler.
    fn tx_context(&self) -> TxContext<DB>;
}

impl<DB, Row, Mode> BuildContext<InboxDelivery<DB, Row, Mode>> for TxContext<DB>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    InboxDelivery<DB, Row, Mode>: TransactionalDelivery<DB>,
{
    fn build(msg: &InboxDelivery<DB, Row, Mode>) -> Self {
        msg.tx_context()
    }
}

/// The delivery's attempt, 1 for the first delivery: the row's `attempt` as it stood before the
/// claim; `None` for a table without one.
///
/// Retry backoff is the handler's: it reads the attempt and answers with
/// `HandlerOutcome::retry_after(..)`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream_sqlx::keys::Attempt;
/// # use ruststream_sqlx::{Inbox, InboxQueue};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(retry_after)] retry_after: chrono::DateTime<chrono::Utc>, #[field(attempt)] attempt: i16, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Charge { order: u64 }
/// # async fn try_charge(_: &Charge) -> bool { true }
///
/// #[subscriber(InboxQueue::<Job>::new("charges"))]
/// async fn charge(request: &Charge, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
///     if try_charge(request).await {
///         return HandlerOutcome::ack();
///     }
///     // Doubling backoff: 1s, 2s, 4s, ...
///     let exponent = u32::try_from(attempt.unwrap_or(1).min(16)).unwrap_or(16);
///     HandlerOutcome::retry_after(Duration::from_secs(1 << (exponent - 1)))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Attempt;

impl ContextField for Attempt {
    type Context = InboxContext;
    type Value = Option<u64>;

    fn read(self, src: &InboxContext) -> Option<u64> {
        src.attempt
    }
}

/// The delivery's transaction: the handler's first `Ctx`, on a registration mounted with
/// `.transactional()`.
///
/// It reads [`TxContext`] and yields [`Tx`](crate::Tx), which goes back to the delivery when the
/// handler ends. A handler reads it once: a second read for one delivery panics, the transaction
/// lent already.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::{PgPool, Postgres};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Shipment { order: i64 }
///
/// #[subscriber(InboxQueue::<Job>::new("shipments"))]
/// async fn ship(shipment: &Shipment, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
///     let shipped = sqlx::query("UPDATE orders SET shipped = true WHERE id = $1")
///         .bind(shipment.order)
///         .execute(&mut *tx)
///         .await;
///     if shipped.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(ship.transactional());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct Tx<DB>(PhantomData<fn() -> DB>);

impl<DB> Default for Tx<DB> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<DB> Clone for Tx<DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB> Copy for Tx<DB> {}

impl<DB> fmt::Debug for Tx<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Tx")
    }
}

impl<DB: QueueDatabase> ContextField for Tx<DB> {
    type Context = TxContext<DB>;
    type Value = super::Tx<DB>;

    /// Lends the delivery's transaction: one lock, no allocation.
    ///
    /// # Panics
    ///
    /// When the transaction is lent already: a handler reads it once per delivery.
    fn read(self, src: &TxContext<DB>) -> super::Tx<DB> {
        super::Tx::lent_by(src.lender).expect(
            "the delivery's transaction is lent already: a handler reads `Ctx<keys::Tx<..>>` \
             once per delivery",
        )
    }
}

/// The pool the broker runs on: writes through it commit on their own, whatever the delivery's
/// outcome.
///
/// It reads [`PoolContext`], which every delivery of an `InboxQueue` subscription lends, or
/// [`TxContext`] after `Ctx<keys::Tx<..>>`. It yields a clone of the service's pool, one
/// reference-count increment, only for a handler that reads it. A pool's type names its
/// database, and so does the key: `keys::Pool<Postgres>`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::Postgres;
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Visit { page: String }
///
/// #[subscriber(InboxQueue::<Job>::new("visits"))]
/// async fn count(visit: &Visit, Ctx(pool): Ctx<keys::Pool<Postgres>>) -> HandlerOutcome {
///     let counted = sqlx::query("UPDATE pages SET visits = visits + 1 WHERE path = $1")
///         .bind(&visit.page)
///         .execute(&pool)
///         .await;
///     if counted.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
/// # }
/// # fn main() {}
/// ```
pub struct Pool<DB>(PhantomData<fn() -> DB>);

impl<DB> Default for Pool<DB> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<DB> Clone for Pool<DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB> Copy for Pool<DB> {}

impl<DB> fmt::Debug for Pool<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Pool")
    }
}

impl<DB: QueueDatabase> ContextField for Pool<DB> {
    type Context = PoolContext<DB>;
    type Value = DbPool<DB>;

    fn read(self, src: &PoolContext<DB>) -> DbPool<DB> {
        src.pool.clone()
    }
}

/// Reads the attempt off a context richer than the one [`Attempt`] names.
#[derive(Clone, Copy)]
struct AttemptOf;

impl<DB: QueueDatabase> Field<PoolContext<DB>> for AttemptOf {
    type Value<'a>
        = Option<u64>
    where
        DB: 'a;

    fn get(self, src: &PoolContext<DB>) -> Option<u64> {
        src.attempt
    }
}

impl<DB: QueueDatabase> Field<TxContext<DB>> for AttemptOf {
    type Value<'a>
        = Option<u64>
    where
        DB: 'a;

    fn get(self, src: &TxContext<DB>) -> Option<u64> {
        src.attempt
    }
}

/// Reads the pool off a transactional delivery's context.
#[derive(Clone, Copy)]
struct PoolOf;

impl<DB: QueueDatabase> Field<TxContext<DB>> for PoolOf {
    type Value<'a>
        = &'static DbPool<DB>
    where
        DB: 'a;

    fn get(self, src: &TxContext<DB>) -> &'static DbPool<DB> {
        src.pool
    }
}

// The reads of a later key from an earlier key's context. The framework's own impl reads a key
// from the one context the key names; these let the inbox's richer contexts serve the keys they
// carry as well, and none overlaps it: each names a context other than the key's own.

impl<S: Sync, DB: QueueDatabase> FromContext<PoolContext<DB>, S> for Ctx<Attempt> {
    type Rejection = Infallible;

    fn from_context(
        ctx: &mut Context<'_, PoolContext<DB>, S>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let attempt = ctx.context(AttemptOf);
        async move { Ok(Self(attempt)) }
    }
}

impl<S: Sync, DB: QueueDatabase> FromContext<TxContext<DB>, S> for Ctx<Attempt> {
    type Rejection = Infallible;

    fn from_context(
        ctx: &mut Context<'_, TxContext<DB>, S>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let attempt = ctx.context(AttemptOf);
        async move { Ok(Self(attempt)) }
    }
}

impl<S: Sync, DB: QueueDatabase> FromContext<TxContext<DB>, S> for Ctx<Pool<DB>> {
    type Rejection = Infallible;

    fn from_context(
        ctx: &mut Context<'_, TxContext<DB>, S>,
    ) -> impl Future<Output = Result<Self, Infallible>> + Send {
        let pool = ctx.context(PoolOf).clone();
        async move { Ok(Self(pool)) }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use ruststream::ContextField;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Error, SqlitePool};

    use super::{Tx, TxContext};
    use crate::inbox::transactional::TxBook;
    use crate::inbox::tx::PoolTx;

    #[tokio::test]
    #[should_panic(expected = "lent already")]
    async fn a_second_read_of_the_transaction_panics() {
        let pool: &'static SqlitePool = Box::leak(Box::new(
            SqlitePoolOptions::new()
                .connect("sqlite::memory:")
                .await
                .expect("an in-memory database opens"),
        ));
        let opened: Result<PoolTx<_>, Error> = PoolTx::begin(pool, None).await;
        let hold = TxBook::leak().enter(opened.expect("begins"));
        let cx = TxContext::new(hold.lender(), pool, Some(1));
        let _first = Tx::default().read(&cx);
        let _second = Tx::default().read(&cx);
    }
}
