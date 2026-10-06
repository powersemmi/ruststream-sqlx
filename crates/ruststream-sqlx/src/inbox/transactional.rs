//! Transactional mode: a handler that writes through its delivery's transaction, which
//! acknowledgement commits; the step that switches a registration to it, the transaction the
//! handler borrows, and the book a subscription lends its deliveries' transactions from.

use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

use super::queue::InboxQueue;

mod book;
pub(crate) mod settle;
mod tx;

pub(crate) use book::{Returned, TxBook, TxHold};
pub(crate) use tx::Lender;
pub use tx::Tx;

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
    /// Each form keeps the transaction its own way. The row lock form lends the claim's
    /// transaction: the subscription sets a savepoint right after each claim, one more statement
    /// per delivery, so a retry discards the handler's writes and still counts the attempt. The
    /// lease form opens a transaction of the delivery's own once the claim committed, one more
    /// statement per delivery: the acknowledgement runs inside it by the lease the delivery holds,
    /// and a delivery whose lease was lost meanwhile rolls everything back and fails with
    /// [`SqlxBrokerError::LeaseLost`](crate::SqlxBrokerError::LeaseLost). The advisory lock form
    /// opens the transaction on the session that holds the row's key, one more statement per
    /// delivery, and ends it before the key's release. Every transaction opens at the table's
    /// isolation level or SQLite mode. A lease table on Postgres at `isolation = repeatable_read`
    /// or `serializable` refuses transactional mode when it starts: its transaction cannot see the
    /// lease extended after its first statement.
    ///
    /// Each delivery in work holds one pool connection, so a handler that takes another connection
    /// of the same pool needs a pool larger than its `workers(n)`. On SQLite a delivery's
    /// transaction holds the database's one write lock from its first write, or from its start in
    /// `immediate` mode, until it settles: every other writer waits for it. Transactional mode
    /// serves single deliveries: a batch handler mounted with it does not compile.
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
