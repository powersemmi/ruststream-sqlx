//! `InboxQueue`: the subscription descriptor, and the startup work of opening one.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::{BrokerMoves, DeclareRetryError, RetryDeclaration, SubscriptionSource};
#[cfg(feature = "asyncapi")]
use ruststream_sqlx_dialect::Role;
use ruststream_sqlx_dialect::{Dialect, Opens};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use super::broker::ConnectedSqlxBroker;
use super::database::QueueDatabase;
use super::engine::Events;
use super::error::SqlxBrokerError;
#[cfg(feature = "asyncapi")]
use super::publish::table_of;
use super::subscriber::InboxSubscriber;
use super::time::LeaseRow;
use super::transactional::{InboxMode, Plain, Transactional};
use super::{FormOn, InboxRow};

mod check;
mod description;
mod open;
#[cfg(test)]
mod tests;

pub(crate) use description::{Description, Timing, refused_declaration};
pub use open::Queue;
pub(crate) use open::{Registration, open};

/// A subscription to a queue table: the rows of `Row` that the name addresses.
///
/// With a `group` field the name selects the group; without one the table is a single queue and
/// the name is its address. By default rows are claimed with `FOR UPDATE SKIP LOCKED` in a
/// transaction held for the whole handler: acknowledgement is the delete (or the `processed_at`
/// mark) and the commit, a retry counts the attempt and commits, and after a crash the database
/// rolls back and the row returns at once. That transaction opens at the table's `isolation`
/// where the struct declares one, and a subscription to a table at a level the broker's dialect
/// does not open ([`Opens`](crate::dialect::Opens)) does not compile.
///
/// Mounted with [`transactional`](crate::InboxSettings::transactional), a subscription lends its
/// handler a transaction to write through, in every form: acknowledgement commits the handler's
/// writes with the row's settlement, and every other outcome discards them first. A row lock
/// subscription lends the claim's transaction.
///
/// A table with a `#[field(locked_until)]` field is claimed by lease instead: the claim writes the
/// lease's expiry into the row, counts the attempt and commits at once, so the lease, not a
/// transaction, holds the row while the handler runs. While the handler runs, the subscription
/// extends the lease each half lease. Each settlement is one statement that takes effect only
/// while the row still holds the lease, a delivery dropped unsettled releases its row at once,
/// and after a crash the row returns once the lease runs out. The lease is the broker's
/// ([`SqlxBroker::lease`](crate::SqlxBroker::lease)) unless the subscription sets its own
/// ([`lease`](Self::lease)). The broker's dialect serves a table's form when it implements the
/// form's trait: [`RowLock`](crate::dialect::RowLock) for the row lock form,
/// [`Lease`](crate::dialect::Lease) for the lease form, [`Advisory`](crate::dialect::Advisory) for
/// the advisory lock form. SQLite has no row locks, so a table there takes the lease form or the
/// advisory lock form, and a subscription to one without `locked_until` or `advisory_lock` does not
/// compile.
///
/// `max_attempts(n)` and `dead_letter(..)` at the mount site map onto the table, declared together:
/// at the cap the row moves to the `dead_letter` group (with a `group` field) or into the
/// `dead_letter` table (one with the same columns). With `max_attempts(1)` every failure moves the
/// row at once. A registration that declares one without the other stops at startup.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(retry_after)]
///     retry_after: DateTime<Utc>,
///     #[field(attempt)]
///     attempt: i16,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // Five attempts, then the row moves to the `emails.failed` group.
///         b.include(send).max_attempts(nonzero!(5u32)).dead_letter("emails.failed");
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxQueue<Row, Mode = Plain> {
    name: Cow<'static, str>,
    poll_interval: Option<Duration>,
    lease: Option<Duration>,
    declaration: RetryDeclaration,
    _row: PhantomData<fn() -> (Row, Mode)>,
}

impl<Row> InboxQueue<Row> {
    /// A subscription to the queue `name` of `Row`'s table, in the plain mode: the handler leaves
    /// the delivery's transaction alone. The mount-site step
    /// [`transactional`](crate::InboxSettings::transactional) switches it to transactional mode.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::PgPool;
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "email_jobs")]
    /// pub struct SendEmail {
    ///     #[field(id, generated)]
    ///     job_id: i64,
    ///     #[field(group)]
    ///     name: String,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Email {
    ///     to: String,
    /// }
    ///
    /// // `emails` selects the rows of group `emails`; a table without a group column would deliver all
    /// // its rows here.
    /// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    /// async fn send(email: &Email) -> HandlerOutcome {
    ///     tracing::info!(to = %email.to, "sending");
    ///     HandlerOutcome::ack()
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
    #[must_use]
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            poll_interval: None,
            lease: None,
            declaration: RetryDeclaration::new(),
            _row: PhantomData,
        }
    }

    /// How long this subscription waits between claims that found the queue empty; the broker's
    /// interval unless set. After a full claim the next one runs at once.
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
    /// #[inbox(table = "alert_jobs")]
    /// pub struct Alert {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Page {
    ///     on_call: String,
    /// }
    ///
    /// // An urgent queue looks again every 50 ms when it runs dry; the broker's other queues keep theirs.
    /// #[subscriber(InboxQueue::<Alert>::new("alerts").poll_interval(Duration::from_millis(50)))]
    /// async fn notify(page: &Page) -> HandlerOutcome {
    ///     tracing::warn!(on_call = %page.on_call, "paging");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     RustStream::new(AppInfo::new("alerts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
    ///         b.include(notify);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = Some(interval);
        self
    }

    /// How long a claim of this subscription leases each row; the broker's lease
    /// ([`SqlxBroker::lease`](crate::SqlxBroker::lease)) unless set.
    ///
    /// A lease is whole seconds, at least one: a shorter one is rounded up. The subscription
    /// extends the lease of every delivery in work each half lease, so a handler may outlast it;
    /// the lease is how long a row stays out of the queue after its process crashed. A lease runs
    /// out under a running handler only when its extensions fail or stop (the database out of
    /// reach, the subscription closed, the broker shut down), and the late settlement then fails
    /// with [`SqlxBrokerError::LeaseLost`]. Only a table with a `#[field(locked_until)]` field
    /// takes a lease; on any other the call does not compile.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream::prelude::*;
    /// use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
    /// use serde::Deserialize;
    /// use sqlx::PgPool;
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "report_jobs")]
    /// pub struct Report {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(locked_until)]
    ///     locked_until: Option<DateTime<Utc>>,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Request {
    ///     month: u32,
    /// }
    ///
    /// // The report of a renderer that crashed waits out a two-minute lease before another
    /// // process takes it.
    /// #[subscriber(InboxQueue::<Report>::new("reports").lease(Duration::from_secs(120)))]
    /// async fn render(request: &Request) -> HandlerOutcome {
    ///     tracing::info!(month = request.month, "rendering");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
    ///         b.include(render);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn lease(mut self, lease: Duration) -> Self
    where
        Row: LeaseRow,
    {
        self.lease = Some(lease);
        self
    }

    /// The same subscription in transactional mode: what the mount-site step
    /// [`transactional`](crate::InboxSettings::transactional) makes of it.
    pub(crate) fn into_transactional(self) -> InboxQueue<Row, Transactional> {
        InboxQueue {
            name: self.name,
            poll_interval: self.poll_interval,
            lease: self.lease,
            declaration: self.declaration,
            _row: PhantomData,
        }
    }
}

impl<Row, Mode> Clone for InboxQueue<Row, Mode> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            poll_interval: self.poll_interval,
            lease: self.lease,
            declaration: self.declaration.clone(),
            _row: PhantomData,
        }
    }
}

impl<Row, Mode: InboxMode> fmt::Debug for InboxQueue<Row, Mode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxQueue")
            .field("row", &type_name::<Row>())
            .field("transactional", &Mode::TRANSACTIONAL)
            .field("name", &self.name)
            .field("poll_interval", &self.poll_interval)
            .field("lease", &self.lease)
            .field("declaration", &self.declaration)
            .finish()
    }
}

impl<DB, D, Row, Mode> SubscriptionSource<ConnectedSqlxBroker<DB, D>> for InboxQueue<Row, Mode>
where
    DB: QueueDatabase,
    D: Dialect + Opens<Row::Opening> + 'static,
    Row: InboxRow + Events<DB>,
    Row::Form: FormOn<D>,
    Mode: InboxMode,
{
    type Subscriber = InboxSubscriber<DB, Row, Mode>;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedSqlxBroker<DB, D>,
    ) -> Result<Self::Subscriber, SqlxBrokerError> {
        open::<DB, Row, Mode>(
            &connected.shared,
            &<Row::Form as FormOn<D>>::erase(&connected.dialect),
            &self.name,
            Timing {
                poll_interval: self.poll_interval,
                lease: self.lease,
            },
            &self.declaration,
            &Description::of::<DB, Row>(),
        )
        .await
    }

    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.declaration = declaration.clone();
        self
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        let group = Row::SPEC.column(Role::Group).map(|_| self.name.as_ref());
        let table = TableBinding {
            table: table_of(&Row::SPEC),
            group,
        };
        // A binding that fails to build is a binding the document goes without.
        Binding::extension("x-sqlx", &table)
            .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
    }

    fn declare_retry_on(
        &self,
        _connected: &ConnectedSqlxBroker<DB, D>,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        // Why a startup check rather than a bound: the core's `max_attempts(..)` and
        // `dead_letter(..)` steps do not consult the descriptor's type, so the table's roles can
        // only answer once the declaration reaches it.
        refused_declaration(&self.name, declaration, &Description::of::<DB, Row>())
            .map_or(Ok(()), |error| {
                Err(DeclareRetryError::Broker(Box::new(error)))
            })
    }
}

/// What a subscription adds to the `AsyncAPI` document: the table it reads, and the group its name
/// selects where the table has one.
#[cfg(feature = "asyncapi")]
#[derive(Serialize)]
struct TableBinding<'a> {
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<&'a str>,
}
