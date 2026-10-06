//! `InboxQueue`: the subscription descriptor, and the startup work of opening one.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::{BrokerMoves, DeclareRetryError, RetryDeclaration, SubscriptionSource};
use ruststream_sqlx_dialect::{
    ClaimShape, Dialect, Form, Opens, Role, Statement, StatementError, TableName, TableSpec,
};
#[cfg(feature = "asyncapi")]
use serde::Serialize;
use sqlx::{Database, Pool};

use super::advisory::LockBook;
use super::broker::{ConnectedSqlxBroker, Shared};
use super::database::QueueDatabase;
use super::engine::{Events, IdAt, Prepared, Shape, Stmt, intern, intern_name};
use super::error::SqlxBrokerError;
use super::kinds::Kinds;
use super::lease::{self, LeaseBook};
use super::publish::table_of;
use super::subscriber::{Holding, InboxSubscriber};
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};
use super::time::LeaseRow;
use super::{FormDialect, FormOn, InboxRow, PayloadRow};

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
/// A table with a `#[field(locked_until)]` field is claimed by lease instead: the claim writes the
/// lease's expiry into the row, counts the attempt and commits at once, so the handler runs
/// outside any transaction. While the handler runs, the subscription extends the lease each half
/// lease. Each settlement is one statement that takes effect only while the row still holds the
/// lease, a delivery dropped unsettled releases its row at once, and after a crash the row
/// returns once the lease runs out. The lease is the broker's
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
pub struct InboxQueue<Row> {
    name: Cow<'static, str>,
    poll_interval: Option<Duration>,
    lease: Option<Duration>,
    declaration: RetryDeclaration,
    _row: PhantomData<fn() -> Row>,
}

impl<Row> InboxQueue<Row> {
    /// A subscription to the queue `name` of `Row`'s table.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream::{Connected, SubscriptionSource};
    /// use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
    /// # #[derive(Inbox, sqlx::FromRow)]
    /// # #[inbox(table = "jobs")]
    /// # pub struct Job { #[field(id)] id: i64, #[field(group)] name: String, #[field(payload)] payload: Vec<u8> }
    ///
    /// let reports = InboxQueue::<Job>::new("reports");
    /// assert_eq!(
    ///     SubscriptionSource::<Connected<SqlxBroker<sqlx::Postgres>>>::name(&reports),
    ///     "reports",
    /// );
    /// # }
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
    /// # #[cfg(feature = "postgres")] {
    /// use std::time::Duration;
    ///
    /// use ruststream_sqlx::{Inbox, InboxQueue};
    /// # #[derive(Inbox, sqlx::FromRow)]
    /// # #[inbox(table = "jobs")]
    /// # pub struct Job { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
    ///
    /// // An urgent queue looks again every 50 ms when it runs dry.
    /// let urgent = InboxQueue::<Job>::new("urgent").poll_interval(Duration::from_millis(50));
    /// # let _ = urgent;
    /// # }
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
}

impl<Row> Clone for InboxQueue<Row> {
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

impl<Row> fmt::Debug for InboxQueue<Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxQueue")
            .field("row", &type_name::<Row>())
            .field("name", &self.name)
            .field("poll_interval", &self.poll_interval)
            .field("lease", &self.lease)
            .field("declaration", &self.declaration)
            .finish()
    }
}

impl<DB, D, Row> SubscriptionSource<ConnectedSqlxBroker<DB, D>> for InboxQueue<Row>
where
    DB: QueueDatabase,
    D: Dialect + Opens<Row::Opening> + 'static,
    Row: InboxRow + Events<DB> + PayloadRow,
    Row::Form: FormOn<D>,
{
    type Subscriber = InboxSubscriber<DB, Row>;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedSqlxBroker<DB, D>,
    ) -> Result<Self::Subscriber, SqlxBrokerError> {
        open::<DB, Row>(
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

/// What a subscription names of its timing; the broker's where it names nothing.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Timing {
    /// How long the claim loop waits after a claim that found the queue short.
    pub(crate) poll_interval: Option<Duration>,
    /// How long a claim leases a row, in the lease form.
    pub(crate) lease: Option<Duration>,
}

/// `lease` in whole seconds, rounded up, and at least one.
fn whole_seconds(lease: Duration) -> Duration {
    let seconds = lease
        .as_secs()
        .saturating_add(u64::from(lease.subsec_nanos() > 0));
    Duration::from_secs(seconds.max(1))
}

/// What a subscription knows of its table when it opens: the description, the events the
/// service implements itself, how the claim selects, and the struct that reads the rows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Description {
    /// The table.
    pub(crate) spec: TableSpec<'static>,
    /// The events the service implements itself.
    pub(crate) shape: Shape,
    /// What the claim selects.
    pub(crate) claim: ClaimShape,
    /// The struct's type, for messages.
    pub(crate) row: &'static str,
    /// The kinds a by-name subscription reads the rows by, where the struct's types allow it.
    pub(crate) kinds: Option<Kinds>,
}

impl Description {
    /// The description `Row`'s derive gives: its table, its events, and whole rows to claim (ids
    /// alone where the service fetches the rows itself).
    pub(crate) fn of<DB, Row>() -> Self
    where
        DB: QueueDatabase,
        Row: InboxRow + Events<DB>,
    {
        let shape = Row::SHAPE;
        Self {
            spec: Row::SPEC,
            shape,
            claim: if shape.custom_fetch {
                ClaimShape::Ids
            } else {
                ClaimShape::Rows
            },
            row: type_name::<Row>(),
            kinds: Row::kinds(),
        }
    }

    /// The description a by-name subscription reads the table by: the crate's own events, and
    /// the columns that run the queue under their roles' names.
    pub(crate) fn by_role(self) -> Self {
        Self {
            shape: Shape::default(),
            claim: ClaimShape::Roles,
            ..self
        }
    }

    /// Whether a delayed retry is the database's own: the table holds `retry_after`, or the
    /// service implements the event.
    pub(crate) const fn native_retry_after(&self) -> bool {
        self.shape.custom_retry_after || self.spec.column(Role::RetryAfter).is_some()
    }

    /// Whether the table is claimed by lease: it has a `locked_until` column.
    pub(crate) const fn leased(&self) -> bool {
        self.spec.column(Role::LockedUntil).is_some()
    }

    /// Whether the table is claimed by advisory lock: its struct names a lock key.
    pub(crate) const fn advisory(&self) -> bool {
        matches!(self.spec.form(), Form::Advisory(_))
    }

    /// Where the claim's select carries the id of a row read alone.
    pub(crate) const fn id_at(&self) -> IdAt {
        match self.claim {
            // Role aliases list the id first, whatever the struct flattens.
            ClaimShape::Roles => IdAt::First,
            ClaimShape::Rows | ClaimShape::Ids => IdAt::of(&self.spec),
        }
    }
}

/// Why `declaration` cannot apply to the table `description` reads, if it cannot.
pub(crate) fn refused_declaration(
    name: &str,
    declaration: &RetryDeclaration,
    description: &Description,
) -> Option<SqlxBrokerError> {
    let spec = description.spec;
    let refuse = |reason: String| SqlxBrokerError::Declaration {
        subscription: name.to_owned(),
        table: table_of(&spec),
        row: description.row,
        reason,
    };
    // The table moves a spent row itself, so the cap and the destination come together: a half
    // would leave the row nowhere to go, or nothing to count before it goes.
    match (declaration.max_attempts(), declaration.dead_letter()) {
        (Some(_), None) => {
            return Some(refuse(
                "the registration declares `max_attempts(..)` without `dead_letter(..)`: name \
                 where a row whose attempts are spent goes"
                    .to_owned(),
            ));
        }
        (None, Some(_)) => {
            return Some(refuse(
                "the registration declares `dead_letter(..)` without `max_attempts(..)`: name the \
                 cap, `max_attempts(1)` to move a row at its first failure"
                    .to_owned(),
            ));
        }
        _ => {}
    }
    if declaration.max_attempts().is_some() && spec.column(Role::Attempt).is_none() {
        return Some(refuse(
            "`max_attempts(..)` counts deliveries in the `attempt` column: add \
             `#[field(attempt)]` to the struct"
                .to_owned(),
        ));
    }
    match declaration.dead_letter() {
        Some(target) if spec.column(Role::Group).is_none() => TableName::parse(target)
            .err()
            .map(|err| refuse(format!("the dead-letter table {err}"))),
        _ => None,
    }
}

/// One open subscription, interned for the life of the process: what its deliveries read to settle
/// and to name themselves. Machinery; statements bind its name, a service never names it.
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Queue {
    /// The subscription's name: its group, or the table's address.
    pub name: &'static str,
    /// The table, qualified with its schema, for messages.
    pub table: &'static str,
    /// The struct that describes the table, for messages.
    pub row: &'static str,
    /// The table's description.
    pub spec: TableSpec<'static>,
    /// Where the claim's select carries the id of a row read alone.
    pub id_at: IdAt,
    /// Whether a delayed retry is the database's own.
    pub native_retry_after: bool,
    /// The kinds the statements of a by-name subscription bind by.
    pub kinds: Option<Kinds>,
    /// The subscription's statements.
    pub prepared: Prepared,
    /// The statement that opens a claim's transaction in place of `BEGIN`, where the dialect
    /// names one: the row lock claim's at the table's opening.
    pub begin_claim: Option<&'static str>,
    /// Whether the rows a claim hands out carry the attempt it counted, so a delivery reports one
    /// less.
    pub counted_attempt: bool,
    /// How long the claim loop waits after a claim that found the queue short.
    pub poll_interval: Duration,
    /// How long a claim leases a row, in whole seconds; `None` outside the lease form.
    pub lease: Option<Duration>,
    /// The declared cap on attempts and where a spent row goes, declared together.
    pub cap: Option<Cap>,
}

/// A registration's cap on a row's attempts and the destination of a row that spent them, which
/// it declares together. Machinery.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cap {
    /// How many deliveries a row gets.
    pub attempts: NonZeroU32,
    /// The group or the table a spent row moves to.
    pub dead_letter: &'static str,
}

static QUEUES: LazyLock<Mutex<Vec<&'static Queue>>> = LazyLock::new(Mutex::default);

impl Queue {
    /// `self` for the life of the process: a delivery reaches its subscription through a
    /// `'static` reference, with no reference count per message.
    fn intern(self) -> &'static Self {
        let mut queues = QUEUES.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(found) = queues.iter().find(|queue| ***queue == self) {
            return found;
        }
        let queue: &'static Self = Box::leak(Box::new(self));
        queues.push(queue);
        queue
    }
}

/// The statements a subscription to the table `description` reads runs, built by the dialect
/// `form` shows: the claim and the statements of the lease and advisory lock forms by the trait of
/// the table's form, the guard of a FIFO group and the settlements by the dialect itself.
fn build(
    form: &FormDialect,
    declaration: &RetryDeclaration,
    description: &Description,
    fail: &impl Fn(String) -> SqlxBrokerError,
) -> Result<Prepared, SqlxBrokerError> {
    let dialect = form.dialect();
    let spec = description.spec;
    let shape = description.shape;
    let refused = |source| SqlxBrokerError::Dialect {
        subscription: String::new(),
        table: String::new(),
        row: "",
        source,
    };
    let claim = (!shape.custom_claim)
        .then(|| form.claim(&spec, description.claim))
        .transpose()
        .map_err(refused)?;
    // Whoever writes the claim: a claim of the service's own takes its rows in the transaction the
    // guard took the group in, as the crate's does.
    let fifo_guard = dialect.fifo_guard(&spec).map_err(refused)?;
    let fetch = (shape.custom_claim && !shape.custom_fetch)
        .then(|| dialect.fetch(&spec))
        .transpose()
        .map_err(refused)?;
    let ack = (!shape.custom_ack)
        .then(|| dialect.ack(&spec))
        .transpose()
        .map_err(refused)?;
    let retry = if shape.custom_retry {
        None
    } else {
        dialect.retry(&spec).map_err(refused)?
    };
    let retry_after = (!shape.custom_retry_after && spec.column(Role::RetryAfter).is_some())
        .then(|| dialect.retry_after(&spec))
        .transpose()
        .map_err(refused)?;
    let discard = (!shape.custom_discard)
        .then(|| dialect.discard(&spec))
        .transpose()
        .map_err(refused)?;
    let (dead_letter, dead_letter_then) = match declaration.dead_letter() {
        Some(_) if shape.custom_dead_letter => (None, None),
        Some(_) if spec.column(Role::Group).is_some() => (
            Some(dialect.dead_letter_group(&spec).map_err(refused)?),
            None,
        ),
        Some(target) => {
            let target = TableName::parse(target)
                .map_err(|err| fail(format!("the dead-letter table {err}")))?;
            let moves = dialect.dead_letter_table(&spec, target).map_err(refused)?;
            let (first, then) = one_or_two(dialect, "moves a dead letter", moves, fail)?;
            (Some(first), then)
        }
        None => (None, None),
    };
    // The advisory lock form's own statements: the lock and the unlock where the database keeps
    // the locks, and the take of a candidate whose lock the delivery's session holds.
    let (lock, unlock, take, take_then) = match form.advisory() {
        Some(advisory) => {
            let takes = advisory.take(&spec, description.claim).map_err(refused)?;
            let (take, then) = one_or_two(dialect, "takes a candidate", takes, fail)?;
            (advisory.lock(), advisory.unlock(), Some(take), then)
        }
        None => (None, None, None, None),
    };
    // Why a startup refusal: the derive gives a table that declares a lease the lease form's
    // type, so only a description written by hand pairs one with another form's dialect.
    let lease = match (description.leased(), form.lease()) {
        (false, _) => None,
        (true, Some(lease)) => Some(lease),
        (true, None) => {
            return Err(refused(StatementError::UnsupportedForm {
                dialect: dialect.name(),
                form: spec.form().name(),
            }));
        }
    };
    // A claim of the service's own, or one the dialect only selects with, leaves each row to a
    // stamp of the crate's inside the claim's transaction.
    let stamps = lease.is_some_and(|lease| shape.custom_claim || !lease.claim_writes_lease());
    let extend = lease
        .filter(|_| !shape.custom_extend)
        .map(|lease| lease.extend(&spec))
        .transpose()
        .map_err(refused)?;
    let stamp = lease
        .filter(|_| stamps)
        .map(|lease| lease.stamp(&spec))
        .transpose()
        .map_err(refused)?;
    Ok(Prepared {
        fifo_guard: fifo_guard.as_ref().map(intern),
        claim: claim.as_ref().map(intern),
        fetch: fetch.as_ref().map(intern),
        ack: ack.as_ref().map(intern),
        retry: retry.as_ref().map(intern),
        retry_after: retry_after.as_ref().map(intern),
        discard: discard.as_ref().map(intern),
        dead_letter: dead_letter.as_ref().map(intern),
        dead_letter_then: dead_letter_then.as_ref().map(intern),
        extend: extend.as_ref().map(intern),
        stamp: stamp.as_ref().map(intern),
        lock: lock.as_ref().map(intern),
        unlock: unlock.as_ref().map(intern),
        take: take.as_ref().map(intern),
        take_then: take_then.as_ref().map(intern),
        stamps,
    })
}

/// The one or two statements `dialect` builds to do `what`, the second run after the first.
///
/// # Errors
///
/// `fail`'s error where the dialect builds none or more than two.
fn one_or_two(
    dialect: &dyn Dialect,
    what: &str,
    statements: Vec<Statement>,
    fail: &impl Fn(String) -> SqlxBrokerError,
) -> Result<(Statement, Option<Statement>), SqlxBrokerError> {
    let count = statements.len();
    let mut statements = statements.into_iter();
    match (statements.next(), statements.next(), statements.next()) {
        (Some(first), then, None) => Ok((first, then)),
        _ => Err(fail(format!(
            "the {} dialect {what} in {count} statements, and the inbox runs one or two",
            dialect.name(),
        ))),
    }
}

/// Opens a subscription to the queue `name` of the table `description` reads, its rows read as
/// `Row`: builds its statements with the dialect `form` shows, checks them, and registers the
/// subscription so a second one is refused.
pub(crate) async fn open<DB, Row>(
    shared: &Arc<Shared<DB>>,
    form: &FormDialect,
    name: &str,
    timing: Timing,
    declaration: &RetryDeclaration,
    description: &Description,
) -> Result<InboxSubscriber<DB, Row>, SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    let table = table_of(&description.spec);
    let row = description.row;
    if shared.is_closed() {
        return Err(SqlxBrokerError::Closed);
    }
    let declared = |reason: String| SqlxBrokerError::Declaration {
        subscription: name.to_owned(),
        table: table.clone(),
        row,
        reason,
    };
    if let Some(refused) = refused_declaration(name, declaration, description) {
        return Err(refused);
    }
    let refused = |source| SqlxBrokerError::Dialect {
        subscription: name.to_owned(),
        table: table.clone(),
        row,
        source,
    };
    let prepared = build(form, declaration, description, &declared).map_err(|err| match err {
        SqlxBrokerError::Dialect { source, .. } => refused(source),
        other => other,
    })?;
    let begin_claim = form.begin_claim(&description.spec).map_err(refused)?;
    let table_name = intern_name(&table);
    // A table without groups is one queue, whatever name the subscription gives it.
    let group = description.spec.column(Role::Group).map(|_| name);
    let registration = Registration::take(shared, table_name, group, name, row)?;
    check(shared, form, &description.spec, &prepared)
        .await
        .map_err(|unchecked| unchecked.named(name, &table, row))?;
    let queue = Queue {
        name: intern_name(name),
        table: table_name,
        row,
        spec: description.spec,
        id_at: description.id_at(),
        native_retry_after: description.native_retry_after(),
        kinds: description.kinds,
        prepared,
        begin_claim,
        counted_attempt: counted_attempt(form, description, &prepared),
        poll_interval: timing.poll_interval.unwrap_or(shared.poll_interval),
        lease: description
            .leased()
            .then(|| whole_seconds(timing.lease.unwrap_or(shared.lease))),
        cap: declaration
            .max_attempts()
            .zip(declaration.dead_letter())
            .map(|(attempts, destination)| Cap {
                attempts,
                dead_letter: intern_name(destination),
            }),
    }
    .intern();
    // A book per subscription, not per queue: a queue's description is shared by every
    // subscription that reads it alike, its deliveries are not.
    let (holding, keeper) = match queue.lease {
        Some(_) => {
            let book = LeaseBook::leak(shared, queue);
            // A child of the connection's token: `shutdown` stops every keeper, and the
            // subscriber stops its own when it drops.
            let stop = shared.stopping.child_token();
            drop(shared.runtime.spawn(lease::keep(book, stop.clone())));
            (Holding::Leases(book), Some(stop.drop_guard()))
        }
        // The connection keeps the book, so `shutdown` reaches the locks of its deliveries.
        None if description.advisory() => (
            Holding::Advisory(LockBook::leak::<Row>(shared, queue)),
            None,
        ),
        None => (Holding::Transaction, None),
    };
    Ok(InboxSubscriber::new(
        Arc::clone(shared),
        queue,
        holding,
        registration,
        keeper,
    ))
}

/// Whether the rows a subscription to `description` hands out carry the attempt its claim or its
/// take counted, so that a delivery reports one less.
///
/// A lease claim that stamps its rows reads them before the stamps count them. A lease claim that
/// writes the lease itself counts and commits first: the service's own fetch after the crate's
/// claim of ids then reads counted rows, and whole rows come back counted where the dialect says
/// so. A claim by role reads the attempt as it was before the count. The take of the advisory lock
/// form reads the columns it names as they were before its count; `*` names none, and the
/// service's own fetch reads the row after the take committed its count.
fn counted_attempt(form: &FormDialect, description: &Description, prepared: &Prepared) -> bool {
    if description.advisory() {
        let counted = match description.claim {
            ClaimShape::Rows => description.spec.selects_all(),
            ClaimShape::Ids => true,
            ClaimShape::Roles => false,
        };
        return counted && description.spec.column(Role::Attempt).is_some();
    }
    let Some(lease) = form.lease().filter(|_| description.leased()) else {
        return false;
    };
    if prepared.stamps {
        return false;
    }
    match description.claim {
        ClaimShape::Rows => lease.claim_counts_attempt(&description.spec),
        ClaimShape::Ids => true,
        ClaimShape::Roles => false,
    }
}

/// Why the startup check failed: no connection, a version the server did not report, a server
/// the dialect refuses, or a statement the server refused.
enum Unchecked {
    Acquire(sqlx::Error),
    Version(&'static str, sqlx::Error),
    Server(StatementError),
    Statement(&'static str, sqlx::Error),
}

impl Unchecked {
    /// The error of the subscription `name` to `table`, read as `row`.
    fn named(self, name: &str, table: &str, row: &'static str) -> SqlxBrokerError {
        let (subscription, table) = (name.to_owned(), table.to_owned());
        match self {
            Self::Acquire(source) => SqlxBrokerError::Sqlx {
                subscription,
                table,
                row,
                statement: "acquire",
                source: Box::new(source),
            },
            Self::Version(statement, source) => SqlxBrokerError::Sqlx {
                subscription,
                table,
                row,
                statement,
                source: Box::new(source),
            },
            Self::Server(StatementError::ServerTooOld {
                server, required, ..
            }) => SqlxBrokerError::ServerTooOld {
                subscription,
                table,
                row,
                server,
                required,
            },
            Self::Server(source) => SqlxBrokerError::Dialect {
                subscription,
                table,
                row,
                source,
            },
            Self::Statement(statement, source) => SqlxBrokerError::Schema {
                subscription,
                table,
                row,
                statement,
                source: Box::new(source),
            },
        }
    }
}

/// The startup check: on one connection of the pool, the server's version where the dialect `form`
/// shows asks for it, then each of `prepared`'s statements prepared, off a paused clock where the
/// connection runs in process.
async fn check<DB: QueueDatabase>(
    shared: &Arc<Shared<DB>>,
    form: &FormDialect,
    spec: &TableSpec<'static>,
    prepared: &Prepared,
) -> Result<(), Unchecked> {
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        let shared = Arc::clone(shared);
        let form = form.clone();
        let spec = *spec;
        let statements: Vec<Stmt> = prepared.statements().collect();
        return off_clock(async move {
            verify(&shared.pool, form.dialect(), &spec, statements.into_iter()).await
        })
        .await
        .unwrap_or_else(|| Err(Unchecked::Acquire(cancelled())));
    }
    verify(&shared.pool, form.dialect(), spec, prepared.statements()).await
}

/// Checks the server's version against `dialect`'s floor for `spec`, where the dialect has one,
/// then prepares each of `statements`, on one connection of `pool`.
async fn verify<DB: QueueDatabase>(
    pool: &Pool<DB>,
    dialect: &dyn Dialect,
    spec: &TableSpec<'_>,
    statements: impl Iterator<Item = Stmt>,
) -> Result<(), Unchecked> {
    let mut conn = pool.acquire().await.map_err(Unchecked::Acquire)?;
    if let Some(query) = dialect.server_version() {
        let version = DB::fetch_text(&mut conn, query)
            .await
            .map_err(|source| Unchecked::Version(query, source))?;
        dialect
            .check_server(spec, &version)
            .map_err(Unchecked::Server)?;
    }
    for statement in statements {
        DB::prepare(&mut conn, statement.sql)
            .await
            .map_err(|source| Unchecked::Statement(statement.sql, source))?;
    }
    Ok(())
}

/// A queue's place in its connection's register of open subscriptions; dropping it frees the
/// place.
pub(crate) struct Registration<DB: Database> {
    shared: Arc<Shared<DB>>,
    table: &'static str,
    /// The group the subscription reads; `None` for a table without groups.
    group: Option<String>,
    name: String,
}

impl<DB: Database> Registration<DB> {
    fn take(
        shared: &Arc<Shared<DB>>,
        table: &'static str,
        group: Option<&str>,
        name: &str,
        row: &'static str,
    ) -> Result<Self, SqlxBrokerError> {
        let mut queues = shared.queues.lock().unwrap_or_else(PoisonError::into_inner);
        if queues
            .iter()
            .any(|(open, open_group)| *open == table && open_group.as_deref() == group)
        {
            return Err(SqlxBrokerError::AlreadySubscribed {
                subscription: name.to_owned(),
                table: table.to_owned(),
                row,
            });
        }
        queues.push((table, group.map(str::to_owned)));
        drop(queues);
        #[cfg(feature = "testing")]
        shared.harness.opened(name);
        Ok(Self {
            shared: Arc::clone(shared),
            table,
            group: group.map(str::to_owned),
            name: name.to_owned(),
        })
    }
}

impl<DB: Database> Drop for Registration<DB> {
    fn drop(&mut self) {
        let mut queues = self
            .shared
            .queues
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        queues.retain(|(table, group)| !(*table == self.table && *group == self.group));
        drop(queues);
        #[cfg(feature = "testing")]
        self.shared.harness.closed(&self.name);
    }
}

impl<DB: Database> fmt::Debug for Registration<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("table", &self.table)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
