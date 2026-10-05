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
use ruststream_sqlx_dialect::{ClaimShape, Dialect, Role, TableName, TableSpec};
#[cfg(feature = "asyncapi")]
use serde::Serialize;
use sqlx::{Database, Pool};

use super::broker::{ConnectedSqlxBroker, Shared};
use super::database::QueueDatabase;
use super::engine::{Events, IdAt, Prepared, Shape, Stmt, intern, intern_name};
use super::error::SqlxBrokerError;
use super::kinds::Kinds;
use super::publish::table_of;
use super::subscriber::InboxSubscriber;
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};
use super::{InboxRow, PayloadRow};

/// A subscription to a queue table: the rows of `Row` that the name addresses.
///
/// With a `group` field the name selects the group; without one the table is a single queue and
/// the name is its address. Rows are claimed with `FOR UPDATE SKIP LOCKED` in a transaction held
/// for the whole handler: acknowledgement is the delete (or the `processed_at` mark) and the
/// commit, a retry counts the attempt and commits, and after a crash the database rolls back and
/// the row returns at once.
///
/// `max_attempts(n)` and `dead_letter(..)` at the mount site map onto the table: at the cap the
/// row moves to the `dead_letter` group (with a `group` field) or into the `dead_letter` table
/// (one with the same columns), and without a destination it is deleted or marked.
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
}

impl<Row> Clone for InboxQueue<Row> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            poll_interval: self.poll_interval,
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
            .field("declaration", &self.declaration)
            .finish()
    }
}

impl<DB, Row> SubscriptionSource<ConnectedSqlxBroker<DB>> for InboxQueue<Row>
where
    DB: QueueDatabase,
    Row: InboxRow + Events<DB> + PayloadRow,
{
    type Subscriber = InboxSubscriber<DB, Row>;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedSqlxBroker<DB>,
    ) -> Result<Self::Subscriber, SqlxBrokerError> {
        open::<DB, Row>(
            &connected.shared,
            &self.name,
            self.poll_interval,
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
        _connected: &ConnectedSqlxBroker<DB>,
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
    /// How long the claim loop waits after a claim that found the queue short.
    pub poll_interval: Duration,
    /// The declared cap on attempts.
    pub max_attempts: Option<NonZeroU32>,
    /// The declared dead-letter destination.
    pub dead_letter: Option<&'static str>,
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

/// The statements a subscription to the table `description` reads runs, built by `dialect`.
fn build(
    dialect: &dyn Dialect,
    declaration: &RetryDeclaration,
    description: &Description,
    fail: &impl Fn(String) -> SqlxBrokerError,
) -> Result<Prepared, SqlxBrokerError> {
    let spec = description.spec;
    let shape = description.shape;
    let refused = |source| SqlxBrokerError::Dialect {
        subscription: String::new(),
        table: String::new(),
        row: "",
        source,
    };
    let claim = (!shape.custom_claim)
        .then(|| dialect.claim(&spec, description.claim))
        .transpose()
        .map_err(refused)?;
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
    let dead_letter = match declaration.dead_letter() {
        Some(_) if shape.custom_dead_letter => None,
        Some(_) if spec.column(Role::Group).is_some() => {
            Some(dialect.dead_letter_group(&spec).map_err(refused)?)
        }
        Some(target) => {
            let target = TableName::parse(target)
                .map_err(|err| fail(format!("the dead-letter table {err}")))?;
            let mut moves = dialect.dead_letter_table(&spec, target).map_err(refused)?;
            if moves.len() != 1 {
                return Err(fail(format!(
                    "the {} dialect moves a dead letter in {} statements, and the inbox runs one",
                    dialect.name(),
                    moves.len()
                )));
            }
            moves.pop()
        }
        None => None,
    };
    Ok(Prepared {
        claim: claim.as_ref().map(intern),
        fetch: fetch.as_ref().map(intern),
        ack: ack.as_ref().map(intern),
        retry: retry.as_ref().map(intern),
        retry_after: retry_after.as_ref().map(intern),
        discard: discard.as_ref().map(intern),
        dead_letter: dead_letter.as_ref().map(intern),
    })
}

/// Opens a subscription to the queue `name` of the table `description` reads, its rows read as
/// `Row`: builds and checks its statements, and registers it so a second one is refused.
pub(crate) async fn open<DB, Row>(
    shared: &Arc<Shared<DB>>,
    name: &str,
    poll_interval: Option<Duration>,
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
    let prepared = build(shared.dialect.get(), declaration, description, &declared).map_err(
        |err| match err {
            SqlxBrokerError::Dialect { source, .. } => SqlxBrokerError::Dialect {
                subscription: name.to_owned(),
                table: table.clone(),
                row,
                source,
            },
            other => other,
        },
    )?;
    let table_name = intern_name(&table);
    // A table without groups is one queue, whatever name the subscription gives it.
    let group = description.spec.column(Role::Group).map(|_| name);
    let registration = Registration::take(shared, table_name, group, name, row)?;
    check(shared, &prepared)
        .await
        .map_err(|unchecked| match unchecked {
            Unchecked::Acquire(source) => SqlxBrokerError::Sqlx {
                subscription: name.to_owned(),
                table: table.clone(),
                row,
                statement: "acquire",
                source: Box::new(source),
            },
            Unchecked::Statement(statement, source) => SqlxBrokerError::Schema {
                subscription: name.to_owned(),
                table: table.clone(),
                row,
                statement,
                source: Box::new(source),
            },
        })?;
    let queue = Queue {
        name: intern_name(name),
        table: table_name,
        row,
        spec: description.spec,
        id_at: description.id_at(),
        native_retry_after: description.native_retry_after(),
        kinds: description.kinds,
        prepared,
        poll_interval: poll_interval.unwrap_or(shared.poll_interval),
        max_attempts: declaration.max_attempts(),
        dead_letter: declaration.dead_letter().map(intern_name),
    }
    .intern();
    Ok(InboxSubscriber::new(
        Arc::clone(shared),
        queue,
        registration,
    ))
}

/// Why the startup check failed: no connection, or a statement the server refused.
enum Unchecked {
    Acquire(sqlx::Error),
    Statement(&'static str, sqlx::Error),
}

/// The startup check: prepares each of `prepared`'s statements on a connection of the pool, off a
/// paused clock where the connection runs in process.
async fn check<DB: QueueDatabase>(
    shared: &Shared<DB>,
    prepared: &Prepared,
) -> Result<(), Unchecked> {
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        let pool = shared.pool.clone();
        let statements: Vec<Stmt> = prepared.statements().collect();
        return off_clock(async move { prepare(&pool, statements.into_iter()).await })
            .await
            .unwrap_or_else(|| Err(Unchecked::Acquire(cancelled())));
    }
    prepare(&shared.pool, prepared.statements()).await
}

/// Prepares each of `statements` on a connection of `pool`.
async fn prepare<DB: QueueDatabase>(
    pool: &Pool<DB>,
    statements: impl Iterator<Item = Stmt>,
) -> Result<(), Unchecked> {
    let mut conn = pool.acquire().await.map_err(Unchecked::Acquire)?;
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
mod tests {
    use ruststream_sqlx_dialect::{ClaimShape, Column, Form, TableSpec};

    use super::Description;
    use crate::inbox::engine::{IdAt, Shape};

    const JOBS: TableSpec<'static> =
        TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("body"));

    fn described(spec: &TableSpec<'static>, shape: Shape, claim: ClaimShape) -> Description {
        Description {
            spec: *spec,
            shape,
            claim,
            row: "Job",
            kinds: None,
        }
    }

    #[test]
    fn a_delayed_retry_is_native_with_its_column_or_the_services_event() {
        let bare = described(&JOBS, Shape::default(), ClaimShape::Rows);
        assert!(!bare.native_retry_after());
        let column = described(
            &JOBS.retry_after(Column::new("retry_after")),
            Shape::default(),
            ClaimShape::Rows,
        );
        assert!(column.native_retry_after());
        let custom = Shape {
            custom_retry_after: true,
            ..Shape::default()
        };
        assert!(described(&JOBS, custom, ClaimShape::Rows).native_retry_after());
    }

    #[test]
    fn a_claim_by_role_carries_the_id_first_whatever_the_struct_flattens() {
        let flat = JOBS.selecting_all();
        assert_eq!(
            described(&flat, Shape::default(), ClaimShape::Rows).id_at(),
            IdAt::Named("job_id")
        );
        assert_eq!(
            described(&flat, Shape::default(), ClaimShape::Roles).id_at(),
            IdAt::First
        );
        assert_eq!(
            described(&JOBS, Shape::default(), ClaimShape::Rows).id_at(),
            IdAt::First
        );
    }
}
