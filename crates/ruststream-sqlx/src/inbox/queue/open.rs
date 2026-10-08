//! Opening a subscription: the statements it prepares and checks, the queue it interns for its
//! deliveries, the book its form holds rows in, and its place in the connection's register.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use ruststream::RetryDeclaration;
use ruststream_sqlx_dialect::{Dialect, Isolation, Opening, Role, TableSpec};
use sqlx::Database;

use super::build::{build, counted_attempt};
use super::check::check;
use super::description::{Description, Timing, refused_declaration, whole_seconds};
use crate::inbox::FormDialect;
use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::database::notify::listen;
use crate::inbox::engine::{Events, IdAt, Prepared, intern_name};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::form::advisory::LockBook;
use crate::inbox::form::lease::{self, LeaseBook};
use crate::inbox::form::row_lock::savepoint_of;
use crate::inbox::named::kinds::Kinds;
use crate::inbox::pools::ServicePool;
use crate::inbox::publish::table_of;
use crate::inbox::subscriber::{Holding, InboxSubscriber, Opened};
use crate::inbox::threads::InboxThreads;
use crate::inbox::transactional::{InboxMode, TxBook};

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
    /// Whether the database takes one writer at a time, so the subscription keeps one claim in
    /// flight: a second one would only wait for the first and for the settlements.
    pub one_writer: bool,
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

/// The subscription's handle on the service's pool, sizing its dedicated threads' pools. A
/// subscription mounted on threads of the crate's own counts their connections first: one that
/// would pass the connection limit refuses to start.
fn service_pool<DB: Database>(
    shared: &Shared<DB>,
    name: &str,
    threads: Option<&InboxThreads>,
) -> Result<&'static ServicePool<DB>, SqlxBrokerError> {
    if let Some(threads) = threads {
        shared.count_threads(name, threads)?;
    }
    let per_thread = threads.map_or(NonZeroU32::MIN, InboxThreads::connections_each);
    Ok(ServicePool::leak(
        shared.pool.clone(),
        shared.runtime.clone(),
        per_thread,
    ))
}

/// Opens a subscription to the queue `name` of the table `description` reads, its rows read as
/// `Row`, in `Mode`: builds its statements with the dialect `form` shows, checks them, and
/// registers the subscription so a second one is refused.
pub(crate) async fn open<DB, Row, Mode>(
    shared: &Arc<Shared<DB>>,
    form: &FormDialect,
    name: &str,
    timing: Timing,
    declaration: &RetryDeclaration,
    description: &Description,
) -> Result<InboxSubscriber<DB, Row, Mode>, SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Mode: InboxMode,
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
    // The claim's opening answered for the table's: what the row lock claim opens with, and what
    // transactional mode opens a delivery's own transaction with.
    let begin_work = form
        .dialect()
        .begin(description.spec.opening())
        .map_err(refused)?;
    if let Some(reason) = lease_unseen_at::<Mode>(form, description) {
        return Err(declared(reason));
    }
    let savepoint = savepoint_of::<Mode>(form);
    let prepared = Prepared {
        transactional: Mode::TRANSACTIONAL,
        begin_work,
        savepoint,
        ..prepared
    };
    let table_name = intern_name(&table);
    // A table without groups is one queue, whatever name the subscription gives it.
    let group = description.spec.column(Role::Group).map(|_| name);
    let registration = Registration::take(shared, table_name, group, name, row)?;
    check(shared, form, &description.spec, &prepared)
        .await
        .map_err(|unchecked| unchecked.named(name, &table, row))?;
    // Listened before the first claim, so a row announced after it still wakes the subscription.
    listen(shared, shared.wakes.table(&description.spec), name, row).await?;
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
        one_writer: one_writer::<DB>(form.dialect()),
        poll_interval: timing.poll_interval.unwrap_or(shared.poll_interval),
        lease: description
            .leased()
            .then(|| whole_seconds(timing.lease.unwrap_or(shared.lease))),
        cap: cap_of(declaration),
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
    // The subscription's own handle on the pool, which each delivery copies for the handler's
    // context instead of counting a reference.
    let pool = service_pool(shared, name, timing.threads.as_ref())?;
    // A transactional delivery's transaction waits in the book while its handler does not hold it;
    // in the advisory lock form the lock book keeps the session that holds it.
    let lending = (Mode::TRANSACTIONAL && !description.advisory()).then(TxBook::leak);
    // Per connection, not on the interned queue: a queue's description is shared by subscriptions
    // of other connections, to other databases, that this connection's publishes do not reach.
    let wake = shared.wakes.table(&description.spec).subscribe(name);
    Ok(InboxSubscriber::new(
        Arc::clone(shared),
        Opened {
            queue,
            holding,
            registration,
            keeper,
            pool,
            lending,
            wake,
        },
    ))
}

/// Why a subscription in `Mode` to the table `description` reads cannot run in transactional mode
/// at the table's isolation level on the dialect `form` shows, if it cannot.
///
/// A transactional lease delivery acknowledges inside its handler's transaction, by the lease the
/// broker last extended. A Postgres transaction at REPEATABLE READ or SERIALIZABLE reads every row
/// as its first statement found it, so it cannot see an extension committed later, and the
/// acknowledgement of a handler that outlived half its lease would find no row: a lease it lost
/// on paper, whose writes it would roll back.
// Why a startup refusal: the dialect is known by its name, and the backend behind an `AnyPool` only
// once the broker connected. MySQL and MariaDB read the latest row in an update at every level, so
// they keep the acknowledgement at theirs.
/// The retry cap `declaration` declares: both its attempts and its dead letter, or none.
fn cap_of(declaration: &RetryDeclaration) -> Option<Cap> {
    declaration
        .max_attempts()
        .zip(declaration.dead_letter())
        .map(|(attempts, destination)| Cap {
            attempts,
            dead_letter: intern_name(destination),
        })
}

/// Whether the database behind `DB` and `dialect` takes one writer at a time: SQLite, through a
/// pool of its own or an `AnyPool`. Its busy handler sleeps instead of queueing a writer, so claims
/// that run at once collide and wait out the sleeps.
fn one_writer<DB: Database>(dialect: &dyn Dialect) -> bool {
    DB::NAME == "SQLite" || dialect.name() == "sqlite"
}

fn lease_unseen_at<Mode: InboxMode>(
    form: &FormDialect,
    description: &Description,
) -> Option<String> {
    let Opening::Isolation(level @ (Isolation::RepeatableRead | Isolation::Serializable)) =
        description.spec.opening()
    else {
        return None;
    };
    if !Mode::TRANSACTIONAL || !description.leased() || form.dialect().name() != "postgres" {
        return None;
    }
    Some(format!(
        "transactional mode acknowledges a lease delivery by the lease the broker last extended, \
         and a Postgres transaction at `isolation = {}` reads rows as its first statement found \
         them, so a handler that outlives half its lease would lose its writes: declare \
         `isolation = read_committed` or no level, or mount the handler without \
         `.transactional()`",
        level.attribute(),
    ))
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
    use ruststream_sqlx_dialect as dialect;

    use super::one_writer;

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_takes_one_writer_at_a_time() {
        assert!(one_writer::<sqlx::Sqlite>(&dialect::Sqlite));
    }

    #[cfg(all(feature = "any", feature = "sqlite"))]
    #[test]
    fn an_any_pool_on_sqlite_takes_one_writer_at_a_time() {
        assert!(one_writer::<sqlx::Any>(&dialect::Sqlite));
    }

    #[cfg(all(feature = "any", feature = "postgres"))]
    #[test]
    fn a_server_takes_writers_at_once() {
        assert!(!one_writer::<sqlx::Postgres>(&dialect::Postgres));
        assert!(!one_writer::<sqlx::Any>(&dialect::Postgres));
    }
}
