//! `SqlxBroker` and its lifecycle: the configuration, the connected form, the closed witness.

use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::join_all;
use ruststream::{Broker, ConnectedBroker};
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
use ruststream::{DescribeServer, ServerSpec};
use ruststream_sqlx_dialect::{Dialect, Opens};
#[cfg(feature = "any")]
use sqlx::Any;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "any"))]
use sqlx::ConnectOptions;
#[cfg(feature = "mysql")]
use sqlx::MySql;
#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;
use sqlx::{Database, Pool};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use super::advisory::LockBook;
use super::built_in::BuiltIn;
use super::database::{BuiltInDialect, QueueDatabase};
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::events::Publish;
use super::publish::Routes;
use super::session::Closing;
use super::{FormDialect, FormOn, PayloadRow};

/// How long a subscription waits between claims that found its queue empty, unless it names
/// another interval.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How long a claim leases a row of a table in the lease form, unless the subscription names
/// another lease.
const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// The dialect a broker will build statements with: one built into the crate, picked from the
/// connection `connect` checks, or the service's own.
pub(crate) enum DialectChoice<DB: Database, D> {
    Picked(fn(&DB::Connection) -> Result<D, SqlxBrokerError>),
    Given(Arc<D>),
}

impl<DB: Database, D: Dialect> DialectChoice<DB, D> {
    /// The dialect of the database `conn` reaches.
    pub(crate) fn resolve(&self, conn: &DB::Connection) -> Result<Arc<D>, SqlxBrokerError> {
        match self {
            Self::Picked(pick) => pick(conn).map(Arc::new),
            Self::Given(dialect) => Ok(Arc::clone(dialect)),
        }
    }

    /// The dialect's name, once there is one to name.
    fn name(&self) -> &'static str {
        match self {
            Self::Picked(_) => "built in, picked at connect",
            Self::Given(dialect) => dialect.name(),
        }
    }
}

impl<DB: Database, D> Clone for DialectChoice<DB, D> {
    fn clone(&self) -> Self {
        match self {
            Self::Picked(pick) => Self::Picked(*pick),
            Self::Given(dialect) => Self::Given(Arc::clone(dialect)),
        }
    }
}

/// The built-in dialect of the database `conn` reaches, or the error that names a database no
/// built-in dialect serves.
fn built_in<DB: BuiltInDialect>(conn: &DB::Connection) -> Result<BuiltIn<DB>, SqlxBrokerError> {
    DB::dialect(conn).ok_or_else(|| SqlxBrokerError::Backend {
        backend: DB::backend(conn).to_owned(),
    })
}

/// How a route reaches its table's form on the dialect the broker connects with.
pub(crate) type FormOf<D> = fn(&Arc<D>) -> FormDialect;

/// The inbox broker: task queues in the service's own tables, served through a sqlx pool.
///
/// [`new`](Self::new) records the pool and does no I/O; the pool belongs to the service, and the
/// broker never closes it. [`connect`](Broker::connect) takes one connection to check the
/// database, and picks the built-in dialect by it where the service passed none. A message in
/// work holds one of the pool's connections until it settles (in the lease form outside
/// transactional mode only while it settles), and a publish takes another, so the pool is sized
/// for both.
/// Subscriptions read tables through [`InboxQueue`](crate::InboxQueue) descriptors; publishing
/// writes them through [`Repository`](crate::Repository) policies or through the routes this
/// builder records.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, InboxQueue, Publish, SqlxBroker};
/// use serde::Deserialize;
/// use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
/// use sqlx::{PgConnection, Postgres};
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
/// impl Publish<Postgres> for SendEmail {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
///             .bind(message.name())
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
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
/// #[ruststream::app]
/// fn app() -> impl App {
///     // A pool built without I/O; the service owns it and closes it.
///     let pool = PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new());
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
///         SqlxBroker::new(pool).route::<SendEmail>("emails"),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub struct SqlxBroker<DB: Database, D = BuiltIn<DB>> {
    pub(crate) pool: Pool<DB>,
    pub(crate) dialect: DialectChoice<DB, D>,
    routes: Routes<DB, FormOf<D>>,
    poll_interval: Duration,
    lease: Duration,
}

impl<DB: Database, D: Dialect> fmt::Debug for SqlxBroker<DB, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlxBroker")
            .field("dialect", &self.dialect.name())
            .field("routes", &self.routes)
            .field("poll_interval", &self.poll_interval)
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

impl<DB: BuiltInDialect> SqlxBroker<DB, BuiltIn<DB>> {
    /// A broker on `pool`, with the dialect built into the crate for its database.
    ///
    /// Synchronous and free of I/O: the database type comes from the pool, and nothing connects
    /// before [`connect`](Broker::connect), which picks the dialect from the connection it checks.
    /// An `AnyPool` reaches a database named only then, and a database whose dialect's feature is
    /// off stops `connect` with [`SqlxBrokerError::Backend`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn build() -> Result<(), sqlx::Error> {
    /// use ruststream_sqlx::SqlxBroker;
    /// use sqlx::postgres::PgPoolOptions;
    ///
    /// let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/app")?;
    /// let broker = SqlxBroker::new(pool);
    /// # let _ = broker;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn new(pool: Pool<DB>) -> Self {
        Self::built(pool, DialectChoice::Picked(built_in::<DB>))
    }
}

impl<DB: QueueDatabase, D: Dialect + 'static> SqlxBroker<DB, D> {
    /// A broker on `pool` whose statements `dialect`, a dialect of the service's own, builds: a
    /// driver this crate has no dialect for, or a built-in dialect the service wraps to write a
    /// statement its own way.
    ///
    /// The dialect's type is the broker's second type parameter, so what a subscription asks of
    /// it is checked where the subscription mounts: a table in the row lock form needs the
    /// dialect to implement [`RowLock`](crate::dialect::RowLock), a lease table
    /// [`Lease`](crate::dialect::Lease), and a table that names an isolation level or a SQLite
    /// mode [`Opens`] for it. The dialect runs while subscriptions start; messages travel through
    /// the statements it built.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn build() -> Result<(), sqlx::Error> {
    /// use ruststream_sqlx::SqlxBroker;
    /// use ruststream_sqlx::dialect::Postgres as PostgresDialect;
    /// use sqlx::Postgres;
    /// use sqlx::postgres::PgPoolOptions;
    ///
    /// // A dialect of the service's own; the dialect module's Postgres stands in for it here.
    /// let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/app")?;
    /// let broker: SqlxBroker<Postgres, PostgresDialect> =
    ///     SqlxBroker::with_dialect(pool, PostgresDialect);
    /// # let _ = broker;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_dialect(pool: Pool<DB>, dialect: D) -> Self {
        Self::built(pool, DialectChoice::Given(Arc::new(dialect)))
    }

    fn built(pool: Pool<DB>, dialect: DialectChoice<DB, D>) -> Self {
        Self {
            pool,
            dialect,
            routes: Routes::default(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            lease: DEFAULT_LEASE,
        }
    }

    /// Leads publishes to `name` into the table of `Row`, through its [`Publish`], and opens a
    /// subscription by that name (`#[subscriber("name")]`) on the same table.
    ///
    /// A name ending in `*` leads every name that starts with what precedes it; an exact name
    /// wins over a prefix, and a longer prefix over a shorter one. Routing a name twice leads it
    /// to the last row type. The broker's default publisher is this route table: a reply or an
    /// `Out` slot without a policy of its own publishes through it.
    ///
    /// A route asks of the broker's dialect what an [`InboxQueue`](crate::InboxQueue) of the row
    /// asks: the table's form, and the isolation level or SQLite mode the struct declares. A
    /// by-name subscription names no row type, so the route is where its table is checked.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream::OutgoingMessage;
    /// use ruststream_sqlx::{Inbox, Publish, SqlxBroker};
    /// use sqlx::{PgConnection, PgPool, Postgres};
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "report_jobs")]
    /// pub struct Report {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(group)]
    ///     name: String,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl Publish<Postgres> for Report {
    ///     async fn publish(
    ///         conn: &mut PgConnection,
    ///         message: &OutgoingMessage<'_>,
    ///     ) -> Result<(), sqlx::Error> {
    ///         sqlx::query("INSERT INTO report_jobs (name, payload) VALUES ($1, $2)")
    ///             .bind(message.name())
    ///             .bind(message.payload())
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// // `reports.daily` and `reports.weekly` both become rows of `report_jobs`.
    /// pub fn broker(pool: PgPool) -> SqlxBroker<Postgres> {
    ///     SqlxBroker::new(pool).route::<Report>("reports.*")
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn route<Row>(mut self, name: impl Into<Cow<'static, str>>) -> Self
    where
        Row: Publish<DB> + Events<DB> + PayloadRow,
        Row::Form: FormOn<D>,
        D: Opens<Row::Opening>,
    {
        self.routes
            .add::<Row>(name.into(), <Row::Form as FormOn<D>>::erase);
        self
    }

    /// How long a subscription waits between claims that found its queue empty; one second
    /// unless set. A subscription's own [`poll_interval`](crate::InboxQueue::poll_interval)
    /// overrides it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn build() -> Result<(), sqlx::Error> {
    /// use std::time::Duration;
    ///
    /// use ruststream_sqlx::SqlxBroker;
    /// use sqlx::postgres::PgPoolOptions;
    ///
    /// let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/app")?;
    /// // Every queue on this broker checks twice a second when it runs dry.
    /// let broker = SqlxBroker::new(pool).poll_interval(Duration::from_millis(500));
    /// # let _ = broker;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// How long a claim leases a row of a table in the lease form (one with a
    /// `#[field(locked_until)]` field); thirty seconds unless set. A subscription's own
    /// [`lease`](crate::InboxQueue::lease) overrides it, and a table in another form ignores it.
    ///
    /// A lease is whole seconds, at least one: a shorter one is rounded up. The subscription
    /// extends the lease of every delivery in work each half lease, so a handler may run longer
    /// than its lease; the lease is how long a row stays out of the queue after its process
    /// crashed. A delivery dropped unsettled releases its row at once. A lease runs out under a
    /// running handler only when its extensions fail or stop (the database out of reach, the
    /// subscription closed, the broker [shut down](ConnectedBroker::shutdown)): the row then goes
    /// to the next claim, and the late settlement fails with [`SqlxBrokerError::LeaseLost`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn build() -> Result<(), sqlx::Error> {
    /// use std::time::Duration;
    ///
    /// use ruststream_sqlx::SqlxBroker;
    /// use sqlx::postgres::PgPoolOptions;
    ///
    /// let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/app")?;
    /// // A row whose process crashed goes back to the queue once its ten-second lease runs out.
    /// let broker = SqlxBroker::new(pool).lease(Duration::from_secs(10));
    /// # let _ = broker;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }
}

impl<DB: QueueDatabase, D: Dialect + 'static> Broker for SqlxBroker<DB, D> {
    type Error = SqlxBrokerError;
    type Connected = ConnectedSqlxBroker<DB, D>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let conn = self
            .pool
            .acquire()
            .await
            .map_err(|source| SqlxBrokerError::Connect { source })?;
        let dialect = self.dialect.resolve(&conn)?;
        drop(conn);
        Ok(ConnectedSqlxBroker::new(self, dialect, Handle::current()))
    }
}

/// What every handle of one connection shares.
pub(crate) struct Shared<DB: Database> {
    pub(crate) pool: Pool<DB>,
    /// The routes, each with the form of its table on the connection's dialect.
    pub(crate) routes: Routes<DB, FormDialect>,
    pub(crate) poll_interval: Duration,
    /// The lease a subscription in the lease form takes, unless it names its own.
    pub(crate) lease: Duration,
    /// The runtime `connect` ran on: the lease keepers run there, and so do the releases of lease
    /// deliveries dropped unsettled.
    pub(crate) runtime: Handle,
    /// The `Closed` flag: set by `shutdown`, read with one atomic load per publish and per claim.
    closed: AtomicBool,
    /// Wakes the claim loops waiting for their next claim when `shutdown` sets the flag, and stops
    /// the lease keepers, whose tokens are its children.
    pub(crate) stopping: CancellationToken,
    /// The queues this connection reads, by table and group; a table without groups is one queue.
    pub(crate) queues: Mutex<Vec<(&'static str, Option<String>)>>,
    /// The books of the connection's advisory subscriptions, each with the deliveries in work and
    /// the sessions that hold their locks.
    pub(crate) locks: Mutex<Vec<&'static LockBook<DB>>>,
    /// The sessions of the connection being closed after they ended holding a lock or a
    /// transaction: `shutdown` waits until none is.
    pub(crate) closing: &'static Closing,
    /// The test harness's books of this connection.
    #[cfg(feature = "testing")]
    pub(crate) harness: super::testing::Harness,
}

impl<DB: Database> Shared<DB> {
    /// Whether `shutdown` ran: the flag every handle of the connection reads before it works.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// The connected inbox broker: the typed witness that the database answered.
///
/// It hands out publishers and opens subscriptions; [`shutdown`](ConnectedBroker::shutdown) stops
/// its claim loops and the extension of its leases, and refuses every handle it handed out. A
/// delivery in work keeps its lease after `shutdown` and settles as before; its lease is no
/// longer extended. In the advisory lock form `shutdown` releases the lock of every delivery in
/// work and returns once no lock of the broker is left; such a delivery's settlement then fails
/// with [`SqlxBrokerError::Closed`].
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # async fn run(pool: sqlx::PgPool) -> Result<(), ruststream_sqlx::SqlxBrokerError> {
/// use ruststream::{Broker, ConnectedBroker};
/// use ruststream_sqlx::SqlxBroker;
///
/// let connected = SqlxBroker::new(pool).connect().await?;
/// let closed = connected.shutdown().await?;
/// tracing::info!(locks_released = closed.locks_released(), "inbox closed");
/// # Ok(())
/// # }
/// ```
pub struct ConnectedSqlxBroker<DB: Database, D = BuiltIn<DB>> {
    pub(crate) shared: Arc<Shared<DB>>,
    /// The dialect the connection's subscriptions build their statements with.
    pub(crate) dialect: Arc<D>,
}

impl<DB: Database, D: Dialect> fmt::Debug for ConnectedSqlxBroker<DB, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedSqlxBroker")
            .field("dialect", &self.dialect.name())
            .field("closed", &self.shared.is_closed())
            .finish_non_exhaustive()
    }
}

impl<DB: Database, D> ConnectedSqlxBroker<DB, D> {
    /// The connected form of `broker`, whose statements `dialect` builds and whose internal tasks
    /// run on `runtime`.
    pub(crate) fn new(broker: SqlxBroker<DB, D>, dialect: Arc<D>, runtime: Handle) -> Self {
        Self {
            shared: Arc::new(Shared {
                pool: broker.pool,
                routes: broker.routes.resolve(|form_of| form_of(&dialect)),
                poll_interval: broker.poll_interval,
                lease: broker.lease,
                closing: Closing::leak(runtime.clone()),
                runtime,
                closed: AtomicBool::new(false),
                stopping: CancellationToken::new(),
                queues: Mutex::new(Vec::new()),
                locks: Mutex::new(Vec::new()),
                #[cfg(feature = "testing")]
                harness: super::testing::Harness::default(),
            }),
            dialect,
        }
    }
}

impl<DB: QueueDatabase, D: Dialect + 'static> ConnectedBroker for ConnectedSqlxBroker<DB, D> {
    type Error = SqlxBrokerError;
    type Closed = ClosedSqlxBroker;

    /// Stops the claim loops and the extension of every lease, and refuses every handle handed
    /// out before. A delivery in work in the lease or row lock form settles as before; its lease
    /// runs out unless it settles first.
    ///
    /// In the advisory lock form it then releases the lock of every delivery in work: each
    /// session unlocks its key and goes back to the pool, or closes where the database does not
    /// confirm the release. It waits for each session lent to a settlement in flight, and for the
    /// sessions still closing after a delivery or a claim was dropped, and returns once no lock of
    /// the broker is left. In a service the runtime has stopped its subscriptions by then, and its
    /// shutdown timeout bounds the handlers that hold a session.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        // Why a flag rather than a type: the pool is the service's and stays open, so a
        // publisher handed out before shutdown would otherwise keep writing.
        self.shared.closed.store(true, Ordering::Release);
        self.shared.stopping.cancel();
        self.shared.closing.begin_shutdown();
        let shared = self.shared;
        async move {
            let books = shared
                .locks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let released = join_all(books.into_iter().map(LockBook::release_all)).await;
            // A session closing after its delivery or its claim was dropped may still hold its
            // lock.
            shared.closing.settled().await;
            Ok(ClosedSqlxBroker {
                locks_released: released.into_iter().sum(),
                connections_closed: shared.closing.forced(),
            })
        }
    }
}

/// The broker after `shutdown`: what it did to the advisory locks its deliveries held.
///
/// A broker without advisory subscriptions reports none of either.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # async fn run(pool: sqlx::PgPool) -> Result<(), ruststream_sqlx::SqlxBrokerError> {
/// use ruststream::{Broker, ConnectedBroker};
/// use ruststream_sqlx::{ClosedSqlxBroker, SqlxBroker};
///
/// let closed: ClosedSqlxBroker = SqlxBroker::new(pool).connect().await?.shutdown().await?;
/// tracing::info!(
///     locks_released = closed.locks_released(),
///     connections_closed = closed.connections_closed(),
///     "inbox closed",
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ClosedSqlxBroker {
    locks_released: usize,
    connections_closed: usize,
}

impl ClosedSqlxBroker {
    /// Locks `shutdown` released with `Unlock` (or freed from the process registry), whose sessions
    /// went back to the pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "postgres")]
    /// # async fn run(pool: sqlx::PgPool) -> Result<(), ruststream_sqlx::SqlxBrokerError> {
    /// use ruststream::{Broker, ConnectedBroker};
    /// use ruststream_sqlx::SqlxBroker;
    ///
    /// let closed = SqlxBroker::new(pool).connect().await?.shutdown().await?;
    /// if closed.locks_released() > 0 {
    ///     tracing::info!(
    ///         released = closed.locks_released(),
    ///         "deliveries in work went back to their queues",
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn locks_released(&self) -> usize {
        self.locks_released
    }

    /// Sessions that held a lock and were closed instead: where `shutdown`'s release failed, and
    /// where a delivery dropped unsettled, or a claim dropped midway, left its session closing when
    /// `shutdown` began.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "postgres")]
    /// # async fn run(pool: sqlx::PgPool) -> Result<(), ruststream_sqlx::SqlxBrokerError> {
    /// use ruststream::{Broker, ConnectedBroker};
    /// use ruststream_sqlx::SqlxBroker;
    ///
    /// let closed = SqlxBroker::new(pool).connect().await?.shutdown().await?;
    /// if closed.connections_closed() > 0 {
    ///     tracing::warn!(
    ///         closed = closed.connections_closed(),
    ///         "sessions holding a lock were closed rather than released",
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn connections_closed(&self) -> usize {
        self.connections_closed
    }
}

// Each description comes from `from_url`, which keeps the host and port and drops the user and
// the password: the document is published and shared.
#[cfg(feature = "postgres")]
impl<D: Dialect + 'static> DescribeServer for SqlxBroker<Postgres, D> {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(
            self.pool.connect_options().to_url_lossy().as_str(),
            "postgres",
        )
    }
}

#[cfg(feature = "mysql")]
impl<D: Dialect + 'static> DescribeServer for SqlxBroker<MySql, D> {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(self.pool.connect_options().to_url_lossy().as_str(), "mysql")
    }
}

/// SQLite runs inside the service, on a file or in memory: the description names the protocol and
/// no host, so it holds neither the database's path nor any other part of its URL, and SQLite takes
/// no credentials.
#[cfg(feature = "sqlite")]
impl<D: Dialect + 'static> DescribeServer for SqlxBroker<Sqlite, D> {
    fn describe_server(&self) -> ServerSpec {
        // Why not `from_url`: SQLite's URL holds a path, not a host, and sqlx's `to_url_lossy`
        // panics on a database named `file:..`, the name `sqlite::memory:` takes.
        // FIXME(sqlx-sqlite 0.9.0): `SqliteConnectOptions::to_url_lossy` panics with "BUG:
        // generated un-parseable URL: InvalidPort" on every database named `file:..`. Once sqlx
        // fixes it, the panic stops ruling out reading the URL here and this note goes; the
        // description stays in-process, since SQLite has no host either way.
        ServerSpec::in_process("sqlite")
    }
}

/// An `AnyPool` is described by the scheme of its URL, as the broker of that database describes
/// itself: the host and port of a server, or SQLite with no host.
#[cfg(feature = "any")]
impl<D: Dialect + 'static> DescribeServer for SqlxBroker<Any, D> {
    fn describe_server(&self) -> ServerSpec {
        // `Any` keeps the URL it was given, so reading it back is free of the SQLite panic above.
        let url = self.pool.connect_options().to_url_lossy();
        match url.scheme() {
            "sqlite" => ServerSpec::in_process("sqlite"),
            "postgres" | "postgresql" => ServerSpec::from_url(url.as_str(), "postgres"),
            "mysql" | "mariadb" => ServerSpec::from_url(url.as_str(), "mysql"),
            scheme => ServerSpec::from_url(url.as_str(), scheme),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClosedSqlxBroker;

    #[test]
    fn the_closed_broker_reports_the_counts_its_debug_shows() {
        let closed = ClosedSqlxBroker {
            locks_released: 2,
            connections_closed: 1,
        };
        assert_eq!(
            (closed.locks_released(), closed.connections_closed()),
            (2, 1)
        );
        assert_eq!(
            format!("{closed:?}"),
            "ClosedSqlxBroker { locks_released: 2, connections_closed: 1 }"
        );
    }
}
