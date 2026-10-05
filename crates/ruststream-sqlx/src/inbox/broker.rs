//! `SqlxBroker` and its lifecycle: the configuration, the connected form, the closed witness.

use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream::{Broker, ConnectedBroker};
use ruststream_sqlx_dialect::Dialect;
use sqlx::{Database, Pool};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use super::PayloadRow;
use super::database::{BuiltInDialect, QueueDatabase};
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::events::Publish;
use super::publish::Routes;

/// How long a subscription waits between claims that found its queue empty, unless it names
/// another interval.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How long a claim leases a row of a table in the lease form, unless the subscription names
/// another lease.
const DEFAULT_LEASE: Duration = Duration::from_secs(30);

/// The dialect a broker builds statements with: one built into the crate, or the service's own.
pub(crate) enum DialectHandle {
    BuiltIn(&'static dyn Dialect),
    Service(Arc<dyn Dialect>),
}

impl DialectHandle {
    pub(crate) fn get(&self) -> &dyn Dialect {
        match self {
            Self::BuiltIn(dialect) => *dialect,
            Self::Service(dialect) => dialect.as_ref(),
        }
    }
}

/// The inbox broker: task queues in the service's own tables, served through a sqlx pool.
///
/// [`new`](Self::new) records the pool and does no I/O; the pool belongs to the service, and the
/// broker never closes it. [`connect`](Broker::connect) takes one connection to check the
/// database. A message in work holds one of the pool's connections until it settles (in the lease
/// form only while it settles), and a publish takes another, so the pool is sized for both.
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
pub struct SqlxBroker<DB: Database> {
    pub(crate) pool: Pool<DB>,
    dialect: DialectHandle,
    routes: Routes<DB>,
    poll_interval: Duration,
    lease: Duration,
}

impl<DB: Database> fmt::Debug for SqlxBroker<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlxBroker")
            .field("dialect", &self.dialect.get().name())
            .field("routes", &self.routes)
            .field("poll_interval", &self.poll_interval)
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

impl<DB: BuiltInDialect> SqlxBroker<DB> {
    /// A broker on `pool`, with the dialect built into the crate for its database.
    ///
    /// Synchronous and free of I/O: the database type comes from the pool, and nothing connects
    /// before [`connect`](Broker::connect).
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
        Self::built(pool, DialectHandle::BuiltIn(DB::dialect()))
    }
}

impl<DB: QueueDatabase> SqlxBroker<DB> {
    /// A broker on `pool` whose statements `dialect` builds: a driver this crate has no dialect
    /// for, served by the service's own.
    ///
    /// The dialect runs while subscriptions start; messages travel through the statements it
    /// built.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn build() -> Result<(), sqlx::Error> {
    /// use ruststream_sqlx::SqlxBroker;
    /// use ruststream_sqlx::dialect::Postgres;
    /// use sqlx::postgres::PgPoolOptions;
    ///
    /// // A dialect value of the service's own; the built-in one stands in for it here.
    /// let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/app")?;
    /// let broker = SqlxBroker::with_dialect(pool, Postgres);
    /// # let _ = broker;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_dialect(pool: Pool<DB>, dialect: impl Dialect + 'static) -> Self {
        Self::built(pool, DialectHandle::Service(Arc::new(dialect)))
    }

    fn built(pool: Pool<DB>, dialect: DialectHandle) -> Self {
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
    {
        self.routes.add::<Row>(name.into());
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

impl<DB: QueueDatabase> Broker for SqlxBroker<DB> {
    type Error = SqlxBrokerError;
    type Connected = ConnectedSqlxBroker<DB>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        drop(
            self.pool
                .acquire()
                .await
                .map_err(|source| SqlxBrokerError::Connect { source })?,
        );
        Ok(ConnectedSqlxBroker::new(self, Handle::current()))
    }
}

/// What every handle of one connection shares.
pub(crate) struct Shared<DB: Database> {
    pub(crate) pool: Pool<DB>,
    pub(crate) dialect: DialectHandle,
    pub(crate) routes: Routes<DB>,
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
/// longer extended.
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
/// let _closed = connected.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct ConnectedSqlxBroker<DB: Database> {
    pub(crate) shared: Arc<Shared<DB>>,
}

impl<DB: Database> fmt::Debug for ConnectedSqlxBroker<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedSqlxBroker")
            .field("dialect", &self.shared.dialect.get().name())
            .field("closed", &self.shared.is_closed())
            .finish_non_exhaustive()
    }
}

impl<DB: Database> ConnectedSqlxBroker<DB> {
    /// The connected form of `broker`, whose internal tasks run on `runtime`.
    pub(crate) fn new(broker: SqlxBroker<DB>, runtime: Handle) -> Self {
        Self {
            shared: Arc::new(Shared {
                pool: broker.pool,
                dialect: broker.dialect,
                routes: broker.routes,
                poll_interval: broker.poll_interval,
                lease: broker.lease,
                runtime,
                closed: AtomicBool::new(false),
                stopping: CancellationToken::new(),
                queues: Mutex::new(Vec::new()),
                #[cfg(feature = "testing")]
                harness: super::testing::Harness::default(),
            }),
        }
    }
}

impl<DB: QueueDatabase> ConnectedBroker for ConnectedSqlxBroker<DB> {
    type Error = SqlxBrokerError;
    type Closed = ClosedSqlxBroker;

    /// Stops the claim loops and the extension of every lease, and refuses every handle handed
    /// out before. A delivery in work settles as before; its lease runs out unless it settles
    /// first.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        // Why a flag rather than a type: the pool is the service's and stays open, so a
        // publisher handed out before shutdown would otherwise keep writing.
        self.shared.closed.store(true, Ordering::Release);
        self.shared.stopping.cancel();
        ready(Ok(ClosedSqlxBroker { _private: () }))
    }
}

/// The terminal witness of a shut-down inbox broker.
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
/// tracing::info!(?closed, "inbox closed");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ClosedSqlxBroker {
    _private: (),
}

#[cfg(feature = "postgres")]
impl ruststream::DescribeServer for SqlxBroker<sqlx::Postgres> {
    fn describe_server(&self) -> ruststream::ServerSpec {
        use sqlx::ConnectOptions;

        // `from_url` keeps the host and port and drops the user and the password: the document
        // is published and shared.
        ruststream::ServerSpec::from_url(
            self.pool.connect_options().to_url_lossy().as_str(),
            "postgres",
        )
    }
}
