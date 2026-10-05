//! `SqlxBroker` and its lifecycle: the configuration, the connected form, the closed witness.

use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream::{Broker, ConnectedBroker};
use ruststream_sqlx_dialect::Dialect;
use sqlx::{Database, Pool};
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
/// database. Subscriptions read tables through [`InboxQueue`](crate::InboxQueue) descriptors;
/// publishing writes them through [`Repository`](crate::Repository) policies or through the
/// routes this builder records.
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
}

impl<DB: Database> fmt::Debug for SqlxBroker<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlxBroker")
            .field("dialect", &self.dialect.get().name())
            .field("routes", &self.routes)
            .field("poll_interval", &self.poll_interval)
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
        Ok(ConnectedSqlxBroker::from(self))
    }
}

/// What every handle of one connection shares.
pub(crate) struct Shared<DB: Database> {
    pub(crate) pool: Pool<DB>,
    pub(crate) dialect: DialectHandle,
    pub(crate) routes: Routes<DB>,
    pub(crate) poll_interval: Duration,
    /// The `Closed` flag: cancelled by `shutdown`, read with one atomic load per publish.
    pub(crate) closed: CancellationToken,
    /// The queues this connection reads, by table and name.
    pub(crate) queues: Mutex<Vec<(&'static str, String)>>,
    /// The test harness's books of this connection.
    #[cfg(feature = "testing")]
    pub(crate) harness: super::testing::Harness,
}

/// The connected inbox broker: the typed witness that the database answered.
///
/// It hands out publishers and opens subscriptions; [`shutdown`](ConnectedBroker::shutdown) stops
/// its claim loops and refuses every handle it handed out.
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
            .field("closed", &self.shared.closed.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl<DB: Database> From<SqlxBroker<DB>> for ConnectedSqlxBroker<DB> {
    fn from(broker: SqlxBroker<DB>) -> Self {
        Self {
            shared: Arc::new(Shared {
                pool: broker.pool,
                dialect: broker.dialect,
                routes: broker.routes,
                poll_interval: broker.poll_interval,
                closed: CancellationToken::new(),
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

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        // Why a flag rather than a type: the pool is the service's and stays open, so a
        // publisher handed out before shutdown would otherwise keep writing.
        self.shared.closed.cancel();
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
