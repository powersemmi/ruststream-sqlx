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

use super::database::notify::{self, Listening, StartListening, start_listening};
use super::database::{BuiltIn, BuiltInDialect, Notifies, QueueDatabase};
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::events::Publish;
use super::form::advisory::LockBook;
use super::form::advisory::session::Closing;
use super::publish::{Routes, TableWake, Wakes};
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
/// use std::error::Error;
///
/// use ruststream::OutgoingMessage;
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, InboxQueue, Publish, SqlxBroker};
/// use serde::Deserialize;
/// use sqlx::postgres::{PgConnectOptions, PgPool};
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
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
///         SqlxBroker::new(pool).route::<SendEmail>("emails"),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
///
/// // sqlx builds a pool only inside a Tokio runtime, so the service builds it before the app.
/// // The service owns the pool and closes it.
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn Error>> {
///     let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
///     app(pool.clone()).run().await?;
///     pool.close().await;
///     Ok(())
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
    /// The start of the listening connection, with `listen_notify`.
    pub(crate) listen: Option<StartListening<DB>>,
}

impl<DB: Database, D: Dialect> fmt::Debug for SqlxBroker<DB, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlxBroker")
            .field("dialect", &self.dialect.name())
            .field("routes", &self.routes)
            .field("poll_interval", &self.poll_interval)
            .field("lease", &self.lease)
            .field("listen_notify", &self.listen.is_some())
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
    /// # mod demo {
    /// use std::error::Error;
    ///
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
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
    ///
    /// #[tokio::main]
    /// pub async fn main() -> Result<(), Box<dyn Error>> {
    ///     // Nothing connects here: the pool connects lazily, and the broker once the service
    ///     // starts. sqlx builds even a lazy pool only inside a Tokio runtime, so it is built
    ///     // here rather than in the app's builder. The options read the `PG*` environment
    ///     // variables.
    ///     let pool = PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new());
    ///     let app = app(pool);
    /// #   // The example runs without a database: it stops before the app connects.
    /// #   drop(app);
    /// #   return Ok(());
    ///     app.run().await?;
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     #[cfg(feature = "postgres")]
    /// #     demo::main()?;
    /// #     Ok(())
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
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx::dialect::{self, ClaimShape, Dialect, Opening, RowLock, Statement, StatementError, TableName, TableSpec};
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::PgPool;
    ///
    /// /// Postgres with statements of the service's own, written out in the crate overview.
    /// #[derive(Debug)]
    /// pub struct Audited;
    /// # impl Dialect for Audited {
    /// #     fn name(&self) -> &'static str { "audited" }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { dialect::Postgres.quote_into(ident, out) }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { dialect::Postgres.placeholder_into(index, out) }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { dialect::Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { dialect::Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { dialect::Postgres.insert(spec) }
    /// #     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> { dialect::Postgres.begin(opening) }
    /// #     fn fifo_guard(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { dialect::Postgres.fifo_guard(spec) }
    /// # }
    /// # impl RowLock for Audited {
    /// #     fn lock_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { dialect::Postgres.lock_claim(spec, shape) }
    /// # }
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
    /// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    /// async fn send(email: &Email) -> HandlerOutcome {
    ///     tracing::info!(to = %email.to, "sending");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     // A `SqlxBroker<Postgres, Audited>`: `send` mounts only because `Audited` implements
    ///     // `RowLock`, the form of `email_jobs`.
    ///     RustStream::new(AppInfo::new("mailer", "0.1.0"))
    ///         .with_broker(SqlxBroker::with_dialect(pool, Audited), |b| {
    ///             b.include(send);
    ///         })
    /// }
    /// # }
    /// # fn main() {}
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
            listen: None,
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
    /// use ruststream_sqlx::prelude::*;
    /// use serde::{Deserialize, Serialize};
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
    /// #[derive(Deserialize)]
    /// struct Daily {
    ///     day: u32,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "reports.weekly")]
    /// struct Weekly {
    ///     week: u32,
    /// }
    ///
    /// // Read by name from `report_jobs`, answered with a row of `report_jobs` in group `reports.weekly`.
    /// #[subscriber("reports.daily", reply)]
    /// async fn roll_up(daily: &Daily) -> Weekly {
    ///     Weekly { week: daily.day / 7 }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     // `reports.daily` and `reports.weekly` both lead into `report_jobs`.
    ///     let broker = SqlxBroker::new(pool).route::<Report>("reports.*");
    ///     RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(roll_up);
    ///     })
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
    /// overrides it. A publish through this broker wakes the subscriptions of its table and group
    /// before then, and so does a notification with [`listen_notify`](Self::listen_notify)
    /// ([waking a subscription](crate#waking-a-subscription)).
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
    /// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    /// async fn send(email: &Email) -> HandlerOutcome {
    ///     tracing::info!(to = %email.to, "sending");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     // Every queue on this broker checks twice a second when it runs dry.
    ///     let broker = SqlxBroker::new(pool).poll_interval(Duration::from_millis(500));
    ///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(send);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::PgPool;
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "video_jobs")]
    /// pub struct Transcode {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(locked_until)]
    ///     locked_until: Option<DateTime<Utc>>,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Video {
    ///     path: String,
    /// }
    ///
    /// #[subscriber(InboxQueue::<Transcode>::new("videos"))]
    /// async fn transcode(video: &Video) -> HandlerOutcome {
    ///     tracing::info!(path = %video.path, "transcoding");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     // A video whose process crashed goes back to the queue once its ten-second lease runs out.
    ///     let broker = SqlxBroker::new(pool).lease(Duration::from_secs(10));
    ///     RustStream::new(AppInfo::new("transcoder", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(transcode);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Wakes the broker's subscriptions on Postgres notifications: a row another process announced
    /// with `pg_notify` is claimed at once, not on the next poll.
    ///
    /// The broker holds one connection of the pool for its life, from
    /// [`connect`](Broker::connect) to [`shutdown`](ConnectedBroker::shutdown), and listens there.
    /// Each subscription listens on the channel of its table, qualified with its schema, as it
    /// opens; a notification whose payload names a group wakes that group's subscription, and one
    /// with an empty payload wakes every subscription of the table. Each publish of the broker
    /// sends `SELECT pg_notify(table, group)` on the connection that wrote the row, right after
    /// the write: one more statement per publish. A publish whose notification fails is still a
    /// publish; the row waits for the poll interval and the failure is logged.
    ///
    /// The poll interval stays: a notification sent while the listening connection was lost is
    /// gone, so after a reconnect every subscription claims once, and a row written by the
    /// service's own SQL or a handler's [`Tx`](crate::Tx) wakes nobody unless the service sends
    /// the notification itself. It is opt-in because of its cost: the pool connection it holds,
    /// the statement each publish adds, and the lock, global to the server, that Postgres takes
    /// at the commit of each transaction that notifies, so those commits run one after another,
    /// which limits many concurrent writers.
    ///
    /// A table whose qualified name is longer than Postgres's 63-byte channel names stops its
    /// subscription at startup with [`SqlxBrokerError::Declaration`].
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
    /// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    /// async fn send(email: &Email) -> HandlerOutcome {
    ///     tracing::info!(to = %email.to, "sending");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     // A row another service writes and announces with
    ///     // `SELECT pg_notify('email_jobs', 'emails')` is sent at once; the poll each minute
    ///     // catches a row whose notification was lost.
    ///     let broker = SqlxBroker::new(pool)
    ///         .poll_interval(Duration::from_secs(60))
    ///         .listen_notify();
    ///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(send);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn listen_notify(mut self) -> Self
    where
        DB: Notifies,
    {
        self.listen = Some(start_listening::<DB>());
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
        let listening = match self.listen {
            Some(start) => Some(
                start(self.pool.clone())
                    .await
                    .map_err(|source| SqlxBrokerError::Connect { source })?,
            ),
            None => None,
        };
        Ok(ConnectedSqlxBroker::new(
            self,
            dialect,
            Handle::current(),
            listening,
        ))
    }
}

/// What every handle of one connection shares.
pub(crate) struct Shared<DB: Database> {
    pub(crate) pool: Pool<DB>,
    /// The routes, each with the form of its table on the connection's dialect.
    pub(crate) routes: Routes<DB, FormDialect, &'static TableWake>,
    /// The wake-ups of the subscriptions of each table the connection's publishers write.
    pub(crate) wakes: Wakes,
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
    /// The listening connection, with `listen_notify`.
    pub(crate) listening: Option<Listening>,
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
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::prelude::*;
/// use serde::Serialize;
/// use sqlx::{PgConnection, PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "cleanup_jobs")]
/// pub struct Cleanup {
///     #[field(id, generated)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for Cleanup {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO cleanup_jobs (payload) VALUES ($1)")
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "cleanup")]
/// struct Sweep {
///     older_than_days: u32,
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("maintenance", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // Once the broker connected, the policy pairs against the `ConnectedSqlxBroker` and the
///         // hook schedules the first sweep.
///         b.after_startup(Repository::<Cleanup>::default(), async move |cleanups| {
///             cleanups.message(&Sweep { older_than_days: 30 }).publish().await
///         });
///     })
/// }
/// # }
/// # fn main() {}
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
    /// The connected form of `broker`, whose statements `dialect` builds, whose internal tasks
    /// run on `runtime`, and which listens on `listening` with `listen_notify`.
    pub(crate) fn new(
        broker: SqlxBroker<DB, D>,
        dialect: Arc<D>,
        runtime: Handle,
        listening: Option<Listening>,
    ) -> Self {
        let wakes = Wakes::default();
        let routes = broker.routes.resolve(
            |form_of| form_of(&dialect),
            |route| wakes.table(&route.description().spec),
        );
        Self {
            shared: Arc::new(Shared {
                pool: broker.pool,
                routes,
                wakes,
                poll_interval: broker.poll_interval,
                lease: broker.lease,
                closing: Closing::leak(runtime.clone()),
                runtime,
                closed: AtomicBool::new(false),
                stopping: CancellationToken::new(),
                queues: Mutex::new(Vec::new()),
                locks: Mutex::new(Vec::new()),
                listening,
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
            notify::close(&shared).await;
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
