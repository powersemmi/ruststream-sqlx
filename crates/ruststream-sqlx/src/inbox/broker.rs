//! `SqlxBroker`: its configuration and the connection it makes; the connected form and the closed
//! witness live in `connected`.

use std::borrow::Cow;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use ruststream::Broker;
use ruststream_sqlx_dialect::{Dialect, Opens};
use sqlx::pool::PoolOptions;
use sqlx::{Database, Pool};
use tokio::runtime::Handle;

use super::database::notify::{StartListening, start_listening};
use super::database::{BuiltIn, BuiltInDialect, Notifies, QueueDatabase};
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::events::Publish;
use super::publish::Routes;
use super::{FormDialect, FormOn, PayloadRow};

mod connected;
mod describe;
mod source;

pub(crate) use connected::Shared;
pub use connected::{ClosedSqlxBroker, ConnectedSqlxBroker};
pub(crate) use source::PoolOwner;
use source::{ConnectOptionsOf, PoolSource, Target};

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
/// [`from_url`](Self::from_url) and [`from_connect_options`](Self::from_connect_options) record
/// the pool's settings and do no I/O, so the service builds the broker in its app's builder;
/// [`connect`](Broker::connect) builds the pool, and
/// [`shutdown`](ruststream::ConnectedBroker::shutdown) closes it. [`new`](Self::new) takes a pool
/// the service shares with code of its own, and the broker never closes that one. `connect` takes
/// one connection to check the database, and picks the built-in dialect by it where the service
/// passed none. A message in
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
///     // The options read the `PG*` environment variables; the broker builds the pool when the
///     // service starts and closes it when the service stops.
///     let broker = SqlxBroker::from_connect_options(PgConnectOptions::new(), PgPoolOptions::new());
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
///         broker.route::<SendEmail>("emails"),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
///
/// # Testing
///
/// A test hands [`TestApp::start`] the app `main` runs, unchanged, and addresses the broker by its
/// production type (feature `testing`). The broker connects in process to the database the test's
/// pool reaches, so the service's SQL runs as in production, and the harness waits for every
/// publish: a row counts for the queue it lands in, its table and group, whatever name a
/// subscription reads it by. SQLite in memory needs no server. After `tokio::time::pause()` the queue reads "now"
/// from the test's clock, which `tb.advance(d)` moves past a delayed retry or a lease; a table on
/// [`DatabaseClock`](crate::DatabaseClock) reads the database's time instead. The crate registers
/// `SqlxBroker<Postgres>`, `SqlxBroker<MySql>`, `SqlxBroker<Sqlite>` and `SqlxBroker<Any>` with the
/// harness, and a broker on a dialect of the service's own is registered by the service
/// ([`with_dialect`](Self::with_dialect)).
///
/// [`TestApp::start`]: https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.start
///
/// ```no_run
/// # #[cfg(all(feature = "sqlite", feature = "chrono", feature = "testing"))]
/// # mod demo {
/// use std::error::Error;
///
/// use chrono::{DateTime, Utc};
/// use ruststream::OutgoingMessage;
/// use ruststream::testing::TestApp;
/// use ruststream_sqlx::prelude::*;
/// use serde::{Deserialize, Serialize};
/// use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
/// use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "thumbnail_jobs")]
/// pub struct MakeThumbnail {
///     #[field(id, generated)]
///     id: i64,
///     #[field(attempt, generated)]
///     attempt: i16,
///     #[field(locked_until)]
///     locked_until: Option<DateTime<Utc>>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Sqlite> for MakeThumbnail {
///     async fn publish(
///         conn: &mut SqliteConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO thumbnail_jobs (payload) VALUES (?)")
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
///
/// #[derive(Serialize, Deserialize, Outgoing)]
/// pub struct Image {
///     path: String,
/// }
///
/// #[subscriber(InboxQueue::<MakeThumbnail>::new("thumbnails"))]
/// async fn thumbnail(image: &Image) -> HandlerOutcome {
///     tracing::info!(path = %image.path, "making a thumbnail");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("thumbnails", "0.1.0")).with_broker(
///         SqlxBroker::new(pool).route::<MakeThumbnail>("thumbnails"),
///         |b| {
///             b.include(thumbnail);
///         },
///     )
/// }
///
/// const SCHEMA: &str = "CREATE TABLE thumbnail_jobs (id INTEGER PRIMARY KEY, \
///     attempt INTEGER NOT NULL DEFAULT 1, locked_until TEXT, payload BLOB NOT NULL)";
///
/// // The body of a `#[tokio::test]`.
/// pub async fn a_thumbnail_is_made() -> Result<(), Box<dyn Error>> {
///     // A database in memory that every connection naming it shares, alive while one of them is
///     // open: the test holds that one beside the pool.
///     let options: SqliteConnectOptions =
///         "sqlite:file:thumbnails?mode=memory&cache=shared".parse()?;
///     let _keeper = SqliteConnection::connect_with(&options).await?;
///     let pool = SqlitePoolOptions::new()
///         .min_connections(1)
///         .idle_timeout(None)
///         .connect_with(options)
///         .await?;
///     sqlx::raw_sql(SCHEMA).execute(&pool).await?;
///
///     let tb = TestApp::start(app(pool)).await?;
///     // Through the route and the service's own `Publish`, as in production.
///     tb.broker::<SqlxBroker<Sqlite>>()
///         .message(&Image { path: "cat.png".to_owned() })
///         .to("thumbnails")
///         .publish()
///         .await?;
///     tb.broker::<SqlxBroker<Sqlite>>()
///         .subscriber("thumbnails")
///         .assert_called_once()
///         .settled(HandlerOutcome::ack());
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct SqlxBroker<DB: Database, D = BuiltIn<DB>> {
    pub(crate) pool: PoolSource<DB>,
    pub(crate) dialect: DialectChoice<DB, D>,
    routes: Routes<DB, FormOf<D>>,
    poll_interval: Duration,
    lease: Duration,
    connection_limit: Option<NonZeroU32>,
    /// The start of the listening connection, with `listen_notify`.
    pub(crate) listen: Option<StartListening<DB>>,
}

impl<DB: Database, D: Dialect> fmt::Debug for SqlxBroker<DB, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlxBroker")
            .field("pool", &self.pool.kind())
            .field("dialect", &self.dialect.name())
            .field("routes", &self.routes)
            .field("poll_interval", &self.poll_interval)
            .field("lease", &self.lease)
            .field("connection_limit", &self.connection_limit)
            .field("listen_notify", &self.listen.is_some())
            .finish_non_exhaustive()
    }
}

impl<DB: BuiltInDialect> SqlxBroker<DB, BuiltIn<DB>> {
    /// A broker that builds its pool from `url` and `pool` when it connects, with the dialect
    /// built into the crate for the database.
    ///
    /// Synchronous and free of I/O, so the service builds it in its app's builder, before the
    /// runtime starts. [`connect`](Broker::connect) builds the pool, takes one connection to
    /// check the database, and picks the dialect by it;
    /// [`shutdown`](ruststream::ConnectedBroker::shutdown) closes the pool once the broker's own
    /// work is done. The driver reads the URL at `connect`: a URL it cannot read stops the
    /// service's start with [`SqlxBrokerError::Url`]. A pool the service also queries outside
    /// its handlers is the service's own: [`new`](Self::new) takes it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use std::env;
    ///
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::postgres::PgPoolOptions;
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
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let url = env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost/mail".into());
    ///     // Nothing connects here: the broker builds the pool when the service starts.
    ///     let broker = SqlxBroker::from_url(url, PgPoolOptions::new().max_connections(8));
    ///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(send);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn from_url(url: impl Into<String>, pool: PoolOptions<DB>) -> Self {
        let target = Target::Url(url.into());
        Self::built(
            PoolSource::Built { target, pool },
            DialectChoice::Picked(built_in::<DB>),
        )
    }

    /// A broker that builds its pool from the driver's connect options and `pool` when it
    /// connects, with the dialect built into the crate for the database.
    ///
    /// The options a service sets in code or reads from the environment
    /// (`PgConnectOptions::new()` reads the `PG*` variables); otherwise as
    /// [`from_url`](Self::from_url): synchronous, free of I/O, the pool built at `connect` and
    /// closed at `shutdown`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     // The options read the `PG*` environment variables; the application name is the
    ///     // service's own.
    ///     let options = PgConnectOptions::new().application_name("mailer");
    ///     let broker = SqlxBroker::from_connect_options(options, PgPoolOptions::new());
    ///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(send);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn from_connect_options(options: ConnectOptionsOf<DB>, pool: PoolOptions<DB>) -> Self {
        let target = Target::Options(options);
        Self::built(
            PoolSource::Built { target, pool },
            DialectChoice::Picked(built_in::<DB>),
        )
    }

    /// A broker on the service's own `pool`, with the dialect built into the crate for its
    /// database.
    ///
    /// For a pool the service shares with the rest of its code: an HTTP endpoint that writes
    /// rows, a migration run before the app. The broker never closes it; the service does, once
    /// the app has stopped. Synchronous and free of I/O: the database type comes from the pool,
    /// and nothing connects before [`connect`](Broker::connect), which picks the dialect from the
    /// connection it checks. An `AnyPool` reaches a database named only then, and a database
    /// whose dialect's feature is off stops `connect` with [`SqlxBrokerError::Backend`]. sqlx
    /// builds a pool only inside a Tokio runtime, so a service that owns one builds it in its
    /// own `main`; a broker that builds its pool itself is [`from_url`](Self::from_url).
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
    /// // The service shares the pool with code of its own, so it builds the pool in its `main`,
    /// // inside the runtime, and closes it after the app.
    /// #[tokio::main]
    /// pub async fn main() -> Result<(), Box<dyn Error>> {
    ///     // A lazy pool: nothing connects before the service starts. The options read the
    ///     // `PG*` environment variables.
    ///     let pool = PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new());
    ///     let app = app(pool.clone());
    /// #   // The example runs without a database: it stops before the app connects.
    /// #   drop(app);
    /// #   return Ok(());
    ///     app.run().await?;
    ///     pool.close().await;
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
        Self::built(
            PoolSource::Service(pool),
            DialectChoice::Picked(built_in::<DB>),
        )
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
    /// The crate registers the brokers of its built-in dialects with the test harness. A test of
    /// a broker on a dialect of the service's own registers its type once,
    /// `ruststream::register_testable_broker!(SqlxBroker<Postgres, Audited>);`, and addresses it
    /// by that type, `tb.broker::<SqlxBroker<Postgres, Audited>>()`.
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
    /// /// Postgres with statements of the service's own.
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
        Self::built(
            PoolSource::Service(pool),
            DialectChoice::Given(Arc::new(dialect)),
        )
    }

    fn built(pool: PoolSource<DB>, dialect: DialectChoice<DB, D>) -> Self {
        Self {
            pool,
            dialect,
            routes: Routes::default(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            lease: DEFAULT_LEASE,
            connection_limit: None,
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
    /// ([waking a subscription](https://powersemmi.github.io/ruststream-sqlx/latest/components/wake/)).
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
    /// subscription closed, the broker [shut down](ruststream::ConnectedBroker::shutdown)): the
    /// row then goes to the next claim, and the late settlement fails with
    /// [`SqlxBrokerError::LeaseLost`].
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

    /// The most connections the service opens on the database: the pool's size, and the
    /// connections of the dedicated threads of every subscription mounted with
    /// [`on_threads`](crate::InboxSettings::on_threads). No limit unless set.
    ///
    /// Each such subscription counts its threads times their connections when it opens. One
    /// that would take the sum past the limit refuses to open, with
    /// [`SqlxBrokerError::ConnectionLimit`] naming the numbers, and the service does not start.
    /// Set it to what the database lets this service open.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream::nonzero;
    /// use ruststream_sqlx::prelude::*;
    /// use sqlx::{PgPool, Postgres};
    /// # #[derive(Inbox, sqlx::FromRow)]
    /// # #[inbox(table = "scan_jobs")]
    /// # pub struct ScanJob { #[field(id)] id: i64, #[field(payload)] payload: Vec<u8> }
    /// # #[derive(serde::Deserialize)]
    /// # struct Image { path: String }
    ///
    /// #[subscriber(InboxQueue::<ScanJob>::new("scans"))]
    /// async fn scan(image: &Image, Ctx(pool): Ctx<keys::Pool<Postgres>>) -> HandlerOutcome {
    ///     let noted = sqlx::query("INSERT INTO scanned (path) VALUES ($1)")
    ///         .bind(&image.path)
    ///         .execute(&pool)
    ///         .await;
    ///     if noted.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     // The database lets this service open 20 connections: the pool's and 4 threads' own.
    ///     let broker = SqlxBroker::new(pool).connection_limit(nonzero!(20u32));
    ///     RustStream::new(AppInfo::new("scanner", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(scan.on_threads(InboxThreads::new(nonzero!(4))));
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn connection_limit(mut self, limit: NonZeroU32) -> Self {
        self.connection_limit = Some(limit);
        self
    }

    /// Wakes the broker's subscriptions on Postgres notifications: a row another process announced
    /// with `pg_notify` is claimed at once, not on the next poll.
    ///
    /// The broker holds one connection of the pool for its life, from
    /// [`connect`](Broker::connect) to [`shutdown`](ruststream::ConnectedBroker::shutdown), and
    /// listens there.
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
        let (pool, owner) = self.pool.open()?;
        let conn = pool
            .acquire()
            .await
            .map_err(|source| SqlxBrokerError::Connect { source })?;
        let dialect = self.dialect.resolve(&conn)?;
        drop(conn);
        let listening = match self.listen {
            Some(start) => Some(
                start(pool.clone())
                    .await
                    .map_err(|source| SqlxBrokerError::Connect { source })?,
            ),
            None => None,
        };
        Ok(ConnectedSqlxBroker::new(
            self,
            (pool, owner),
            dialect,
            Handle::current(),
            listening,
        ))
    }
}
