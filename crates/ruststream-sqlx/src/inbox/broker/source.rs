//! Where a broker's pool comes from: the service's own, which the broker never closes, or one the
//! broker builds at `connect` from the settings its constructor recorded, and closes at
//! `shutdown`.

#[cfg(any(feature = "postgres", feature = "mysql", feature = "any"))]
use std::borrow::Cow;

#[cfg(any(feature = "postgres", feature = "mysql", feature = "any"))]
use sqlx::ConnectOptions;
use sqlx::pool::PoolOptions;
use sqlx::{Connection, Database, Pool};

use crate::inbox::error::SqlxBrokerError;

/// The settings of a database connection, as the database's driver reads them.
pub(crate) type ConnectOptionsOf<DB> = <<DB as Database>::Connection as Connection>::Options;

/// Where the pool of a broker comes from.
pub(crate) enum PoolSource<DB: Database> {
    /// The service's pool, shared with the rest of the service: the broker never closes it.
    Service(Pool<DB>),
    /// The settings of a pool the broker builds at `connect`.
    Built {
        /// The database the pool connects to.
        target: Target<DB>,
        /// The pool's own settings: its size, its timeouts.
        pool: PoolOptions<DB>,
    },
}

/// The database a pool the broker builds connects to.
pub(crate) enum Target<DB: Database> {
    /// A URL, read by the driver at `connect`.
    Url(String),
    /// The driver's connect options.
    Options(ConnectOptionsOf<DB>),
}

/// Who closes the pool a connected broker serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PoolOwner {
    /// The service: the broker leaves the pool open at `shutdown`.
    Service,
    /// The broker, which built it: `shutdown` closes it.
    Broker,
}

impl<DB: Database> PoolSource<DB> {
    /// The pool the broker serves and who closes it.
    ///
    /// Opens no connection: a pool the broker builds connects when the broker first takes a
    /// connection. sqlx starts a pool's maintenance on the calling runtime, so this runs inside
    /// `connect`.
    ///
    /// # Errors
    ///
    /// [`SqlxBrokerError::Url`] when the driver cannot read the recorded URL.
    pub(crate) fn open(&self) -> Result<(Pool<DB>, PoolOwner), SqlxBrokerError> {
        let (target, pool) = match self {
            Self::Service(pool) => return Ok((pool.clone(), PoolOwner::Service)),
            Self::Built { target, pool } => (target, pool.clone()),
        };
        let options = match target {
            Target::Url(url) => url
                .parse::<ConnectOptionsOf<DB>>()
                .map_err(|source| SqlxBrokerError::Url { source })?,
            Target::Options(options) => options.clone(),
        };
        Ok((pool.connect_lazy_with(options), PoolOwner::Broker))
    }

    /// The URL of the database, for the server description; its credentials are dropped there.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "any"))]
    pub(crate) fn url(&self) -> Cow<'_, str> {
        match self {
            Self::Service(pool) => Cow::Owned(pool.connect_options().to_url_lossy().into()),
            Self::Built {
                target: Target::Url(url),
                ..
            } => Cow::Borrowed(url),
            Self::Built {
                target: Target::Options(options),
                ..
            } => Cow::Owned(options.to_url_lossy().into()),
        }
    }

    /// Which of the two the broker serves, for its `Debug`.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::Service(_) => "the service's",
            Self::Built { .. } => "built at connect",
        }
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::time::Duration;

    #[cfg(feature = "testing")]
    use ruststream::testing::InProcess;
    use ruststream::{Broker, ConnectedBroker};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};
    use tokio::time::timeout;

    use crate::{SqlxBroker, SqlxBrokerError};

    /// A database in memory that every connection naming it shares, and the connection that
    /// keeps it alive while the test runs.
    async fn shared_memory(name: &str) -> (String, SqliteConnection) {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let keeper = SqliteConnection::connect(&url)
            .await
            .expect("the database opens");
        (url, keeper)
    }

    #[test]
    fn the_settings_are_recorded_outside_any_runtime() {
        // The app's builder runs before the runtime: neither constructor may need one.
        let from_url = SqlxBroker::<Sqlite>::from_url("sqlite:jobs.db", SqlitePoolOptions::new());
        let from_options = SqlxBroker::<Sqlite>::from_connect_options(
            SqliteConnectOptions::new().filename("jobs.db"),
            SqlitePoolOptions::new(),
        );
        assert!(format!("{from_url:?}").starts_with("SqlxBroker"));
        assert!(format!("{from_options:?}").starts_with("SqlxBroker"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_builds_the_pool_from_the_url_and_shutdown_closes_it() {
        let (url, _keeper) = shared_memory("source_url").await;
        let connected =
            SqlxBroker::<Sqlite>::from_url(url, SqlitePoolOptions::new().max_connections(3))
                .connect()
                .await
                .expect("the broker connects");
        let pool = connected.shared.pool.clone();
        assert_eq!(
            (pool.options().get_max_connections(), pool.is_closed()),
            (3, false),
            "the pool follows the recorded options and is open while the broker is"
        );
        connected.shutdown().await.expect("the broker shuts down");
        assert!(pool.is_closed(), "the broker closes the pool it built");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_builds_the_pool_from_connect_options() {
        let (url, _keeper) = shared_memory("source_options").await;
        let options: SqliteConnectOptions = url.parse().expect("the URL parses");
        let connected =
            SqlxBroker::<Sqlite>::from_connect_options(options, SqlitePoolOptions::new())
                .connect()
                .await
                .expect("the broker connects");
        let pool = connected.shared.pool.clone();
        connected.shutdown().await.expect("the broker shuts down");
        assert!(pool.is_closed(), "the broker closes the pool it built");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_connection_lent_out_does_not_hold_up_shutdown() {
        let (url, _keeper) = shared_memory("source_lent").await;
        let connected = SqlxBroker::<Sqlite>::from_url(url, SqlitePoolOptions::new())
            .connect()
            .await
            .expect("the broker connects");
        let pool = connected.shared.pool.clone();
        // What a delivery in work in the row lock form holds across shutdown.
        let mut lent = pool.acquire().await.expect("a connection");
        timeout(Duration::from_secs(5), connected.shutdown())
            .await
            .expect("shutdown returns with a connection lent out")
            .expect("the broker shuts down");
        assert!(pool.is_closed(), "the pool takes no new work");
        sqlx::query("SELECT 1")
            .execute(&mut *lent)
            .await
            .expect("the lent connection still serves its delivery");
        assert!(
            pool.acquire().await.is_err(),
            "a closed pool lends no new connection"
        );
    }

    #[cfg(feature = "testing")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_in_process_connect_builds_and_closes_the_pool_too() {
        let (url, _keeper) = shared_memory("source_in_process").await;
        let connected = SqlxBroker::<Sqlite>::from_url(url, SqlitePoolOptions::new())
            .connect_in_process()
            .await
            .expect("the broker connects in process");
        let pool = connected.shared.pool.clone();
        connected.shutdown().await.expect("the broker shuts down");
        assert!(pool.is_closed(), "the broker closes the pool it built");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_leaves_the_service_pool_open() {
        let (url, _keeper) = shared_memory("source_service").await;
        let pool = SqlitePool::connect(&url).await.expect("the pool connects");
        let connected = SqlxBroker::new(pool.clone())
            .connect()
            .await
            .expect("the broker connects");
        connected.shutdown().await.expect("the broker shuts down");
        assert!(!pool.is_closed(), "the service's pool stays the service's");
        pool.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_url_the_driver_cannot_read_stops_connect() {
        let refused = SqlxBroker::<Sqlite>::from_url(
            "sqlite:jobs.db?mode=sideways",
            SqlitePoolOptions::new(),
        )
        .connect()
        .await
        .expect_err("the URL names no SQLite mode");
        assert!(
            matches!(refused, SqlxBrokerError::Url { .. }),
            "refused with {refused:?}"
        );
        assert!(
            refused
                .to_string()
                .starts_with("the inbox broker could not read its database URL"),
            "{refused}"
        );
    }
}
