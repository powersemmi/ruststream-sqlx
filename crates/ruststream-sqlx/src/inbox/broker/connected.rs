//! The connected inbox broker, the state its handles share, and the closed witness `shutdown`
//! returns.

use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::join_all;
use ruststream::ConnectedBroker;
use ruststream_sqlx_dialect::Dialect;
use sqlx::{Database, Pool};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use crate::inbox::FormDialect;
use crate::inbox::database::notify::{self, Listening};
use crate::inbox::database::{BuiltIn, QueueDatabase};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::form::advisory::LockBook;
use crate::inbox::form::advisory::session::Closing;
use crate::inbox::publish::{Routes, TableWake, Wakes};
use crate::inbox::threads::InboxThreads;

use super::SqlxBroker;

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
    /// The most connections the service opens on the database, where it set a limit.
    connection_limit: Option<NonZeroU32>,
    /// The connections the dedicated threads of the subscriptions opened so far open at most.
    thread_connections: Mutex<u64>,
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
    pub(crate) harness: crate::inbox::testing::Harness,
}

impl<DB: Database> Shared<DB> {
    /// Whether `shutdown` ran: the flag every handle of the connection reads before it works.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Counts `threads`, the dedicated threads of the subscription `name` opens, against the
    /// connection limit: the pool's size and every subscription's threads so far must fit it.
    ///
    /// # Errors
    ///
    /// [`SqlxBrokerError::ConnectionLimit`] when the sum passes the limit; the threads are not
    /// counted then.
    pub(crate) fn count_threads(
        &self,
        name: &str,
        threads: &InboxThreads,
    ) -> Result<(), SqlxBrokerError> {
        let pool = u64::from(self.pool.options().get_max_connections());
        let mut counted = self
            .thread_connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let wanted = counted.saturating_add(threads.total_connections());
        if let Some(limit) = self.connection_limit
            && pool.saturating_add(wanted) > u64::from(limit.get())
        {
            drop(counted);
            return Err(SqlxBrokerError::ConnectionLimit {
                subscription: name.to_owned(),
                limit: limit.get(),
                pool,
                threads: wanted,
            });
        }
        *counted = wanted;
        drop(counted);
        Ok(())
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
                connection_limit: broker.connection_limit,
                thread_connections: Mutex::new(0),
                closing: Closing::leak(runtime.clone()),
                runtime,
                closed: AtomicBool::new(false),
                stopping: CancellationToken::new(),
                queues: Mutex::new(Vec::new()),
                locks: Mutex::new(Vec::new()),
                listening,
                #[cfg(feature = "testing")]
                harness: crate::inbox::testing::Harness::default(),
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
