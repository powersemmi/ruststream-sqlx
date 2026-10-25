//! The pool the outbox writes through, and the runtime its connections belong to.

use std::sync::OnceLock;

use sqlx::pool::PoolConnection;
use sqlx::{Database, Error, Pool};
use tokio::runtime::Handle;

use crate::home;
#[cfg(feature = "inbox")]
use crate::inbox::writers;

/// The pool every handle of one outbox shares, set once, and the runtime the service runs its
/// handlers' callers on, learnt from the first delivery the subscription middleware sees.
///
/// A publish on a dedicated thread's runtime takes its connection as if on that runtime, so a
/// connection the pool opens for it outlives the thread.
#[derive(Debug)]
pub struct Store<DB: Database> {
    pool: OnceLock<Pool<DB>>,
    home: OnceLock<Handle>,
    /// The pool's database where it takes one writer at a time, learnt by the first record a
    /// task writes while it holds such a database's delivery transaction.
    #[cfg(feature = "inbox")]
    one_writer: OnceLock<Option<u64>>,
}

impl<DB: Database> Default for Store<DB> {
    fn default() -> Self {
        #[cfg(feature = "inbox")]
        writers::track();
        Self {
            pool: OnceLock::new(),
            home: OnceLock::new(),
            #[cfg(feature = "inbox")]
            one_writer: OnceLock::new(),
        }
    }
}

impl<DB: Database> From<Pool<DB>> for Store<DB> {
    fn from(pool: Pool<DB>) -> Self {
        #[cfg(feature = "inbox")]
        writers::track();
        Self {
            pool: OnceLock::from(pool),
            home: OnceLock::new(),
            #[cfg(feature = "inbox")]
            one_writer: OnceLock::new(),
        }
    }
}

impl<DB: Database> Store<DB> {
    /// The pool, once set.
    pub(super) fn get(&self) -> Option<&Pool<DB>> {
        self.pool.get()
    }

    /// Sets the pool, unless it is set already.
    pub(super) fn set(&self, pool: Pool<DB>) -> Result<(), Pool<DB>> {
        self.pool.set(pool)
    }

    /// The pool's database where it takes one writer at a time, learnt from `learn` the first time.
    #[cfg(feature = "inbox")]
    pub(super) fn one_writer(&self, learn: impl FnOnce() -> Option<u64>) -> Option<u64> {
        *self.one_writer.get_or_init(learn)
    }

    /// Learns the service's runtime, `home`, unless it knows it already: one load once it does.
    pub(super) fn learn_home(&self, home: &Handle) {
        self.home.get_or_init(|| home.clone());
    }

    /// A connection of `pool`, taken as if on the service's runtime once the store knows it, and
    /// on the caller's before: a publish before any delivery runs on the service's runtime.
    pub(super) async fn acquire(&self, pool: &Pool<DB>) -> Result<PoolConnection<DB>, Error> {
        match self.home.get() {
            Some(home) => home::acquire(pool, home).await,
            None => pool.acquire().await,
        }
    }
}
