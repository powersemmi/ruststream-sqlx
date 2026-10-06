//! The process's registry of the advisory keys in work, for a dialect whose locks the process
//! keeps, and how the process tells the databases of those keys apart.

use std::any::Any;
use std::collections::HashSet;
use std::hash::BuildHasher;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use foldhash::fast::FixedState;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use sqlx::ConnectOptions;
use sqlx::any::AnyConnectOptions;
#[cfg(feature = "mysql")]
use sqlx::mysql::MySqlConnectOptions;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgConnectOptions;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Database, Pool};

/// The database `pool` reaches, as the process tells databases apart, hashed once when a
/// subscription opens. A SQLite database is its file, by its canonical path where the file
/// resolves, so two spellings of one file are one database, or the name of an in-memory database,
/// under `Any` too; any other database is the URL its options render.
pub(super) fn database_of<DB: Database>(pool: &Pool<DB>) -> u64 {
    let options = pool.connect_options();
    let options: &dyn Any = &*options;
    let hasher = FixedState::with_seed(SEED);
    #[cfg(feature = "sqlite")]
    if let Some(sqlite) = options.downcast_ref::<SqliteConnectOptions>() {
        return file_of(&hasher, sqlite);
    }
    if let Some(any) = options.downcast_ref::<AnyConnectOptions>() {
        #[cfg(feature = "sqlite")]
        if any.database_url.scheme() == "sqlite"
            && let Ok(sqlite) = SqliteConnectOptions::from_url(&any.database_url)
        {
            return file_of(&hasher, &sqlite);
        }
        return hasher.hash_one(any.database_url.as_str());
    }
    #[cfg(feature = "postgres")]
    if let Some(postgres) = options.downcast_ref::<PgConnectOptions>() {
        return hasher.hash_one(postgres.to_url_lossy().as_str());
    }
    #[cfg(feature = "mysql")]
    if let Some(mysql) = options.downcast_ref::<MySqlConnectOptions>() {
        return hasher.hash_one(mysql.to_url_lossy().as_str());
    }
    // A database of a driver the crate does not name shares its keys with every other such
    // database: they wait for each other, a delay and never a double delivery.
    0
}

/// The SQLite database `sqlite` opens, hashed: its file by its canonical path, or the name of an
/// in-memory database, which resolves to no file.
#[cfg(feature = "sqlite")]
fn file_of(hasher: &FixedState, sqlite: &SqliteConnectOptions) -> u64 {
    let name = sqlite.get_filename();
    std::fs::canonicalize(name).map_or_else(|_| hasher.hash_one(name), |file| hasher.hash_one(file))
}

/// The process's registry of advisory keys in work, for a dialect whose locks the process keeps
/// (SQLite): the 64-bit hash of each key with the database it belongs to.
///
/// Two keys with one hash wait for each other, a delay and never a double delivery. A key is its
/// database's, as a server keeps each database's locks apart: two databases in one process hold one
/// key apart, and two brokers on one database pass over each other's keys.
pub(crate) struct ProcessLocks;

/// The seed of the key hashes, fixed so a key hashes alike in every run.
const SEED: u64 = 0x5d1c_9e37_79b9_7f4a;

/// The hashes of the keys in work. The set keeps its capacity, so a key in work allocates only
/// when the process has more keys in work than ever before.
static KEYS: LazyLock<Mutex<HashSet<u64, FixedState>>> =
    LazyLock::new(|| Mutex::new(HashSet::with_hasher(FixedState::default())));

impl ProcessLocks {
    /// Takes `key` of the database `database` names for the process: `None` while a key of its
    /// hash is in work.
    pub(crate) fn try_take(database: u64, key: &str) -> Option<ProcessKey> {
        let hash = FixedState::with_seed(SEED).hash_one((database, key));
        // The key is made only when taken: a key dropped here would free its hash, which another
        // holder keeps.
        let taken = Self::keys().insert(hash);
        taken.then(|| ProcessKey(hash))
    }

    fn keys() -> MutexGuard<'static, HashSet<u64, FixedState>> {
        KEYS.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A key the process keeps in work, by its hash; dropping it frees the key.
#[derive(Debug)]
pub(crate) struct ProcessKey(u64);

impl Drop for ProcessKey {
    fn drop(&mut self) {
        ProcessLocks::keys().remove(&self.0);
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::path::PathBuf;

    use sqlx::Error;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::{ProcessLocks, database_of};

    #[tokio::test]
    async fn a_sqlite_database_is_its_file_however_it_is_spelled() -> Result<(), Error> {
        let dir = std::env::temp_dir().join(format!("rs-sqlx-identity-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("nested")).map_err(Error::Io)?;
        let open = async |path: PathBuf| {
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(path)
                        .create_if_missing(true),
                )
                .await
        };
        let jobs = open(dir.join("jobs.db")).await?;
        let respelled = open(dir.join("nested").join("..").join("jobs.db")).await?;
        let other = open(dir.join("other.db")).await?;
        assert_eq!(
            database_of(&jobs),
            database_of(&respelled),
            "two spellings of one file are one database"
        );
        assert_ne!(database_of(&jobs), database_of(&other));
        for pool in [jobs, respelled, other] {
            pool.close().await;
        }
        std::fs::remove_dir_all(&dir).map_err(Error::Io)?;
        // An in-memory database is its name: one name is one database, two names are two.
        let memory = |name: &str| {
            let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
            SqlitePoolOptions::new().connect_lazy(&url)
        };
        let first = memory("rs-sqlx-identity-first")?;
        assert_eq!(
            database_of(&first),
            database_of(&memory("rs-sqlx-identity-first")?)
        );
        assert_ne!(
            database_of(&first),
            database_of(&memory("rs-sqlx-identity-second")?)
        );
        Ok(())
    }

    #[test]
    fn a_refused_take_keeps_the_holders_key() {
        let held = ProcessLocks::try_take(0, "unit-refused-take").expect("a free key is taken");
        assert!(ProcessLocks::try_take(0, "unit-refused-take").is_none());
        assert!(
            ProcessLocks::try_take(0, "unit-refused-take").is_none(),
            "the refused take left the key with its holder"
        );
        drop(held);
        assert!(
            ProcessLocks::try_take(0, "unit-refused-take").is_some(),
            "a freed key is taken again"
        );
    }
}
