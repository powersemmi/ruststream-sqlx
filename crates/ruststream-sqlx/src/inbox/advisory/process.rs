//! The process's registry of the advisory keys in work, for a dialect whose locks the process
//! keeps, and how the process tells the databases of those keys apart.

use std::any::Any;
use std::collections::{HashMap, HashSet};
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
/// (SQLite): the 64-bit hash of each key with the database it belongs to, and how many keys in work
/// each database holds.
///
/// Two keys with one hash wait for each other, a delay and never a double delivery. A key is its
/// database's, as a server keeps each database's locks apart: two databases in one process hold one
/// key apart, and two brokers on one database pass over each other's keys.
pub(crate) struct ProcessLocks;

/// The seed of the key hashes, fixed so a key hashes alike in every run.
const SEED: u64 = 0x5d1c_9e37_79b9_7f4a;

/// The keys in work. Both collections keep their capacity, and a database keeps its count at zero,
/// so a key in work allocates only when the process has more keys in work than ever before, or a
/// database it has not seen.
static KEYS: LazyLock<Mutex<Registry>> = LazyLock::new(|| {
    Mutex::new(Registry {
        keys: HashSet::with_hasher(FixedState::default()),
        held: HashMap::with_hasher(FixedState::default()),
    })
});

/// The keys in work, and how many each database holds.
struct Registry {
    /// The hashes of the keys in work.
    keys: HashSet<u64, FixedState>,
    /// How many keys in work each database holds, by the database as the process tells them apart.
    held: HashMap<u64, usize, FixedState>,
}

impl ProcessLocks {
    /// Takes `key` of the database `database` names for the process: `None` while a key of its
    /// hash is in work.
    pub(crate) fn try_take(database: u64, key: &str) -> Option<ProcessKey> {
        let hash = FixedState::with_seed(SEED).hash_one((database, key));
        let mut registry = Self::registry();
        // The key is made only when taken: a key dropped here would free its hash, which another
        // holder keeps.
        if !registry.keys.insert(hash) {
            return None;
        }
        *registry.held.entry(database).or_insert(0) += 1;
        drop(registry);
        Some(ProcessKey { hash, database })
    }

    /// How many keys in work the database `database` names holds, in every subscription and
    /// broker of the process.
    pub(crate) fn held(database: u64) -> usize {
        Self::registry().held.get(&database).copied().unwrap_or(0)
    }

    fn registry() -> MutexGuard<'static, Registry> {
        KEYS.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A key the process keeps in work, by its hash, with its database; dropping it frees the key.
#[derive(Debug)]
pub(crate) struct ProcessKey {
    hash: u64,
    database: u64,
}

impl Drop for ProcessKey {
    fn drop(&mut self) {
        let mut registry = ProcessLocks::registry();
        registry.keys.remove(&self.hash);
        if let Some(held) = registry.held.get_mut(&self.database) {
            *held = held.saturating_sub(1);
        }
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

    #[test]
    fn the_registry_counts_the_keys_each_database_holds() {
        // Databases no other test names, so the counts are this test's alone.
        let (jobs, other) = (0x5eed_0001, 0x5eed_0002);
        let first = ProcessLocks::try_take(jobs, "unit-count-1").expect("a free key is taken");
        let second = ProcessLocks::try_take(jobs, "unit-count-2").expect("a free key is taken");
        let elsewhere = ProcessLocks::try_take(other, "unit-count-1").expect("a free key is taken");
        assert!(
            ProcessLocks::try_take(jobs, "unit-count-1").is_none(),
            "a refused take counts nothing"
        );
        assert_eq!(ProcessLocks::held(jobs), 2);
        assert_eq!(ProcessLocks::held(other), 1);
        drop(first);
        assert_eq!(ProcessLocks::held(jobs), 1, "a freed key leaves the count");
        drop((second, elsewhere));
        assert_eq!(
            (ProcessLocks::held(jobs), ProcessLocks::held(other)),
            (0, 0)
        );
    }
}
