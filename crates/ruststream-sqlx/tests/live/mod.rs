//! The gate the live suites share, the stands they run on, and the rows they read.
//!
//! A live test skips when its stand's URL is unset, which keeps `cargo test` usable on a laptop
//! with no stand. The same skip in a job that started the stand would be a lie, so
//! `just test-brokers` and CI set `RUSTSTREAM_REQUIRE_LIVE`, and under it a skip fails, naming
//! what it wanted. The SQLite stand runs in memory and needs no URL, so it never skips.
//!
//! A suite writes each test once. [`matrix!`] runs it on every stand a feature turns on, once per
//! form of the queue rows that stand serves; [`fifo_matrix!`] the same for the forms whose claim
//! statement takes a FIFO group's head, the row lock and the lease form, and [`server_matrix!`]
//! those on the stands that run as servers. [`stands!`] runs a test once per stand, for a test
//! whose rows name their own form, [`row_lock_stands!`] once per stand that serves the row lock
//! form, [`lease_stands!`] once per stand of the lease form, [`advisory_stands!`] once per stand
//! of the advisory lock form's suite, and [`advisory_server_stands!`] once per such stand that runs
//! as a server. A stand module gives each
//! test a database of its own, the dialect the broker builds the stand's statements with, and reads
//! the tables in its own SQL; a row module holds the queue rows of one form under the names every
//! form shares.

// Each live suite is its own test binary and uses the part of this module its topic needs, so
// what one of them leaves alone, a macro included, is not dead code.
#![allow(dead_code, unused_macros)]

use std::future::Future;

use sqlx::{Database as Backend, Pool};

#[cfg(feature = "mysql")]
pub(crate) mod mysql;
#[cfg(feature = "postgres")]
pub(crate) mod postgres;
// The shared rows keep times and JSON headers; a suite of other column types runs without them.
#[cfg(all(feature = "chrono", feature = "json"))]
pub(crate) mod rows;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

/// The MariaDB stand: a server of its own, which the MySQL stand's SQL reads and writes.
#[cfg(feature = "mysql")]
pub(crate) mod mariadb {
    pub(crate) use super::mysql::Db;

    use ruststream_sqlx::dialect;

    use super::Database;

    /// The variable that names the stand: a MariaDB URL whose user may create databases.
    pub(crate) const URL: &str = "MARIADB_TEST_URL";

    /// The dialect the broker builds the stand's statements with: the MySQL stand's.
    pub(crate) const DIALECT: dialect::MySql = super::mysql::DIALECT;

    /// A fresh database on the stand, or `None` to skip the test.
    pub(crate) async fn database() -> Option<Database<Db>> {
        super::mysql::database_on(URL).await
    }
}

/// The variable a job sets to say it stood its databases up, so skipping past one is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The stand's URL that `variable` names, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and `variable` is not: a job that started a stand and lost
/// its address is a broken job, and the tests behind it would pass without running.
pub(crate) fn url(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {variable} is unset or empty"
            );
            eprintln!("{variable} is not set; skipping the live suite");
            None
        }
    }
}

/// Awaits `read` on a running tokio clock, then pauses the clock again.
///
/// A paused clock jumps to its next timer whenever the runtime waits, and a reply on its way from
/// the database is such a wait, so the pool's acquire timeout can fire under a read. The broker's
/// in-process connection keeps its own calls off a paused clock; a test reads its tables through
/// here.
pub(crate) async fn unpaused<T>(read: impl Future<Output = T>) -> T {
    tokio::time::resume();
    let value = read.await;
    tokio::time::pause();
    value
}

/// A database of the test's own on a stand, with the stand's schema applied.
///
/// Its stand's module creates it, drops it in `finish` and reads its tables.
pub(crate) struct Database<DB: Backend> {
    /// A pool on the database, the one the test's broker takes.
    pub(crate) pool: Pool<DB>,
    /// The database's name on the stand.
    name: String,
    /// The stand's URL, which `finish` connects to again to drop the database.
    url: String,
    /// A connection the stand holds open beside the pool, where the database lives only while one
    /// is open.
    keeper: Option<DB::Connection>,
}

/// One module per stand and per form of the queue rows, each holding `$items`.
///
/// A stand appears when its feature is on; the rows of a form appear when the form exists on that
/// stand. Each module sees the suite's own items, the stand's `Db`, `database` and `DIALECT`, and
/// the rows of its form.
macro_rules! matrix {
    ($($items:item)*) => {
        $crate::live::fifo_matrix! { $($items)* }

        #[cfg(feature = "postgres")]
        mod postgres_advisory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mysql_advisory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb_advisory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }

        #[cfg(feature = "sqlite")]
        mod sqlite_advisory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }
    };
}

/// One module per stand and per form whose claim statement takes a FIFO group's head, the row lock
/// and the lease form, each holding `$items`: for a test that runs that statement itself.
///
/// A stand appears when its feature is on; SQLite has no row locks, so it runs the lease form
/// alone. Each module sees the suite's own items, the stand's `Db`, `database` and `DIALECT`, and
/// the rows of its form.
macro_rules! fifo_matrix {
    ($($items:item)*) => {
        $crate::live::server_matrix! { $($items)* }

        #[cfg(feature = "sqlite")]
        mod sqlite_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }
    };
}

/// One module per stand that runs as a server and per form whose claim statement takes a FIFO
/// group's head, each holding `$items`: for a test of what such claims on separate connections do
/// side by side, which SQLite's one writer keeps apart.
///
/// A stand appears when its feature is on. Each module sees the suite's own items, the stand's
/// `Db`, `database` and `DIALECT`, and the rows of its form.
macro_rules! server_matrix {
    ($($items:item)*) => {
        #[cfg(feature = "postgres")]
        mod postgres_row_lock {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::row_lock::*;
            $($items)*
        }

        #[cfg(feature = "postgres")]
        mod postgres_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mysql_row_lock {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::row_lock::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mysql_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb_row_lock {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::row_lock::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }
    };
}

/// One module per stand, each holding `$items`: for a test whose rows name their own form, one
/// every stand serves.
///
/// A stand appears when its feature is on. Each module sees the suite's own items and the stand's
/// `Db` and `database`.
macro_rules! stands {
    ($($items:item)*) => {
        $crate::live::row_lock_stands! { $($items)* }

        #[cfg(feature = "sqlite")]
        mod sqlite {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::{Db, database};
            $($items)*
        }
    };
}

/// One module per stand of the lease form, each holding `$items`: for a test of what a lease does.
///
/// A stand appears when its feature is on. Each module sees the suite's own items, the stand's
/// `Db`, `database` and `DIALECT`, and the rows of the lease form.
macro_rules! lease_stands {
    ($($items:item)*) => {
        #[cfg(feature = "postgres")]
        mod postgres_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mysql_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }

        #[cfg(feature = "sqlite")]
        mod sqlite_lease {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::lease::*;
            $($items)*
        }
    };
}

/// One module per stand of the advisory lock form's suite, each holding `$items`.
///
/// A stand appears when its feature is on. Each module sees the suite's own items, the stand's
/// `Db`, `database` and `DIALECT`, and the rows of the advisory lock form.
macro_rules! advisory_stands {
    ($($items:item)*) => {
        $crate::live::advisory_server_stands! { $($items)* }

        #[cfg(feature = "sqlite")]
        mod sqlite {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::sqlite::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }
    };
}

/// One module per stand of the advisory lock form that runs as a server, each holding `$items`:
/// the stands whose databases keep the locks.
///
/// A stand appears when its feature is on. Each module sees the suite's own items, the stand's
/// `Db`, `database` and `DIALECT`, and the rows of the advisory lock form.
macro_rules! advisory_server_stands {
    ($($items:item)*) => {
        #[cfg(feature = "postgres")]
        mod postgres {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mysql {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{DIALECT, Db, database};
            #[allow(unused_imports)]
            use crate::live::rows::advisory::*;
            $($items)*
        }
    };
}

/// One module per stand whose database locks rows, each holding `$items`: for a test whose rows
/// take the row lock form, which SQLite does not serve.
///
/// A stand appears when its feature is on. Each module sees the suite's own items and the stand's
/// `Db` and `database`.
macro_rules! row_lock_stands {
    ($($items:item)*) => {
        #[cfg(feature = "postgres")]
        mod postgres {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::postgres::{Db, database};
            $($items)*
        }

        $crate::live::mysql_stands! { $($items)* }
    };
}

/// One module per stand that speaks MySQL, each holding `$items`: for a test of what MySQL and
/// MariaDB do alike.
///
/// The stands appear with the `mysql` feature. Each module sees the suite's own items and the
/// stand's `Db` and `database`.
macro_rules! mysql_stands {
    ($($items:item)*) => {
        #[cfg(feature = "mysql")]
        mod mysql {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mysql::{Db, database};
            $($items)*
        }

        #[cfg(feature = "mysql")]
        mod mariadb {
            #[allow(unused_imports)]
            use super::*;
            #[allow(unused_imports)]
            use crate::live::mariadb::{Db, database};
            $($items)*
        }
    };
}

#[allow(unused_imports)]
pub(crate) use {
    advisory_server_stands, advisory_stands, fifo_matrix, lease_stands, matrix, mysql_stands,
    row_lock_stands, server_matrix, stands,
};
