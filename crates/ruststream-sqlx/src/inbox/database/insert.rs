//! The plumbing of the insert the derive generates: the statements it builds at compile time, one
//! per built-in dialect, and the connection that picks one.

#[cfg(feature = "any")]
use sqlx::Any;
#[cfg(feature = "any")]
use sqlx::AnyConnection;
use sqlx::{Database, Error};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};

use super::QueueDatabase;
#[cfg(feature = "any")]
use super::built_in::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

/// The inserts the derive builds at compile time, one per built-in dialect it was built with.
/// Machinery; the derive writes it, never a service.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct InsertSql<'s> {
    /// The Postgres insert.
    pub postgres: Option<&'s str>,
    /// The MySQL and MariaDB insert.
    pub mysql: Option<&'s str>,
    /// The SQLite insert.
    pub sqlite: Option<&'s str>,
}

#[cfg(feature = "any")]
impl<'s> InsertSql<'s> {
    /// The insert of the database behind the `AnyPool` backend named `backend`.
    fn for_backend(&self, backend: &str) -> Option<&'s str> {
        match backend {
            POSTGRES_BACKEND => self.postgres,
            MYSQL_BACKEND => self.mysql,
            SQLITE_BACKEND => self.sqlite,
            _ => None,
        }
    }
}

/// A connection the inserts the derive builds at compile time run on: its database, and which
/// of the derive's statements it runs. Machinery; never named directly.
#[doc(hidden)]
pub trait OnConnection: Send {
    /// The database the connection reaches.
    type Database: QueueDatabase;

    /// The connection itself.
    fn connection(&mut self) -> &mut <Self::Database as Database>::Connection;

    /// The insert of this connection's database among `sql`, or `None` when the derive built none
    /// for it.
    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str>;
}

#[cfg(feature = "postgres")]
impl OnConnection for PgConnection {
    type Database = Postgres;

    fn connection(&mut self) -> &mut Self {
        self
    }

    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.postgres
    }
}

#[cfg(feature = "mysql")]
impl OnConnection for MySqlConnection {
    type Database = MySql;

    fn connection(&mut self) -> &mut Self {
        self
    }

    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.mysql
    }
}

#[cfg(feature = "sqlite")]
impl OnConnection for SqliteConnection {
    type Database = Sqlite;

    fn connection(&mut self) -> &mut Self {
        self
    }

    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.sqlite
    }
}

#[cfg(feature = "any")]
impl OnConnection for AnyConnection {
    type Database = Any;

    fn connection(&mut self) -> &mut Self {
        self
    }

    /// The insert of the database the connection reaches: `Any` passes a statement to its
    /// backend as written.
    fn insert_sql<'s>(&self, sql: &InsertSql<'s>) -> Option<&'s str> {
        sql.for_backend(self.backend_name())
    }
}

/// The error of a generated insert on a connection whose database the derive built no statement
/// for. Machinery; the derive calls it.
#[doc(hidden)]
#[must_use]
pub fn no_insert(row: &'static str) -> Error {
    Error::Configuration(
        format!("`{row}` has no generated insert for this connection's database").into(),
    )
}

#[cfg(all(test, feature = "any"))]
mod tests {
    use super::InsertSql;
    use crate::inbox::database::built_in::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

    #[test]
    fn an_any_backend_runs_the_insert_of_its_database() {
        let sql = InsertSql {
            postgres: Some("INSERT .. ($1)"),
            mysql: Some("INSERT .. (?)"),
            sqlite: None,
        };
        assert_eq!(sql.for_backend(POSTGRES_BACKEND), Some("INSERT .. ($1)"));
        assert_eq!(sql.for_backend(MYSQL_BACKEND), Some("INSERT .. (?)"));
        assert_eq!(
            sql.for_backend(SQLITE_BACKEND),
            None,
            "the derive was built without that dialect"
        );
        assert_eq!(sql.for_backend("MSSQL"), None);
    }
}
