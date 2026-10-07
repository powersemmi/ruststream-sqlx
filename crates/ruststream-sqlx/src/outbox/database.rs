//! The databases the outbox's default statements run on, and the statement each picks among the
//! ones `#[derive(Outbox)]` builds at compile time.

use std::future::Future;

#[cfg(feature = "any")]
use sqlx::Any;
#[cfg(feature = "any")]
use sqlx::AnyConnection;
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
use sqlx::Arguments as _;
use sqlx::{Database, Error, FromRow};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};
#[cfg(feature = "sqlite")]
use sqlx::{Sqlite, SqliteConnection};

/// The texts of one default statement of an outbox record, one per built-in dialect the derive
/// was built with. Machinery; the derive writes it, never a service.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OutboxSql<'s> {
    /// The Postgres text.
    pub postgres: Option<&'s str>,
    /// The MySQL and MariaDB text.
    pub mysql: Option<&'s str>,
    /// The SQLite text.
    pub sqlite: Option<&'s str>,
}

/// The name `AnyConnection::backend_name` gives each built-in database, as sqlx names it.
#[cfg(feature = "any")]
const POSTGRES_BACKEND: &str = "PostgreSQL";
#[cfg(feature = "any")]
const MYSQL_BACKEND: &str = "MySQL";
#[cfg(feature = "any")]
const SQLITE_BACKEND: &str = "SQLite";

#[cfg(feature = "any")]
impl<'s> OutboxSql<'s> {
    /// The text of the database behind the `AnyPool` backend named `backend`.
    fn for_backend(&self, backend: &str) -> Option<&'s str> {
        match backend {
            POSTGRES_BACKEND => self.postgres,
            MYSQL_BACKEND => self.mysql,
            SQLITE_BACKEND => self.sqlite,
            _ => None,
        }
    }
}

/// A sqlx database the default events of an outbox record run on: `#[derive(Outbox)]` builds
/// their statements for each built-in dialect at compile time, and the connection's database picks
/// its own.
///
/// [`Postgres`](sqlx::Postgres), [`MySql`](sqlx::MySql) (MySQL and MariaDB) and
/// [`Sqlite`](sqlx::Sqlite) implement it under their features, and [`Any`](sqlx::Any) under `any`,
/// where the database the connection reached picks the text. A record's default event on a
/// database the derive built no statement for fails with [`sqlx::Error::Configuration`] naming the
/// record and the event.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::{Outbox, OutboxDatabase, outbox};
/// use sqlx::{PgConnection, Postgres};
///
/// // outbox: id BIGSERIAL PRIMARY KEY, name TEXT, payload BYTEA; a processed record is deleted
/// #[derive(Outbox, sqlx::FromRow)]
/// #[outbox(table = "outbox")]
/// pub struct OrderEvent {
///     #[field(id)]
///     id: i64,
///     #[field(name)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// // The record of a published message is the service's own statement.
/// impl outbox::Publish<Postgres> for OrderEvent {
///     async fn publish(
///         conn: &mut PgConnection,
///         msg: &OutgoingMessage<'_>,
///     ) -> Result<i64, sqlx::Error> {
///         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
///             .bind(msg.name())
///             .bind(msg.payload())
///             .fetch_one(conn)
///             .await
///     }
/// }
///
/// // The default events run on any `OutboxDatabase`: here the mark of a processed record.
/// pub async fn mark<DB: OutboxDatabase>(
///     conn: &mut DB::Connection,
///     id: &i64,
/// ) -> Result<(), sqlx::Error>
/// where
///     OrderEvent: outbox::Ack<DB>,
/// {
///     <OrderEvent as outbox::Ack<DB>>::ack(conn, id).await
/// }
/// # }
/// # fn main() {}
/// ```
pub trait OutboxDatabase: Database {
    /// The text of `sql` this connection's database runs, or `None` when the derive built none for
    /// it. Machinery.
    #[doc(hidden)]
    fn statement<'s>(conn: &Self::Connection, sql: &OutboxSql<'s>) -> Option<&'s str>;

    /// Binds the name a recovery reads the records of. Machinery.
    #[doc(hidden)]
    fn bind_name(arguments: &mut Self::Arguments, name: &str) -> Result<(), Error>;

    /// Runs a statement. Machinery.
    #[doc(hidden)]
    fn execute<'c>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Runs a statement that reads at most one record. Machinery.
    #[doc(hidden)]
    fn fetch_optional<'c, Record>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Option<Record>, Error>> + Send + 'c
    where
        Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c;

    /// Runs a statement that reads every record it selects. Machinery.
    #[doc(hidden)]
    fn fetch_all<'c, Record>(
        conn: &'c mut Self::Connection,
        sql: &'static str,
        arguments: Self::Arguments,
    ) -> impl Future<Output = Result<Vec<Record>, Error>> + Send + 'c
    where
        Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c;
}

/// `OutboxDatabase` for one sqlx database, whose connection picks the text `$pick` names.
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite",
    feature = "any"
))]
macro_rules! outbox_database {
    ($database:ty, $connection:ty, |$conn:ident, $sql:ident| $pick:expr) => {
        impl OutboxDatabase for $database {
            fn statement<'s>($conn: &$connection, $sql: &OutboxSql<'s>) -> Option<&'s str> {
                $pick
            }

            fn bind_name(arguments: &mut Self::Arguments, name: &str) -> Result<(), Error> {
                arguments.add(name).map_err(Error::Encode)
            }

            async fn execute(
                conn: &mut $connection,
                sql: &'static str,
                arguments: Self::Arguments,
            ) -> Result<(), Error> {
                sqlx::query_with(sql, arguments).execute(conn).await?;
                Ok(())
            }

            async fn fetch_optional<'c, Record>(
                conn: &'c mut $connection,
                sql: &'static str,
                arguments: Self::Arguments,
            ) -> Result<Option<Record>, Error>
            where
                Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c,
            {
                sqlx::query_as_with(sql, arguments)
                    .fetch_optional(conn)
                    .await
            }

            async fn fetch_all<'c, Record>(
                conn: &'c mut $connection,
                sql: &'static str,
                arguments: Self::Arguments,
            ) -> Result<Vec<Record>, Error>
            where
                Record: for<'r> FromRow<'r, Self::Row> + Send + Unpin + 'c,
            {
                sqlx::query_as_with(sql, arguments).fetch_all(conn).await
            }
        }
    };
}

#[cfg(feature = "postgres")]
outbox_database!(Postgres, PgConnection, |_conn, sql| sql.postgres);
#[cfg(feature = "mysql")]
outbox_database!(MySql, MySqlConnection, |_conn, sql| sql.mysql);
#[cfg(feature = "sqlite")]
outbox_database!(Sqlite, SqliteConnection, |_conn, sql| sql.sqlite);
// `Any` passes a statement to its backend as written, so the backend picks the text.
#[cfg(feature = "any")]
outbox_database!(Any, AnyConnection, |conn, sql| sql
    .for_backend(conn.backend_name()));

/// The error of a default event on a connection whose database the derive built no statement for.
/// Machinery; the derive calls it.
#[doc(hidden)]
#[must_use]
pub fn no_outbox_statement(record: &'static str, event: &'static str) -> Error {
    Error::Configuration(
        format!("`{record}` has no generated {event} statement for this connection's database")
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::no_outbox_statement;

    #[test]
    fn the_missing_statement_names_the_record_and_the_event() {
        assert_eq!(
            no_outbox_statement("OrderEvent", "fetch").to_string(),
            "error with configuration: `OrderEvent` has no generated fetch statement for this \
             connection's database"
        );
    }

    #[cfg(feature = "any")]
    #[test]
    fn an_any_backend_runs_the_text_of_its_database() {
        use super::{MYSQL_BACKEND, OutboxSql, POSTGRES_BACKEND, SQLITE_BACKEND};

        let sql = OutboxSql {
            postgres: Some("SELECT .. $1"),
            mysql: Some("SELECT .. ?"),
            sqlite: None,
        };
        assert_eq!(sql.for_backend(POSTGRES_BACKEND), Some("SELECT .. $1"));
        assert_eq!(sql.for_backend(MYSQL_BACKEND), Some("SELECT .. ?"));
        assert_eq!(sql.for_backend(SQLITE_BACKEND), None);
        assert_eq!(sql.for_backend("MSSQL"), None);
    }

    #[cfg(all(
        feature = "any",
        feature = "postgres",
        feature = "mysql",
        feature = "sqlite"
    ))]
    #[test]
    fn the_backend_names_are_sqlxs() {
        use sqlx::Database;

        use super::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};

        assert_eq!(POSTGRES_BACKEND, <sqlx::Postgres as Database>::NAME);
        assert_eq!(MYSQL_BACKEND, <sqlx::MySql as Database>::NAME);
        assert_eq!(SQLITE_BACKEND, <sqlx::Sqlite as Database>::NAME);
    }
}
