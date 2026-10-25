//! What a broker reports of its server to the generated document: the host and port its URL
//! names, never its credentials.

use ruststream::{DescribeServer, ServerSpec};
use ruststream_sqlx_dialect::Dialect;
#[cfg(feature = "any")]
use sqlx::Any;
#[cfg(feature = "mysql")]
use sqlx::MySql;
#[cfg(feature = "postgres")]
use sqlx::Postgres;
#[cfg(feature = "sqlite")]
use sqlx::Sqlite;

use super::SqlxBroker;

#[cfg(feature = "postgres")]
impl<D: Dialect + 'static> DescribeServer for SqlxBroker<Postgres, D> {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(&self.pool.url(), "postgres")
    }
}

#[cfg(feature = "mysql")]
impl<D: Dialect + 'static> DescribeServer for SqlxBroker<MySql, D> {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(&self.pool.url(), "mysql")
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
        let url = self.pool.url();
        let scheme = url.split_once(':').map_or(&*url, |(scheme, _)| scheme);
        match scheme {
            "sqlite" => ServerSpec::in_process("sqlite"),
            "postgres" | "postgresql" => ServerSpec::from_url(&url, "postgres"),
            "mysql" | "mariadb" => ServerSpec::from_url(&url, "mysql"),
            scheme => ServerSpec::from_url(&url, scheme),
        }
    }
}
