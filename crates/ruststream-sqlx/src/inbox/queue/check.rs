//! The startup check of a subscription: the server's version where its dialect asks for one, and
//! each of its statements prepared.

use std::sync::Arc;

use ruststream_sqlx_dialect::{Dialect, StatementError, TableSpec};
use sqlx::Pool;

use crate::inbox::FormDialect;
use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{Prepared, Stmt};
use crate::inbox::error::SqlxBrokerError;
#[cfg(feature = "testing")]
use crate::inbox::testing::{cancelled, off_clock};

/// Why the startup check failed: no connection, a version the server did not report, a server
/// the dialect refuses, or a statement the server refused.
pub(super) enum Unchecked {
    Acquire(sqlx::Error),
    Version(&'static str, sqlx::Error),
    Server(StatementError),
    Statement(&'static str, sqlx::Error),
}

impl Unchecked {
    /// The error of the subscription `name` to `table`, read as `row`.
    pub(super) fn named(self, name: &str, table: &str, row: &'static str) -> SqlxBrokerError {
        let (subscription, table) = (name.to_owned(), table.to_owned());
        match self {
            Self::Acquire(source) => SqlxBrokerError::Sqlx {
                subscription,
                table,
                row,
                statement: "acquire",
                source: Box::new(source),
            },
            Self::Version(statement, source) => SqlxBrokerError::Sqlx {
                subscription,
                table,
                row,
                statement,
                source: Box::new(source),
            },
            Self::Server(StatementError::ServerTooOld {
                server, required, ..
            }) => SqlxBrokerError::ServerTooOld {
                subscription,
                table,
                row,
                server,
                required,
            },
            Self::Server(source) => SqlxBrokerError::Dialect {
                subscription,
                table,
                row,
                source,
            },
            Self::Statement(statement, source) => SqlxBrokerError::Schema {
                subscription,
                table,
                row,
                statement,
                source: Box::new(source),
            },
        }
    }
}

/// The startup check: on one connection of the pool, the server's version where the dialect `form`
/// shows asks for it, then each of `prepared`'s statements prepared, off a paused clock where the
/// connection runs in process.
pub(super) async fn check<DB: QueueDatabase>(
    shared: &Arc<Shared<DB>>,
    form: &FormDialect,
    spec: &TableSpec<'static>,
    prepared: &Prepared,
) -> Result<(), Unchecked> {
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        let shared = Arc::clone(shared);
        let form = form.clone();
        let spec = *spec;
        let statements: Vec<Stmt> = prepared.statements().collect();
        return off_clock(async move {
            verify(&shared.pool, form.dialect(), &spec, statements.into_iter()).await
        })
        .await
        .unwrap_or_else(|| Err(Unchecked::Acquire(cancelled())));
    }
    verify(&shared.pool, form.dialect(), spec, prepared.statements()).await
}

/// Checks the server's version against `dialect`'s floor for `spec`, where the dialect has one,
/// then prepares each of `statements`, on one connection of `pool`.
async fn verify<DB: QueueDatabase>(
    pool: &Pool<DB>,
    dialect: &dyn Dialect,
    spec: &TableSpec<'_>,
    statements: impl Iterator<Item = Stmt>,
) -> Result<(), Unchecked> {
    let mut conn = pool.acquire().await.map_err(Unchecked::Acquire)?;
    if let Some(query) = dialect.server_version() {
        let version = DB::fetch_text(&mut conn, query)
            .await
            .map_err(|source| Unchecked::Version(query, source))?;
        dialect
            .check_server(spec, &version)
            .map_err(Unchecked::Server)?;
    }
    for statement in statements {
        DB::prepare(&mut conn, statement.sql)
            .await
            .map_err(|source| Unchecked::Statement(statement.sql, source))?;
    }
    Ok(())
}
