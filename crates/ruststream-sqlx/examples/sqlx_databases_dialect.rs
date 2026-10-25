//! A dialect of the service's own over SQLite: it wraps the built-in SQLite dialect, writes its
//! own acknowledgement, and leaves every other statement to the built-in one.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_databases_dialect --features inbox,sqlite,chrono,testing
//! ```

use std::error::Error;
use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:dialect]
/// One row of `email_jobs`, claimed under a lease.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct SendEmail {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

use ruststream_sqlx::dialect::{
    self, ClaimShape, Dialect, Lease, Opening, Param, Role, Statement, StatementError, TableName,
    TableSpec,
};

/// The group an acknowledged row moves to.
const ACKNOWLEDGED: &str = "acknowledged";

/// SQLite, with an acknowledgement of the service's own: an acknowledged row of a table with
/// groups moves to the `acknowledged` group and stays there for an audit.
#[derive(Debug)]
struct Audited;

impl Dialect for Audited {
    fn name(&self) -> &'static str {
        "audited"
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let Some(group) = spec.column(Role::Group) else {
            return dialect::Sqlite.ack(spec);
        };
        let mut sql = String::from("UPDATE ");
        self.quote_into(spec.table(), &mut sql);
        sql.push_str(" SET ");
        self.quote_into(group.name(), &mut sql);
        sql.push_str(" = '");
        sql.push_str(ACKNOWLEDGED);
        sql.push_str("' WHERE ");
        self.quote_into(spec.id().name(), &mut sql);
        sql.push_str(" = ");
        self.placeholder_into(NonZeroUsize::MIN, &mut sql);
        Ok(Statement::new(sql, [Param::Id]))
    }

    // Every other statement is the built-in dialect's, the provided methods it overrides included.
    fn quote_into(&self, ident: &str, out: &mut String) {
        dialect::Sqlite.quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        dialect::Sqlite.placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.fetch(spec)
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        dialect::Sqlite.retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        dialect::Sqlite.dead_letter_table(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.insert(spec)
    }

    fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
        dialect::Sqlite.begin(opening)
    }
}

// The lease form, which `SendEmail` takes: its claim, the extension of a lease in work and the
// stamp of a claimed row.
impl Lease for Audited {
    fn lease_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::Sqlite.lease_claim(spec, shape)
    }

    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.extend(spec)
    }

    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Sqlite.stamp(spec)
    }

    fn claim_counts_attempt(&self, spec: &TableSpec<'_>) -> bool {
        dialect::Sqlite.claim_counts_attempt(spec)
    }

    fn begin_lease_claim(&self) -> Option<&'static str> {
        dialect::Sqlite.begin_lease_claim()
    }
}

// A `SqlxBroker<Sqlite, Audited>`: the dialect is part of the broker's type.
fn broker(pool: SqlitePool) -> SqlxBroker<Sqlite, Audited> {
    SqlxBroker::with_dialect(pool, Audited).route::<SendEmail>("emails")
}
// --8<-- [end:dialect]

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

fn service(broker: SqlxBroker<Sqlite, Audited>) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
        b.include(send);
    })
}

// The broker on a dialect of the service's own takes a pool the service builds, so `main` builds
// it inside the runtime and closes it after the app.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect("sqlite:mail.db?mode=rwc").await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(broker(pool.clone())).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS email_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

/// A publish writes a row into the group it names.
impl Publish<Sqlite> for SendEmail {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO email_jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "emails")]
struct Email {
    to: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{ACKNOWLEDGED, Audited, Email, SCHEMA, broker, service};

    // The crate registers the brokers of its built-in dialects with the harness; a broker on a
    // dialect of the service's own is registered by the service.
    ruststream::register_testable_broker!(SqlxBroker<Sqlite, Audited>);

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool.
    async fn database(name: &str) -> Result<(SqlitePool, SqliteConnection), Box<dyn Error>> {
        let options: SqliteConnectOptions =
            format!("sqlite:file:{name}?mode=memory&cache=shared").parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_email_moves_to_its_audit_group() -> Result<(), Box<dyn Error>> {
        let (pool, _keeper) = database("sqlx_databases_dialect").await?;
        let tb = TestApp::start(service(broker(pool.clone()))).await?;
        let email = Email {
            to: "ops@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<Sqlite, Audited>>()
            .message(&email)
            .publish()
            .await?;

        tb.broker::<SqlxBroker<Sqlite, Audited>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&email)
            .settled(HandlerOutcome::ack());
        let groups: Vec<String> = sqlx::query_scalar("SELECT name FROM email_jobs")
            .fetch_all(&pool)
            .await?;
        assert_eq!(groups, [ACKNOWLEDGED], "the row stays, in the audit group");
        Ok(())
    }
}
