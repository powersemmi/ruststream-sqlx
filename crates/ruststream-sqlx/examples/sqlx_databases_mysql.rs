//! An email queue in a MySQL or MariaDB table in the lease form, with the index its claims read in
//! order: each claim reads the rows of its own group alone.
//!
//! ```text
//! DATABASE_URL=mysql://user:password@localhost/mail \
//!     cargo run -p ruststream-sqlx --example sqlx_databases_mysql --features inbox,mysql,chrono,testing
//! ```

use std::env;
use std::error::Error;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySql, MySqlConnection, MySqlPool};

use ruststream_sqlx::prelude::*;

// --8<-- [start:service]
/// One row of `welcome_jobs`, claimed under a lease. The claim stamps `locked_until` with a whole
/// second, so a `DATETIME` column without fractions holds it.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "welcome_jobs")]
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

/// The claim order, `(group, id)` for this table's columns, keeps each claim to its group's rows.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS welcome_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    locked_until DATETIME,
    payload BLOB NOT NULL,
    INDEX claim_order (name, id)
)";

#[subscriber(InboxQueue::<SendEmail>::new("welcome"))]
async fn welcome(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending the welcome");
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<SendEmail>::new("receipts"))]
async fn receipt(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending the receipt");
    HandlerOutcome::ack()
}

fn service(pool: MySqlPool) -> RustStream {
    let broker = SqlxBroker::new(pool)
        .route::<SendEmail>("welcome")
        .route::<SendEmail>("receipts");
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(broker, |b| {
        b.include(welcome);
        b.include(receipt);
    })
}
// --8<-- [end:service]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = MySqlPool::connect(&env::var("DATABASE_URL")?).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

/// A publish writes a row into the group it names.
impl Publish<MySql> for SendEmail {
    async fn publish(
        conn: &mut MySqlConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO welcome_jobs (name, payload) VALUES (?, ?)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Email {
    to: String,
}

// The live suites' gate: each test runs on a database of its own on the MySQL stand that
// `MYSQL_TEST_URL` names or the MariaDB stand that `MARIADB_TEST_URL` names, and skips without it.
#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::MySql;

    use super::live::{Database, mariadb, mysql};
    use super::{Email, SCHEMA, service};

    /// The body both stands run: an email reaches the subscription of its group alone.
    async fn an_email_reaches_its_group(db: Database<MySql>) -> Result<(), Box<dyn Error>> {
        sqlx::raw_sql(SCHEMA).execute(&db.pool).await?;
        let tb = TestApp::start(service(db.pool.clone())).await?;
        let email = Email {
            to: "ada@example.com".to_owned(),
        };
        tb.broker::<SqlxBroker<MySql>>()
            .message(&email)
            .to("receipts")
            .publish()
            .await?;

        tb.broker::<SqlxBroker<MySql>>()
            .subscriber("receipts")
            .assert_called_once()
            .with(&email)
            .settled(HandlerOutcome::ack());
        tb.broker::<SqlxBroker<MySql>>()
            .subscriber("welcome")
            .assert_not_called();
        tb.shutdown().await?;
        db.finish().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_mysql() -> Result<(), Box<dyn Error>> {
        let Some(db) = mysql::database().await else {
            return Ok(());
        };
        an_email_reaches_its_group(db).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn on_mariadb() -> Result<(), Box<dyn Error>> {
        let Some(db) = mariadb::database().await else {
            return Ok(());
        };
        an_email_reaches_its_group(db).await
    }
}
