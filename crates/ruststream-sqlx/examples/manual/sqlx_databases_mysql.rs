//! The service of `examples/sqlx_databases_mysql.rs` without the macros: the table is described
//! with `InboxSpec`, and one handler type serves both groups.
//!
//! ```text
//! DATABASE_URL=mysql://user:password@localhost/mail \
//!     cargo run -p ruststream-sqlx --example manual_sqlx_databases_mysql --features inbox,mysql,chrono,testing,asyncapi
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use sqlx::mysql::{MySql, MySqlConnection, MySqlPoolOptions};

use ruststream_sqlx::prelude::*;

// --8<-- [start:service]
/// One row of `welcome_jobs`, claimed under a lease. The claim stamps `locked_until` with a whole
/// second, so a `DATETIME` column without fractions holds it.
#[derive(sqlx::FromRow)]
struct SendEmail {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("welcome_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SendEmail {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The claim order, `(group, id)` for this table's columns, keeps each claim to its group's rows.
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS welcome_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    locked_until DATETIME,
    payload BLOB NOT NULL,
    INDEX claim_order (name, id)
)";

struct Mail;

impl Handle<Email> for Mail {
    fn handle(
        &self,
        email: &Email,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %email.to, group = ctx.name(), "sending");
        ready(Ok(()))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, MySqlPoolOptions::new())
        .route::<SendEmail>("welcome")
        .route::<SendEmail>("receipts");
    RustStream::new(AppInfo::new("mailer", "0.1.0"))
        // The table, created before the subscriptions check it.
        .on_startup(async move |()| {
            let mut conn = MySqlConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            for group in ["welcome", "receipts"] {
                let mail = subscriber(group, Mail)
                    .map_source(move |_| InboxQueue::<SendEmail>::new(group));
                b.include(mail.build());
            }
        })
}
// --8<-- [end:service]

fn main() -> ExitCode {
    let url = env::var("DATABASE_URL").unwrap_or_else(|_| "mysql://localhost/mail".to_owned());
    run_main(|| service(&url))
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Email {
    to: String,
}

// The live suites' gate: each test runs on a database of its own on the MySQL stand that
// `MYSQL_TEST_URL` names or the MariaDB stand that `MARIADB_TEST_URL` names, and skips without it.
#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
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
        let tb = TestApp::start(service(&db.url())).await?;
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
