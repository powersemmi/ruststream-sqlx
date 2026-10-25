//! A fetch of the service's own: each claimed job is read joined to its order.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_headers_fetch --features inbox,sqlite,chrono,json,testing
//! ```

use chrono::{DateTime, Utc};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

#[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
#[inbox(table = "mail_jobs")]
struct MailHeaders {
    #[field(id, generated)]
    job_id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    order_id: i64,
}

// --8<-- [start:fetch]
use ruststream_sqlx::Fetch;
use sqlx::types::Json;
use sqlx::{Sqlite, SqliteConnection};

/// A job and the order it is for, read by the service's own fetch over a join.
#[derive(Debug, Clone, Inbox, sqlx::FromRow)]
#[inbox(custom(fetch))]
struct OrderMail {
    #[field(headers)]
    #[sqlx(flatten)]
    headers: MailHeaders,
    customer: String,
    total: i64,
}

impl Fetch<Sqlite> for OrderMail {
    async fn fetch(conn: &mut SqliteConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
        // SQLite takes a list of ids as a JSON array, read back by `json_each`.
        sqlx::query_as(
            "SELECT j.job_id, j.locked_until, j.order_id, o.customer, o.total \
             FROM mail_jobs j JOIN orders o ON o.id = j.order_id \
             WHERE j.job_id IN (SELECT value FROM json_each(?))",
        )
        .bind(Json(ids))
        .fetch_all(conn)
        .await
    }
}
// --8<-- [end:fetch]

#[subscriber(InboxQueue::<OrderMail>::new("receipts"))]
async fn send_receipt(mail: &OrderMail) -> HandlerOutcome {
    tracing::info!(
        order = mail.headers.order_id,
        customer = %mail.customer,
        total = mail.total,
        "sending the receipt"
    );
    HandlerOutcome::ack()
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    RustStream::new(AppInfo::new("shop", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            // A job whose order is gone joins no row: the default decode policy drops it.
            b.include(send_receipt);
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:shop.db?mode=rwc")
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS mail_jobs (
    job_id INTEGER PRIMARY KEY,
    locked_until TEXT,
    order_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer TEXT NOT NULL,
    total INTEGER NOT NULL
);
";

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{OrderMail, SCHEMA, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool it reads the table through.
    async fn database(
        name: &str,
    ) -> Result<(String, SqlitePool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let options: SqliteConnectOptions = url.parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    // A row the service writes itself reaches the harness's books once a claim takes it, so the
    // test runs on the real clock and lets the claim happen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_handler_takes_the_joined_row_and_a_job_without_its_order_drops()
    -> Result<(), Box<dyn Error>> {
        let (url, pool, _keeper) = database("sqlx_headers_fetch").await?;
        sqlx::raw_sql(
            "INSERT INTO orders (id, customer, total) VALUES (7, 'ada', 1200); \
             INSERT INTO mail_jobs (order_id) VALUES (7);",
        )
        .execute(&pool)
        .await?;
        let tb = TestApp::start_live(service(&url)).await?;
        tb.advance(Duration::from_millis(500)).await?;
        let lent = tb
            .broker::<SqlxBroker<Sqlite>>()
            .subscriber("receipts")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .received_values::<OrderMail>();
        let joined: Vec<(&str, i64)> = lent
            .iter()
            .map(|mail| (mail.customer.as_str(), mail.total))
            .collect();
        assert_eq!(
            joined,
            [("ada", 1200)],
            "the job of order 7 joined its order"
        );

        // A job for an order that is gone joins no row.
        sqlx::query("INSERT INTO mail_jobs (order_id) VALUES (8)")
            .execute(&pool)
            .await?;
        tb.advance(Duration::from_secs(2)).await?;
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("receipts")
            .assert_called(2)
            .settled(HandlerOutcome::drop())
            .assert_last_failed_to_decode();
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM mail_jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            remaining, 0,
            "the acknowledgement and the decode policy settled both jobs"
        );
        Ok(())
    }
}
