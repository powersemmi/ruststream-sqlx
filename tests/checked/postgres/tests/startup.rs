//! The statements the broker prepares when a checked struct's subscription opens are the texts
//! `cargo sqlx prepare` captured from its derive, byte for byte: each subscription opens on a
//! pool of one connection, and the server lists what that connection prepared.
//!
//! The test runs against the stand named by `POSTGRES_TEST_URL`, in a database of its own; it
//! skips without one unless `RUSTSTREAM_REQUIRE_LIVE=1`.

#![cfg(feature = "checked")]

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::Path;

use ruststream::{Broker, ConnectedBroker, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use ruststream_sqlx_checked_postgres::{Email, Entry, Order, Reminder, SendEmail, Webhook};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool};

/// The texts the fixture's `.sqlx` data holds.
fn checked() -> BTreeSet<String> {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join(".sqlx");
    fs::read_dir(data)
        .expect("the offline data is committed")
        .map(|entry| {
            let text = fs::read_to_string(entry.expect("an entry reads").path())
                .expect("a query file reads");
            let query: serde_json::Value =
                serde_json::from_str(&text).expect("a query file parses");
            query["query"]
                .as_str()
                .expect("a query file names its text")
                .to_owned()
        })
        .collect()
}

/// A database of its own on the stand, with the fixture's tables, and its name; none without a
/// stand.
async fn database() -> Option<(PgPool, PgConnection, String)> {
    let Ok(url) = env::var("POSTGRES_TEST_URL") else {
        assert!(
            env::var("RUSTSTREAM_REQUIRE_LIVE").as_deref() != Ok("1"),
            "RUSTSTREAM_REQUIRE_LIVE=1 but POSTGRES_TEST_URL is not set"
        );
        eprintln!("skipping: set POSTGRES_TEST_URL to run against a Postgres stand");
        return None;
    };
    let name = format!("rs_sqlx_checked_{}", std::process::id());
    let mut admin = PgConnection::connect(&url)
        .await
        .expect("the stand accepts a connection");
    sqlx::raw_sql(AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
        .execute(&mut admin)
        .await
        .expect("the stand creates a test database");
    let options: PgConnectOptions = url.parse().expect("the stand's URL parses");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.database(&name))
        .await
        .expect("the test database accepts a connection");
    sqlx::raw_sql(include_str!("../migrations/0001_queues.sql"))
        .execute(&pool)
        .await
        .expect("the fixture's tables are created");
    Some((pool, admin, name))
}

#[tokio::test]
async fn the_broker_prepares_at_startup_the_texts_the_derive_checked() {
    let Some((pool, mut admin, name)) = database().await else {
        return;
    };
    let connected = SqlxBroker::new(pool.clone())
        .connect()
        .await
        .expect("the broker connects");
    // Opening a subscription runs its startup check, which prepares every statement it runs on
    // the pool's one connection.
    let send = InboxQueue::<SendEmail>::new("outgoing")
        .subscribe(&connected)
        .await;
    let email = InboxQueue::<Email>::new("emails")
        .subscribe(&connected)
        .await;
    let reminder = InboxQueue::<Reminder>::new("reminders")
        .subscribe(&connected)
        .await;
    let entry = InboxQueue::<Entry>::new("ledger")
        .subscribe(&connected)
        .await;
    let webhook = InboxQueue::<Webhook>::new("hooks")
        .subscribe(&connected)
        .await;
    let order = InboxQueue::<Order>::new("orders")
        .subscribe(&connected)
        .await;
    assert!(send.is_ok() && email.is_ok() && reminder.is_ok());
    assert!(entry.is_ok() && webhook.is_ok() && order.is_ok());
    let prepared: Vec<String> =
        sqlx::query_scalar("SELECT statement FROM pg_prepared_statements WHERE NOT from_sql")
            .fetch_all(&pool)
            .await
            .expect("the prepared statements read");
    let checked = checked();
    let unchecked: Vec<&String> = prepared
        .iter()
        .filter(|text| !checked.contains(*text))
        // This query lists what the connection prepared, and is prepared itself.
        .filter(|text| !text.contains("pg_prepared_statements"))
        .collect();
    assert!(prepared.len() > 10, "{prepared:#?}");
    assert!(
        unchecked.is_empty(),
        "prepared at startup, not checked: {unchecked:#?}"
    );
    drop((send, email, reminder, entry, webhook, order));
    connected.shutdown().await.expect("the broker stops");
    pool.close().await;
    sqlx::raw_sql(AssertSqlSafe(format!(
        r#"DROP DATABASE "{name}" WITH (FORCE)"#
    )))
    .execute(&mut admin)
    .await
    .expect("the stand drops the test database");
}
