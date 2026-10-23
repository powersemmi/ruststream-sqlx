//! The service of `examples/guide/outbox.rs` without the derive, `outbox!` and the handler
//! attribute: the record is described with `OutboxSpec`, the registry tracks it with
//! `Outbox::deferred().track(..)`, and the handlers are named types.
//!
//! ```text
//! RUSTSTREAM_SQLX_OUTBOX=on cargo run -p ruststream-sqlx --example manual_guide_outbox --features outbox,sqlite,testing,asyncapi -- run
//! ```

use std::future::{Future, ready};

use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::outbox::spec::ProcessedAt;
use ruststream_sqlx::outbox::{OutboxSpec, OutboxTable, TrackedName};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:schema]
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    payload BLOB NOT NULL,
    processed_at TEXT
)";
// --8<-- [end:schema]

// --8<-- [start:record]
use ruststream_sqlx::outbox::{self, Outbox};

/// One record of the `outbox` table.
#[derive(sqlx::FromRow)]
struct PaymentOutbox {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for PaymentOutbox {
    type Id = i64;
    type Table = OutboxSpec<(ProcessedAt,)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .processed_at(Column::new("processed_at"));

    fn id(&self) -> &i64 {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// The name `PaymentTaken` is published under.
struct Payments;

impl TrackedName for Payments {
    const NAME: &'static str = "payments";
}

/// The service writes the record and returns its id; the outbox sends the id with the message.
impl outbox::Publish<Sqlite> for PaymentOutbox {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES (?, ?) RETURNING id")
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}
// --8<-- [end:record]

// --8<-- [start:handlers]
use ruststream::memory::prelude::*;
use sqlx::SqlitePool;

/// The reply to `payments` is recorded before it is sent.
struct TakePayment;

// The app's state is the pool `on_startup` returns.
impl Handle<Checkout, PaymentTaken, (), (), SqlitePool> for TakePayment {
    fn handle(
        &self,
        checkout: &Checkout,
        _outs: &(),
        _ctx: &mut Context<'_, (), SqlitePool>,
    ) -> impl Future<Output = Result<PaymentTaken, HandlerOutcome>> {
        tracing::info!(order = checkout.order, "payment taken");
        ready(Ok(PaymentTaken {
            order: checkout.order,
        }))
    }
}

/// The acknowledgement marks the payment's record processed.
struct Ship;

impl Handle<PaymentTaken, (), (), (), SqlitePool> for Ship {
    fn handle(
        &self,
        payment: &PaymentTaken,
        _outs: &(),
        _ctx: &mut Context<'_, (), SqlitePool>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = payment.order, "shipping");
        ready(Ok(()))
    }
}
// --8<-- [end:handlers]

// --8<-- [start:app]
use std::convert::Infallible;
use std::io;

fn service(url: String) -> impl App {
    // The registry starts without a pool: sqlx builds one only inside the Tokio runtime.
    let tracking = Outbox::deferred().track::<PaymentOutbox, Payments>();
    let registry = tracking.clone();

    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| {
            let pool = SqlitePool::connect(&url).await.map_err(io::Error::other)?;
            sqlx::raw_sql(SCHEMA)
                .execute(&pool)
                .await
                .map_err(io::Error::other)?;
            registry.set_pool(pool.clone()).map_err(io::Error::other)?;
            Ok::<_, io::Error>(pool)
        })
        .layer(tracking.layer())
        .publish_layer(tracking.publish_layer())
        .after_shutdown(async move |pool| {
            pool.close().await;
            Ok::<_, Infallible>(())
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("checkout", TakePayment).reply().build())
                .out_reply(Publish);
            b.include(subscriber("payments", Ship).build());
            // Once the subscriptions are open, the unprocessed records are sent again.
            b.after_startup(Publish, tracking.republish());
        })
}

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:payments.db?mode=rwc".to_owned())
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Checkout {
    order: u64,
}

impl OutgoingDestination for Checkout {
    type Form = FixedName;
    const DESTINATION: &'static str = "checkout";
}

impl MessageHeaders for Checkout {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PaymentTaken {
    order: u64,
}

impl OutgoingDestination for PaymentTaken {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for PaymentTaken {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::outbox::OUTBOX_ID_HEADER;
    use sqlx::{Connection, SqliteConnection};

    use super::{Checkout, PaymentTaken, SCHEMA, service};

    // A test build tracks only with `RUSTSTREAM_SQLX_OUTBOX=on`, read once per process: without it
    // there is nothing to check, and a job that asked for every suite fails instead of passing.
    fn tracking_on() -> bool {
        if env::var("RUSTSTREAM_SQLX_OUTBOX").as_deref() == Ok("on") {
            return true;
        }
        assert!(
            env::var("RUSTSTREAM_REQUIRE_LIVE").map_or(true, |value| value.is_empty()),
            "RUSTSTREAM_REQUIRE_LIVE is set, but RUSTSTREAM_SQLX_OUTBOX is not `on`"
        );
        eprintln!("RUSTSTREAM_SQLX_OUTBOX is not `on`; skipping the outbox test");
        false
    }

    // A database in memory that every connection naming it shares, alive while the test holds
    // this connection: the service's own pool opens on the same URL.
    async fn database(name: &str) -> Result<(String, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let mut keeper = SqliteConnection::connect(&url).await?;
        sqlx::raw_sql(SCHEMA).execute(&mut keeper).await?;
        Ok((url, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payment_is_recorded_sent_with_its_id_and_marked_once_shipped()
    -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (url, mut keeper) = database("manual_guide_outbox_tracked").await?;
        let tb = TestApp::start(service(url)).await?;
        tb.broker::<MemoryBroker>()
            .message(&Checkout { order: 7 })
            .publish()
            .await?;

        let published = tb.broker::<MemoryBroker>().published::<()>("payments");
        let ids: Vec<_> = published
            .messages()
            .iter()
            .map(|message| message.headers().get_str(OUTBOX_ID_HEADER))
            .collect();
        assert_eq!(ids, [Some("1")], "the payment carries its record's id");
        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&PaymentTaken { order: 7 })
            .settled(HandlerOutcome::ack());
        let records: Vec<(String, bool)> =
            sqlx::query_as("SELECT name, processed_at IS NOT NULL FROM outbox")
                .fetch_all(&mut keeper)
                .await?;
        assert_eq!(records, [("payments".to_owned(), true)]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unprocessed_payment_is_sent_again_at_startup() -> Result<(), Box<dyn Error>> {
        if !tracking_on() {
            return Ok(());
        }
        let (url, mut keeper) = database("manual_guide_outbox_republish").await?;
        // A payment recorded by an instance that stopped before shipping it.
        sqlx::query("INSERT INTO outbox (name, payload) VALUES ('payments', ?)")
            .bind(br#"{"order":3}"#.as_slice())
            .execute(&mut keeper)
            .await?;
        let tb = TestApp::start(service(url)).await?;
        tb.settle().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&PaymentTaken { order: 3 });
        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NULL")
                .fetch_one(&mut keeper)
                .await?;
        assert_eq!(
            left, 0,
            "the shipping handler processed the payment sent again"
        );
        Ok(())
    }

    // The way a service's own tests run: the outbox stays off, and the handlers run as without it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn with_the_outbox_off_the_payment_is_shipped_untracked() -> Result<(), Box<dyn Error>> {
        if env::var("RUSTSTREAM_SQLX_OUTBOX").as_deref() == Ok("on") {
            return Ok(());
        }
        let (url, mut keeper) = database("manual_guide_outbox_off").await?;
        let tb = TestApp::start(service(url)).await?;
        tb.broker::<MemoryBroker>()
            .message(&Checkout { order: 7 })
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let records: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
            .fetch_one(&mut keeper)
            .await?;
        assert_eq!(records, 0, "nothing recorded");
        Ok(())
    }
}
