//! An outbox record described by hand beside the same record derived, on the in-process SQLite
//! stand: both describe one table, so the registry builds the same statements from either, and a
//! reply is recorded and its acknowledgement marks the record in both forms.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::dialect::{Column, MySql, OutboxDialect, Postgres, Sqlite as SqliteDialect};
use ruststream_sqlx::outbox::spec::{Headers, ProcessedAt, own};
use ruststream_sqlx::outbox::{self, OUTBOX_ID_HEADER, Outbox, TrackedName};
use ruststream_sqlx::{HeaderRow, OutboxSpec, OutboxTable};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::{Error, Sqlite, SqliteConnection, SqlitePool};

use crate::live::sqlite::database;
use crate::tracking_on;

/// The `headers` column: a JSON object of strings, `NULL` for none.
type HeaderColumn = Option<Json<BTreeMap<String, String>>>;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Request {
    id: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u32,
}

#[subscriber("requests", reply)]
async fn place(request: &Request) -> Order {
    Order { id: request.id }
}

#[subscriber("orders")]
async fn fulfil(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The record of a published message: the same statement in both forms.
async fn record(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> Result<i64, Error> {
    sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
        .bind(msg.name())
        .bind(msg.payload())
        .fetch_one(conn)
        .await
}

/// The record as the derive describes it.
mod derived {
    use ruststream_sqlx::{Outbox, outbox};
    use sqlx::{Error, Sqlite, SqliteConnection};

    use super::HeaderColumn;

    #[derive(Debug, Outbox, sqlx::FromRow)]
    #[outbox(table = "outbox")]
    pub(crate) struct OrderEvent {
        #[field(id)]
        id: i64,
        #[field(name)]
        name: String,
        #[field(payload)]
        payload: Vec<u8>,
        #[field(headers)]
        headers: HeaderColumn,
        #[field(processed_at)]
        processed_at: Option<super::DateTime<super::Utc>>,
    }

    impl outbox::Publish<Sqlite> for OrderEvent {
        async fn publish(
            conn: &mut SqliteConnection,
            msg: &ruststream::OutgoingMessage<'_>,
        ) -> Result<i64, Error> {
            super::record(conn, msg).await
        }
    }
}

/// The name the hand-described records track, as a type.
struct Orders;

impl TrackedName for Orders {
    const NAME: &'static str = "orders";
}

/// The record described by hand: the struct holds only what the outbox reads.
#[derive(Debug, sqlx::FromRow)]
struct OrderEvent {
    id: i64,
    name: String,
    payload: Vec<u8>,
    headers: HeaderColumn,
}

impl OutboxTable for OrderEvent {
    type Id = i64;
    type Table = OutboxSpec<(Headers, ProcessedAt)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .headers(Column::new("headers"))
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

impl HeaderRow for OrderEvent {
    type Column = HeaderColumn;

    fn headers_mut(&mut self) -> &mut HeaderColumn {
        &mut self.headers
    }
}

impl outbox::Publish<Sqlite> for OrderEvent {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> Result<i64, Error> {
        record(conn, msg).await
    }
}

/// A record described by hand whose acknowledgement is its own: it deletes the record, where the
/// default would mark it.
#[derive(Debug, sqlx::FromRow)]
struct ArchivedEvent {
    id: i64,
    name: String,
    payload: Vec<u8>,
}

impl OutboxTable for ArchivedEvent {
    type Id = i64;
    type Table = OutboxSpec<(ProcessedAt, own::Ack)>;
    const TABLE: Self::Table = OutboxSpec::new(
        "outbox",
        Column::new("id"),
        Column::new("name"),
        Column::new("payload"),
    )
    .processed_at(Column::new("processed_at"))
    .own::<own::Ack>();

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

impl outbox::Publish<Sqlite> for ArchivedEvent {
    async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> Result<i64, Error> {
        record(conn, msg).await
    }
}

impl outbox::Ack<Sqlite> for ArchivedEvent {
    async fn ack(conn: &mut SqliteConnection, id: &i64) -> Result<(), Error> {
        sqlx::query("DELETE FROM outbox WHERE id = $1")
            .bind(*id)
            .execute(conn)
            .await
            .map(drop)
    }
}

/// The records of `outbox` in id order, as `(name, processed)`.
async fn processed(pool: &SqlitePool) -> Vec<(String, bool)> {
    sqlx::query_as("SELECT name, processed_at IS NOT NULL FROM outbox ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("the outbox reads")
}

/// The service, tracked by `tracking`: a request is answered with an order, which `fulfil`
/// acknowledges. Returns the records once the order settled, and the id the order carried.
macro_rules! scenario {
    ($tracking:expr, $pool:expr) => {{
        let tracking = $tracking;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .layer(tracking.layer())
            .publish_layer(tracking.publish_layer())
            .with_broker(MemoryBroker::new(), |b| {
                b.include(place).out_reply(Publish);
                b.include(fulfil);
            });
        let tb = TestApp::start(app).await.expect("the app starts");
        tb.broker::<MemoryBroker>()
            .message(&Request { id: 7 })
            .to("requests")
            .publish()
            .await
            .expect("the request publishes");
        tb.settle().await.expect("the deliveries settle");
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(1)
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        let carried: Vec<Option<String>> = tb
            .broker::<MemoryBroker>()
            .published::<()>("orders")
            .messages()
            .iter()
            .map(|message| {
                message
                    .headers()
                    .get_str(OUTBOX_ID_HEADER)
                    .map(str::to_owned)
            })
            .collect();
        tb.shutdown().await.expect("the app stops");
        (processed($pool).await, carried)
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_derived_record_registered_by_the_macro_tracks_a_reply() {
    if !tracking_on() {
        return;
    }
    let db = database().await.expect("SQLite needs no server");
    let tracking = ruststream_sqlx::outbox! {
        pool: db.pool.clone(),
        "orders" => derived::OrderEvent,
    };
    let (records, carried) = scenario!(tracking, &db.pool);
    assert_eq!(records, [("orders".to_owned(), true)]);
    assert_eq!(carried, [Some("1".to_owned())]);
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_record_described_by_hand_and_tracked_by_type_tracks_a_reply() {
    if !tracking_on() {
        return;
    }
    let db = database().await.expect("SQLite needs no server");
    let tracking = Outbox::new(db.pool.clone()).track::<OrderEvent, Orders>();
    let (records, carried) = scenario!(tracking, &db.pool);
    assert_eq!(
        records,
        [("orders".to_owned(), true)],
        "the reply was recorded, and its acknowledgement marked the record"
    );
    assert_eq!(carried, [Some("1".to_owned())]);
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_event_of_the_records_own_runs_instead_of_the_default() {
    if !tracking_on() {
        return;
    }
    let db = database().await.expect("SQLite needs no server");
    let tracking = Outbox::new(db.pool.clone()).track::<ArchivedEvent, Orders>();
    let (records, carried) = scenario!(tracking, &db.pool);
    assert_eq!(records, [], "the record's own ack deleted the record");
    assert_eq!(carried, [Some("1".to_owned())]);
    db.finish().await;
}

#[test]
fn both_forms_describe_one_table_and_build_the_same_statements() {
    let derived = <derived::OrderEvent as OutboxTable>::TABLE.spec();
    let manual = <OrderEvent as OutboxTable>::TABLE.spec();
    assert_eq!(derived, manual);
    let dialects: [&dyn OutboxDialect; 3] = [&Postgres, &MySql, &SqliteDialect];
    for dialect in dialects {
        for statement in [
            OutboxDialect::outbox_fetch,
            OutboxDialect::outbox_mark,
            OutboxDialect::outbox_recover,
        ] {
            let from_derived = statement(dialect, &derived).expect("the derived table fits");
            let from_manual = statement(dialect, &manual).expect("the manual table fits");
            assert_eq!(from_derived.sql(), from_manual.sql());
        }
    }
    assert_eq!(
        SqliteDialect
            .outbox_mark(&manual)
            .expect("the table fits")
            .sql(),
        "UPDATE `outbox` SET `processed_at` = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now') \
         WHERE `id` = ?"
    );
}

#[test]
#[should_panic(expected = "`orders` is registered with the outbox twice")]
fn a_name_tracked_by_type_after_its_registration_by_string_panics() {
    let _ = Outbox::<Sqlite>::deferred()
        .register::<derived::OrderEvent>("orders")
        .track::<OrderEvent, Orders>();
}

#[test]
#[should_panic(expected = "`orders` is registered with the outbox twice")]
fn a_name_registered_by_string_after_it_was_tracked_by_type_panics() {
    let _ = Outbox::<Sqlite>::deferred()
        .track::<OrderEvent, Orders>()
        .register::<derived::OrderEvent>("orders");
}
