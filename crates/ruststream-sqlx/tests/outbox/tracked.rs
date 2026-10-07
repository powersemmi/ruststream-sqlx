//! What tracking does to a service's publishes and deliveries, on each stand: a publish under a
//! registered name is recorded and carries its record's id, a delivery that carries an id takes
//! its record and settles it by the handler's outcome, and the startup republish sends what no
//! consumer processed.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};

use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::testing::TestApp;
use ruststream_sqlx::outbox::{Nil, OUTBOX_ID_HEADER, Outbox, Registered};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;

use crate::live::stands;
use crate::records::{Headers, OrderRecord, ParcelRecord, RefundRecord};
use crate::stand::Stand;
use crate::{SWITCH, tracking_on};

/// What a consumer answers a message with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Outcome {
    Ack,
    Drop,
    Retry,
    RetryAfter,
}

impl Outcome {
    fn settle(self) -> HandlerOutcome {
        match self {
            Self::Ack => HandlerOutcome::ack(),
            Self::Drop => HandlerOutcome::drop(),
            Self::Retry => HandlerOutcome::retry(),
            Self::RetryAfter => HandlerOutcome::retry_after(Duration::from_secs(3600)),
        }
    }
}

/// A request the service answers with an order or a note.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Request {
    id: u32,
    outcome: Outcome,
}

/// An order: published under `orders`, a registered name.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u32,
    outcome: Outcome,
}

/// A refund: published under `refunds`, a registered name whose records are deleted.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "refunds")]
struct Refund {
    id: u32,
    outcome: Outcome,
}

/// A parcel: published under `parcels`, a registered name whose record runs the service's own
/// events.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "parcels")]
struct Parcel {
    id: u32,
    outcome: Outcome,
}

/// A note: published under `notes`, a name the outbox does not track.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "notes")]
struct Note {
    id: u32,
}

/// The id header a test puts on a message it publishes itself.
#[derive(Serialize)]
struct OutboxId<'a> {
    #[serde(rename = "x-ruststream-outbox-id")]
    id: &'a str,
}

/// Stamps a reply with the tenant, a header the record keeps.
struct Stamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Stamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut().insert("tenant", "acme");
    }
}

#[subscriber("requests", reply)]
async fn place(request: &Request) -> Order {
    Order {
        id: request.id,
        outcome: request.outcome,
    }
}

#[subscriber("lookups", reply)]
async fn look_up(request: &Request) -> Note {
    Note { id: request.id }
}

#[subscriber("orders")]
async fn fulfil(order: &Order) -> HandlerOutcome {
    order.outcome.settle()
}

#[subscriber("refunds")]
async fn pay_back(refund: &Refund) -> HandlerOutcome {
    refund.outcome.settle()
}

#[subscriber("parcels")]
async fn ship(parcel: &Parcel) -> HandlerOutcome {
    parcel.outcome.settle()
}

#[subscriber("notes")]
async fn read(_note: &Note) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// What the app does once it started.
#[derive(Clone, Copy)]
enum Startup {
    Nothing,
    Republish,
    RepublishOnly(&'static [&'static str]),
    /// Publishes an order and a note through `wrap` over the live publisher.
    Wrap,
}

/// A time column, `NULL` for none: a mark or a take.
type Time = Option<DateTime<Utc>>;

/// The JSON the default codec writes for `value`.
fn json(value: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(value).expect("the value encodes")
}

/// The headers column's value holding `tenant`.
fn tenant(value: &str) -> Json<BTreeMap<String, String>> {
    Json(BTreeMap::from([("tenant".to_owned(), value.to_owned())]))
}

/// The id headers of what was published under `name`, in order.
fn published_ids<State: Send + Sync + 'static>(
    tb: &TestApp<State>,
    name: &str,
) -> Vec<Option<String>> {
    tb.broker::<MemoryBroker>()
        .published::<()>(name)
        .messages()
        .iter()
        .map(|message| {
            message
                .headers()
                .get_str(OUTBOX_ID_HEADER)
                .map(str::to_owned)
        })
        .collect()
}

/// `error` and every error under it, one per line.
fn chain(error: &(dyn Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        text.push('\n');
        text.push_str(&next.to_string());
        source = next.source();
    }
    text
}

stands! {
    use sqlx::Pool;

    type Tracking = Outbox<
        Db,
        Registered<ParcelRecord, Registered<RefundRecord, Registered<OrderRecord, Nil>>>,
    >;

    /// The registry: `orders` in `outbox`, `refunds` in `outbox_plain`, `parcels` in
    /// `outbox_taken`.
    fn registered(tracking: Outbox<Db>) -> Tracking {
        tracking
            .register::<OrderRecord>("orders")
            .register::<RefundRecord>("refunds")
            .register::<ParcelRecord>("parcels")
    }

    /// The service: its handlers, the two middlewares, and what it does once it started. `pool`,
    /// when given, is set in `on_startup`.
    fn app(tracking: &Tracking, pool: Option<Pool<Db>>, startup: Startup) -> impl App<State = ()> {
        let on_startup = tracking.clone();
        let hook = tracking.clone();
        RustStream::new(AppInfo::new("shop", "0.0.0"))
            .on_startup(move |()| async move {
                pool.map_or(Ok(()), |pool| on_startup.set_pool(pool))
            })
            .layer(tracking.layer())
            .publish_layer(tracking.publish_layer())
            .with_broker(MemoryBroker::new(), move |b| {
                b.include(place).out_reply(Publish).transform(Stamp);
                b.include(look_up);
                b.include(fulfil).max_attempts(nonzero!(1u32));
                b.include(pay_back).max_attempts(nonzero!(1u32));
                b.include(ship).max_attempts(nonzero!(1u32));
                b.include(read);
                match startup {
                    Startup::Nothing => {}
                    Startup::Republish => b.after_startup(Publish, hook.republish()),
                    Startup::RepublishOnly(names) => {
                        b.after_startup(Publish, hook.republish_names(names.iter().copied()));
                    }
                    Startup::Wrap => b.after_startup(Publish, move |live| async move {
                        let publisher = hook.wrap(live);
                        publisher.message(&Order { id: 9, outcome: Outcome::Ack }).publish().await?;
                        publisher.message(&Note { id: 9 }).publish().await
                    }),
                }
            })
    }

    /// Starts the service with its pool given to `new`.
    async fn started(pool: &Pool<Db>, startup: Startup) -> TestApp<()> {
        let tracking = registered(Outbox::new(pool.clone()));
        TestApp::start(app(&tracking, None, startup)).await.expect("the app starts")
    }

    /// The records of `outbox` in id order, as `(id, name, payload, headers, processed)`.
    async fn orders(pool: &Pool<Db>) -> Vec<(i64, String, Vec<u8>, Headers, bool)> {
        let rows: Vec<(i64, String, Vec<u8>, Headers, Time)> = sqlx::query_as(
            "SELECT id, name, payload, headers, processed_at FROM outbox ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .expect("the outbox reads");
        rows.into_iter()
            .map(|(id, name, payload, headers, processed_at)| {
                (id, name, payload, headers, processed_at.is_some())
            })
            .collect()
    }

    /// The records of `outbox_plain` in id order, as `(id, name, retries)`.
    async fn refunds(pool: &Pool<Db>) -> Vec<(i64, String, i32)> {
        sqlx::query_as("SELECT id, name, retries FROM outbox_plain ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("the plain outbox reads")
    }

    /// Writes the record of `order`, processed or not, and returns its id.
    async fn insert_order(pool: &Pool<Db>, order: &Order, headers: Headers, processed: bool) -> i64 {
        let mut conn = pool.acquire().await.expect("a connection opens");
        let insert = sqlx::query(<Db as Stand>::returning_id(
            "INSERT INTO outbox (name, payload, headers, processed_at) VALUES ('orders', ?, ?, ?)",
        ))
        .bind(json(order))
        .bind(headers)
        .bind(processed.then(Utc::now));
        <Db as Stand>::inserted(&mut conn, insert).await.expect("the record is written")
    }

    /// Writes the record of `refund`, retried `retries` times, and returns its id.
    async fn insert_refund(pool: &Pool<Db>, refund: &Refund, retries: i32) -> i64 {
        let mut conn = pool.acquire().await.expect("a connection opens");
        let insert = sqlx::query(<Db as Stand>::returning_id(
            "INSERT INTO outbox_plain (name, payload, retries) VALUES ('refunds', ?, ?)",
        ))
        .bind(json(refund))
        .bind(retries);
        <Db as Stand>::inserted(&mut conn, insert).await.expect("the record is written")
    }

    /// Writes the record of `parcel`, taken or not, with `headers`, and returns its id.
    async fn insert_parcel(pool: &Pool<Db>, parcel: &Parcel, headers: Headers, taken: bool) -> i64 {
        let mut conn = pool.acquire().await.expect("a connection opens");
        let insert = sqlx::query(<Db as Stand>::returning_id(
            "INSERT INTO outbox_taken (name, payload, headers, taken_at) \
             VALUES ('parcels', ?, ?, ?)",
        ))
        .bind(json(parcel))
        .bind(headers)
        .bind(taken.then(Utc::now));
        <Db as Stand>::inserted(&mut conn, insert).await.expect("the record is written")
    }

    /// The records of `outbox_taken` in id order, as `(id, taken, processed, attempts)`.
    async fn parcels(pool: &Pool<Db>) -> Vec<(i64, bool, bool, i32)> {
        let rows: Vec<(i64, Time, Time, i32)> = sqlx::query_as(
            "SELECT id, taken_at, processed_at, attempts FROM outbox_taken ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .expect("the taken outbox reads");
        rows.into_iter()
            .map(|(id, taken_at, processed_at, attempts)| {
                (id, taken_at.is_some(), processed_at.is_some(), attempts)
            })
            .collect()
    }

    /// Drops `outbox`, so every statement on it fails.
    async fn drop_outbox(pool: &Pool<Db>) {
        sqlx::query("DROP TABLE outbox").execute(pool).await.expect("the table drops");
    }

    /// Publishes `value` to `name` with `id` in the id header, and lets the service handle it.
    async fn publish_tracked<State: Send + Sync + 'static>(
        tb: &TestApp<State>,
        name: &str,
        value: &(impl Serialize + Sync),
        id: &str,
    ) {
        tb.broker::<MemoryBroker>()
            .publish_with_headers(name, value, &OutboxId { id })
            .await
            .expect("the message is handled");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reply_to_a_registered_name_is_recorded_and_carries_its_id() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let tb = started(&db.pool, Startup::Nothing).await;
        let request = Request { id: 1, outcome: Outcome::Ack };
        tb.broker::<MemoryBroker>().publish("requests", &request).await.expect("the request is handled");

        let order = Order { id: 1, outcome: Outcome::Ack };
        let rows = orders(&db.pool).await;
        let id = rows.first().expect("the reply was recorded").0;
        assert_eq!(rows, [(id, "orders".to_owned(), json(&order), Some(tenant("acme")), true)]);
        assert_eq!(published_ids(&tb, "orders"), [Some(id.to_string())]);
        tb.broker::<MemoryBroker>().subscriber("orders").assert_called(1).with(&order);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reply_to_a_name_not_registered_is_not_recorded() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let tb = started(&db.pool, Startup::Nothing).await;
        let request = Request { id: 2, outcome: Outcome::Ack };
        tb.broker::<MemoryBroker>().publish("lookups", &request).await.expect("the request is handled");

        assert_eq!(published_ids(&tb, "notes"), [None]);
        tb.broker::<MemoryBroker>().subscriber("notes").assert_called(1).with(&Note { id: 2 });
        assert_eq!(orders(&db.pool).await, []);
        assert_eq!(refunds(&db.pool).await, []);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_whose_record_fails_is_not_sent() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let tb = started(&db.pool, Startup::Nothing).await;
        drop_outbox(&db.pool).await;
        let request = Request { id: 3, outcome: Outcome::Ack };
        let _ = tb.broker::<MemoryBroker>().publish("requests", &request).await;

        tb.broker::<MemoryBroker>().published::<Order>("orders").assert_not_called();
        tb.broker::<MemoryBroker>().subscriber("orders").assert_not_called();
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    /// Writes an unprocessed record of an order settled with `outcome`, delivers it with its id,
    /// and returns what `outbox` holds after the handler ran once.
    async fn delivered(outcome: Outcome) -> Option<Vec<(i64, String, Vec<u8>, Headers, bool)>> {
        if !tracking_on() {
            return None;
        }
        let db = database().await?;
        let order = Order { id: 4, outcome };
        let id = insert_order(&db.pool, &order, None, false).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        publish_tracked(&tb, "orders", &order, &id.to_string()).await;
        tb.broker::<MemoryBroker>().subscriber("orders").assert_called(1).with(&order);
        tb.shutdown().await.expect("the app stops");
        let rows = orders(&db.pool).await;
        db.finish().await;
        Some(rows)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acknowledged_delivery_marks_its_record() {
        let Some(rows) = delivered(Outcome::Ack).await else { return };
        assert_eq!(rows.iter().map(|row| row.4).collect::<Vec<_>>(), [true]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_delivery_marks_its_record() {
        let Some(rows) = delivered(Outcome::Drop).await else { return };
        assert_eq!(rows.iter().map(|row| row.4).collect::<Vec<_>>(), [true]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_delivery_leaves_its_record_unprocessed() {
        let Some(rows) = delivered(Outcome::Retry).await else { return };
        assert_eq!(rows.iter().map(|row| row.4).collect::<Vec<_>>(), [false]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_retried_later_leaves_its_record_unprocessed() {
        let Some(rows) = delivered(Outcome::RetryAfter).await else { return };
        assert_eq!(rows.iter().map(|row| row.4).collect::<Vec<_>>(), [false]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_processed_record_is_acknowledged_without_the_handler() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let order = Order { id: 5, outcome: Outcome::Retry };
        let id = insert_order(&db.pool, &order, None, true).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        publish_tracked(&tb, "orders", &order, &id.to_string()).await;

        // The handler retries this order, so an acknowledgement is the layer's own.
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(1)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deliveries_without_a_tracked_id_run_untracked() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let order = Order { id: 6, outcome: Outcome::Ack };
        let id = insert_order(&db.pool, &order, None, false).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        // A registered name without the header, a header that is not an id, and a header on a
        // name the outbox does not track.
        tb.broker::<MemoryBroker>().publish("orders", &order).await.expect("the order is handled");
        publish_tracked(&tb, "orders", &order, "six").await;
        publish_tracked(&tb, "notes", &Note { id: 6 }, &id.to_string()).await;

        tb.broker::<MemoryBroker>().subscriber("orders").assert_called(2);
        tb.broker::<MemoryBroker>().subscriber("notes").assert_called(1);
        let rows = orders(&db.pool).await;
        assert_eq!(rows, [(id, "orders".to_owned(), json(&order), None, false)]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_record_without_processed_at_is_deleted_or_retried_by_its_own_events() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let acked = Refund { id: 7, outcome: Outcome::Ack };
        let retried = Refund { id: 8, outcome: Outcome::Retry };
        let acked_id = insert_refund(&db.pool, &acked, 0).await;
        let retried_id = insert_refund(&db.pool, &retried, 0).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        publish_tracked(&tb, "refunds", &acked, &acked_id.to_string()).await;
        publish_tracked(&tb, "refunds", &retried, &retried_id.to_string()).await;

        tb.broker::<MemoryBroker>().subscriber("refunds").assert_called(2);
        assert_eq!(refunds(&db.pool).await, [(retried_id, "refunds".to_owned(), 1)]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_settlement_that_fails_keeps_the_handlers_outcome() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        // A second retry breaks the table's check, so the retry event fails.
        let refund = Refund { id: 19, outcome: Outcome::Retry };
        let id = insert_refund(&db.pool, &refund, 1).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        publish_tracked(&tb, "refunds", &refund, &id.to_string()).await;

        tb.broker::<MemoryBroker>()
            .subscriber("refunds")
            .assert_called(1)
            .settled(HandlerOutcome::retry());
        assert_eq!(refunds(&db.pool).await, [(id, "refunds".to_owned(), 1)]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_whose_record_cannot_be_taken_is_retried_without_the_handler() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let order = Order { id: 10, outcome: Outcome::Ack };
        let id = insert_order(&db.pool, &order, None, false).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        drop_outbox(&db.pool).await;
        publish_tracked(&tb, "orders", &order, &id.to_string()).await;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(1)
            // The handler acknowledges this order, so a retry is the layer's own.
            .settled(HandlerOutcome::retry());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_republish_sends_what_no_consumer_processed() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let first = Order { id: 11, outcome: Outcome::Ack };
        let done = Order { id: 12, outcome: Outcome::Ack };
        let second = Order { id: 13, outcome: Outcome::Ack };
        let refunded = Refund { id: 14, outcome: Outcome::Ack };
        let first_id = insert_order(&db.pool, &first, Some(tenant("acme")), false).await;
        insert_order(&db.pool, &done, None, true).await;
        let second_id = insert_order(&db.pool, &second, None, false).await;
        insert_refund(&db.pool, &refunded, 0).await;
        let tb = started(&db.pool, Startup::Republish).await;
        tb.settle().await.expect("the republished messages are handled");

        assert_eq!(tb.broker::<MemoryBroker>().subscriber("orders").received::<Order>(), [first, second]);
        tb.broker::<MemoryBroker>().subscriber("refunds").assert_called(1).with(&refunded);
        assert_eq!(
            published_ids(&tb, "orders"),
            [Some(first_id.to_string()), Some(second_id.to_string())],
        );
        let published = tb.broker::<MemoryBroker>().published::<()>("orders");
        let tenants: Vec<_> = published
            .messages()
            .iter()
            .map(|message| message.headers().get_str("tenant"))
            .collect();
        assert_eq!(tenants, [Some("acme"), None]);
        assert!(orders(&db.pool).await.iter().all(|row| row.4), "every record is processed");
        assert_eq!(refunds(&db.pool).await, []);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn republish_names_sends_only_the_names_it_lists() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let order = Order { id: 15, outcome: Outcome::Ack };
        let refunded = Refund { id: 16, outcome: Outcome::Ack };
        insert_order(&db.pool, &order, None, false).await;
        let refund_id = insert_refund(&db.pool, &refunded, 0).await;
        let tb = started(&db.pool, Startup::RepublishOnly(&["orders"])).await;
        tb.settle().await.expect("the republished messages are handled");

        tb.broker::<MemoryBroker>().subscriber("orders").assert_called(1).with(&order);
        tb.broker::<MemoryBroker>().subscriber("refunds").assert_not_called();
        assert_eq!(refunds(&db.pool).await, [(refund_id, "refunds".to_owned(), 0)]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_publish_through_wrap_is_recorded_and_carries_its_id() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let tb = started(&db.pool, Startup::Wrap).await;
        tb.settle().await.expect("the published messages are handled");

        let order = Order { id: 9, outcome: Outcome::Ack };
        let rows = orders(&db.pool).await;
        let id = rows.first().expect("the publish was recorded").0;
        assert_eq!(rows, [(id, "orders".to_owned(), json(&order), None, true)]);
        assert_eq!(published_ids(&tb, "orders"), [Some(id.to_string())]);
        assert_eq!(published_ids(&tb, "notes"), [None]);
        tb.broker::<MemoryBroker>().subscriber("orders").assert_called(1).with(&order);
        tb.broker::<MemoryBroker>().subscriber("notes").assert_called(1);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pool_set_in_on_startup_tracks() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let tracking = registered(Outbox::deferred());
        let tb = TestApp::start(app(&tracking, Some(db.pool.clone()), Startup::Nothing))
            .await
            .expect("the app starts");
        let request = Request { id: 17, outcome: Outcome::Ack };
        tb.broker::<MemoryBroker>().publish("requests", &request).await.expect("the request is handled");

        let rows = orders(&db.pool).await;
        let id = rows.first().expect("the reply was recorded").0;
        assert_eq!(published_ids(&tb, "orders"), [Some(id.to_string())]);
        assert!(rows.iter().all(|row| row.4), "the consumer processed the record");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tracked_publish_without_a_pool_fails_naming_the_name() {
        if !tracking_on() {
            return;
        }
        let tracking = registered(Outbox::deferred());
        let Err(error) = TestApp::start(app(&tracking, None, Startup::Wrap)).await else {
            panic!("a tracked publish without a pool must fail the startup");
        };
        let text = chain(&error);
        assert!(text.contains("`orders`"), "the error names the name: {text}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tracked_delivery_without_a_pool_is_retried_without_the_handler() {
        if !tracking_on() {
            return;
        }
        let tracking = registered(Outbox::deferred());
        let tb = TestApp::start(app(&tracking, None, Startup::Nothing)).await.expect("the app starts");
        let order = Order { id: 18, outcome: Outcome::Ack };
        publish_tracked(&tb, "orders", &order, "18").await;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(1)
            // The handler acknowledges this order, so a retry is the layer's own.
            .settled(HandlerOutcome::retry());
        tb.shutdown().await.expect("the app stops");
    }

    /// Writes an untaken record of a parcel settled with `outcome`, delivers it with its id, and
    /// returns what `outbox_taken` holds after the handler ran once.
    async fn shipped(outcome: Outcome) -> Option<Vec<(i64, bool, bool, i32)>> {
        if !tracking_on() {
            return None;
        }
        let db = database().await?;
        let parcel = Parcel { id: 20, outcome };
        let id = insert_parcel(&db.pool, &parcel, None, false).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        publish_tracked(&tb, "parcels", &parcel, &id.to_string()).await;
        tb.broker::<MemoryBroker>().subscriber("parcels").assert_called(1).with(&parcel);
        tb.shutdown().await.expect("the app stops");
        let rows = parcels(&db.pool).await;
        db.finish().await;
        Some(rows)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_services_own_fetch_takes_and_its_own_ack_marks_the_record() {
        let Some(rows) = shipped(Outcome::Ack).await else { return };
        let states: Vec<_> = rows.iter().map(|row| (row.1, row.2, row.3)).collect();
        assert_eq!(states, [(true, true, 0)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_services_own_retry_releases_the_record_and_counts_the_attempt() {
        let Some(rows) = shipped(Outcome::Retry).await else { return };
        let states: Vec<_> = rows.iter().map(|row| (row.1, row.2, row.3)).collect();
        assert_eq!(states, [(false, false, 1)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_services_own_discard_deletes_the_record() {
        let Some(rows) = shipped(Outcome::Drop).await else { return };
        assert_eq!(rows, []);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_record_the_services_own_fetch_refuses_is_acknowledged_without_the_handler() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let parcel = Parcel { id: 21, outcome: Outcome::Retry };
        let id = insert_parcel(&db.pool, &parcel, None, true).await;
        let tb = started(&db.pool, Startup::Nothing).await;
        publish_tracked(&tb, "parcels", &parcel, &id.to_string()).await;

        // The handler retries this parcel, so an acknowledgement is the layer's own.
        tb.broker::<MemoryBroker>()
            .subscriber("parcels")
            .assert_called(1)
            .settled(HandlerOutcome::ack());
        assert_eq!(parcels(&db.pool).await, [(id, true, false, 0)]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_services_own_recovery_republishes_only_what_it_selects() {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let waiting = Parcel { id: 22, outcome: Outcome::Ack };
        let taken = Parcel { id: 23, outcome: Outcome::Ack };
        let waiting_id = insert_parcel(&db.pool, &waiting, Some(tenant("acme")), false).await;
        let taken_id = insert_parcel(&db.pool, &taken, None, true).await;
        let tb = started(&db.pool, Startup::Republish).await;
        tb.settle().await.expect("the republished messages are handled");

        tb.broker::<MemoryBroker>().subscriber("parcels").assert_called(1).with(&waiting);
        assert_eq!(published_ids(&tb, "parcels"), [Some(waiting_id.to_string())]);
        let published = tb.broker::<MemoryBroker>().published::<()>("parcels");
        let tenants: Vec<_> = published
            .messages()
            .iter()
            .map(|message| message.headers().get_str("tenant"))
            .collect();
        assert_eq!(tenants, [Some("acme")]);
        assert_eq!(
            parcels(&db.pool).await,
            [(waiting_id, true, true, 0), (taken_id, true, false, 0)],
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_build_with_the_outbox_off_touches_no_database() {
        if env::var(SWITCH).as_deref() == Ok("on") {
            eprintln!("{SWITCH} is `on`; skipping the test of a build that leaves the outbox off");
            return;
        }
        // Every connection of this pool fails, so a statement anywhere fails the test.
        let tracking = registered(Outbox::new(<Db as Stand>::unreachable()));
        let tb = TestApp::start(app(&tracking, None, Startup::Republish))
            .await
            .expect("the app starts without its database");
        let request = Request { id: 24, outcome: Outcome::Ack };
        tb.broker::<MemoryBroker>()
            .publish("requests", &request)
            .await
            .expect("the request is handled");
        let order = Order { id: 25, outcome: Outcome::Ack };
        publish_tracked(&tb, "orders", &order, "25").await;

        // The reply carries no id, and the test's own message keeps the one it was given.
        assert_eq!(published_ids(&tb, "orders"), [None, Some("25".to_owned())]);
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
    }
}
