//! The middle loop of the wall-clock comparison: this crate driven by hand, with no service.
//!
//! A drain connects `SqlxBroker`, opens the scenario's subscription on the connected broker and
//! reads its stream of deliveries: the decode, the count and the acknowledgement are written
//! here, where the runtime would dispatch them to a handler. A publish pairs the crate's own
//! publisher and sends through it. So the loop runs every statement this crate runs, in the order
//! it runs them, and nothing of the framework's runtime: against the raw loop it is what the
//! crate itself costs, and against the service it is what the runtime adds on top.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::pin::pin;

use futures::StreamExt;
use ruststream::{
    BatchSubscriber, Broker, Carries, ConnectedBroker, IncomingMessage, Lend, OutgoingMessage,
    PublishPolicy, Publisher, Subscribe, Subscriber, SubscriptionSource,
};
use ruststream_sqlx::{ConnectedSqlxBroker, InboxQueue, Repository, Routed, SqlxBroker};
use sqlx::{PgPool, Postgres};

use super::framework::JOBS;
use super::raw::read_payload;
use super::tables::{AdvisoryJob, LeaseJob, NamedJob, OrderRow, ReplyJob, RowLockJob};
use super::{BATCH, Confirmation, Latch, ORDERS, Order, OrderPlaced, REPLIES};

/// The connected broker every drain and publish runs on.
type Connected = ConnectedSqlxBroker<Postgres>;

async fn connect(broker: SqlxBroker<Postgres>) -> Connected {
    broker.connect().await.expect("the broker connects")
}

async fn shut_down(connected: Connected) {
    connected.shutdown().await.expect("the broker shuts down");
}

/// Reads `subscriber` one delivery at a time until `latch` is drained: `read`, the count, the
/// acknowledgement, in the order the runtime runs them around a handler.
pub async fn consume<Sub>(mut subscriber: Sub, latch: &Latch, read: impl Fn(&Sub::Message))
where
    Sub: Subscriber,
{
    let mut deliveries = pin!(subscriber.stream());
    while let Some(delivery) = deliveries.next().await {
        let delivery = delivery.expect("the subscription delivers");
        read(&delivery);
        latch.arrived();
        delivery.ack().await.expect("the acknowledgement settles");
        if latch.remaining() == 0 {
            return;
        }
    }
    panic!("the subscription ended before the table was drained");
}

/// Reads `subscriber` in batches of up to `size` until `latch` is drained: each delivery read,
/// counted and acknowledged in the order the batch yields them, as the runtime settles a batch.
pub async fn consume_batches<Sub>(mut subscriber: Sub, size: NonZeroUsize, latch: &Latch)
where
    Sub: BatchSubscriber,
{
    let mut batches = pin!(subscriber.batches(size));
    while let Some(batch) = batches.next().await {
        for delivery in batch.expect("the subscription delivers") {
            read_payload(delivery.payload());
            latch.arrived();
            delivery.ack().await.expect("the acknowledgement settles");
        }
        if latch.remaining() == 0 {
            return;
        }
    }
    panic!("the subscription ended before the table was drained");
}

/// A payload table read through `InboxQueue`, one delivery at a time.
async fn queue<Row>(pool: PgPool, latch: &Latch)
where
    InboxQueue<Row>: SubscriptionSource<Connected>,
    <InboxQueue<Row> as SubscriptionSource<Connected>>::Subscriber: Subscriber,
{
    let connected = connect(SqlxBroker::new(pool)).await;
    let subscriber = InboxQueue::<Row>::new(JOBS)
        .subscribe(&connected)
        .await
        .expect("the subscription opens");
    consume(subscriber, latch, |delivery| {
        read_payload(delivery.payload());
    })
    .await;
    shut_down(connected).await;
}

/// The row lock form, one delivery at a time.
pub async fn row_lock(pool: PgPool, latch: Latch) {
    queue::<RowLockJob>(pool, &latch).await;
}

/// The lease form, one delivery at a time.
pub async fn lease(pool: PgPool, latch: Latch) {
    queue::<LeaseJob>(pool, &latch).await;
}

/// The advisory lock form, one delivery at a time.
pub async fn advisory(pool: PgPool, latch: Latch) {
    queue::<AdvisoryJob>(pool, &latch).await;
}

/// The row lock form, each delivery answered through the connected broker's default publisher,
/// `Routed`, before it is acknowledged: the encode into a buffer the loop keeps, as a dispatch loop
/// reuses one.
pub async fn reply(pool: PgPool, latch: Latch) {
    let connected = connect(SqlxBroker::new(pool).route::<ReplyJob>(REPLIES)).await;
    let publisher = Routed.pair(&connected).await.expect("the publisher pairs");
    let mut subscriber = InboxQueue::<RowLockJob>::new(JOBS)
        .subscribe(&connected)
        .await
        .expect("the subscription opens");
    let mut buffer = Vec::new();
    {
        let mut deliveries = pin!(subscriber.stream());
        while let Some(delivery) = deliveries.next().await {
            let delivery = delivery.expect("the subscription delivers");
            let order: Order =
                serde_json::from_slice(delivery.payload()).expect("the body decodes");
            latch.arrived();
            buffer.clear();
            serde_json::to_writer(
                &mut buffer,
                &Confirmation {
                    id: black_box(order.id),
                },
            )
            .expect("the reply encodes");
            publisher
                .publish(OutgoingMessage::new(REPLIES, &buffer[..]), None)
                .await
                .expect("the reply is inserted");
            delivery.ack().await.expect("the acknowledgement settles");
            if latch.remaining() == 0 {
                break;
            }
        }
    }
    assert_eq!(
        latch.remaining(),
        0,
        "the subscription ended before the table was drained"
    );
    drop(subscriber);
    drop(publisher);
    shut_down(connected).await;
}

/// The subscription by name a route leads into [`NamedJob`]'s table.
pub async fn by_name(pool: PgPool, latch: Latch) {
    let connected = connect(SqlxBroker::new(pool).route::<NamedJob>(JOBS)).await;
    let subscriber = connected
        .subscribe(JOBS)
        .await
        .expect("the subscription opens");
    consume(subscriber, &latch, |delivery| {
        read_payload(delivery.payload());
    })
    .await;
    shut_down(connected).await;
}

/// Row mode: the delivery lends the row the driver decoded.
pub async fn row_mode(pool: PgPool, latch: Latch) {
    let connected = connect(SqlxBroker::new(pool)).await;
    let subscriber = InboxQueue::<OrderRow>::new(JOBS)
        .subscribe(&connected)
        .await
        .expect("the subscription opens");
    consume(subscriber, &latch, |delivery| {
        let order: &OrderRow = delivery.carried().expect("the delivery carries its row");
        black_box((order.customer.len(), order.quantity));
    })
    .await;
    shut_down(connected).await;
}

/// The row lock form in batches of [`BATCH`].
pub async fn batch(pool: PgPool, latch: Latch) {
    let connected = connect(SqlxBroker::new(pool)).await;
    let subscriber = InboxQueue::<RowLockJob>::new(JOBS)
        .subscribe(&connected)
        .await
        .expect("the subscription opens");
    consume_batches(subscriber, BATCH, &latch).await;
    shut_down(connected).await;
}

/// Publishes through `publisher` once per delivery the latch expects: the encode into a buffer
/// the loop keeps, as a publish inside a handler's dispatch loop reuses one, then the publish.
async fn publish(publisher: &impl Publisher<Payload = Lend, Options = ()>, latch: &Latch) {
    let mut buffer = Vec::new();
    for _ in 0..latch.total() {
        buffer.clear();
        serde_json::to_writer(&mut buffer, &OrderPlaced::fixed()).expect("the message encodes");
        publisher
            .publish(OutgoingMessage::new(ORDERS, &buffer[..]), None)
            .await
            .expect("the publish writes a row");
        latch.arrived();
    }
}

/// A `Repository` of [`NamedJob`], which names its table at compile time.
pub async fn repository(pool: PgPool, latch: Latch) {
    let connected = connect(SqlxBroker::new(pool)).await;
    let publisher = Repository::<NamedJob>::default()
        .pair(&connected)
        .await
        .expect("the publisher pairs");
    publish(&publisher, &latch).await;
    drop(publisher);
    shut_down(connected).await;
}

/// `Routed`: the message's name finds [`NamedJob`]'s table through a route.
pub async fn routed(pool: PgPool, latch: Latch) {
    let connected = connect(SqlxBroker::new(pool).route::<NamedJob>(ORDERS)).await;
    let publisher = Routed.pair(&connected).await.expect("the publisher pairs");
    publish(&publisher, &latch).await;
    drop(publisher);
    shut_down(connected).await;
}
