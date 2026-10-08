//! The outbox as a plugin: one app over one broker, in three variants that differ in the outbox
//! alone.
//!
//! - **No outbox**: the app a user writes without one. A relay answers each command with a
//!   reply, a sink consumes the reply, and a message published from outside a handler goes
//!   straight to the broker.
//! - **By hand**: the same app with the outbox written by the service itself, in raw sqlx and no
//!   crate machinery: the record inserted before the publish, then taken into work and marked in
//!   the handler, on the connections the crate's middleware takes. This is the reference a user
//!   would write.
//! - **Outbox**: the same app with this crate's outbox: the registry, its two layers, and the
//!   republish at startup. A message published from outside a handler goes through
//!   [`Outbox::wrap`].
//!
//! The variants take the places of the raw, adapter and framework loops of an inbox scenario:
//! the outbox against no outbox is the plugin's whole cost, and the outbox against the one
//! written by hand is what the crate's own machinery adds.
//!
//! The apps are written once per broker: over `MemoryBroker`, where the code-cost benchmarks
//! count them, and over Redis Pub/Sub through `ruststream-fred`, where the outbox's own example
//! runs and the wall clock times them.

use std::env;
use std::num::NonZeroUsize;
use std::time::Duration;

use ruststream::HeaderMap;
use ruststream::runtime::{App, PublishExt};
use ruststream_sqlx::outbox::{Nil, OUTBOX_ID_HEADER, Outbox, Registered};
use ruststream_sqlx::{OutboxTable, dialect};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres};
use tokio::time::sleep;

use super::Latch;
use super::raw::{self, Sql};
use super::tables::{OUTBOX_INSERT, OutboxRecord};

/// The name the tracked scenarios record: the reply's.
pub const TRACKED: &str = "out";

/// The name an untracked scenario's registry records, which no message of the app carries.
pub const UNTRACKED: &str = "elsewhere";

/// What a scenario publishes into the app: a command its relay answers.
#[derive(Debug, Serialize, Deserialize, ruststream::Outgoing)]
#[outgoing(name = "in")]
pub struct Command {
    pub id: i64,
}

/// The relay's answer, and what a tracked publish or delivery carries.
#[derive(Debug, Serialize, Deserialize, ruststream::Outgoing)]
#[outgoing(name = "out")]
pub struct Reply {
    pub id: i64,
}

/// The registry this crate's variant runs with.
pub type Tracking = Outbox<Postgres, Registered<OutboxRecord, Nil>>;

/// The crate's outbox for `name`, on `pool`.
pub fn tracking(pool: PgPool, name: &'static str) -> Tracking {
    Outbox::new(pool).register::<OutboxRecord>(name)
}

/// The state of the hand-written variant: the latch, the pool its statements run on, and the
/// fetch and the mark the crate's middleware would run, rendered by the same dialect.
#[derive(Debug)]
pub struct ByHand {
    pub latch: Latch,
    pub pool: PgPool,
    pub fetch: Sql,
    pub mark: Sql,
}

impl ByHand {
    pub fn new(pool: PgPool, latch: Latch) -> Self {
        let (fetch, mark) = raw::outbox(&dialect::Postgres, &OutboxRecord::TABLE.spec());
        Self {
            latch,
            pool,
            fetch,
            mark,
        }
    }

    /// Takes the record `id` into work and marks it once `handle` ran, each on a connection of
    /// its own, as the middleware takes them.
    async fn fetched_and_marked(&self, id: i64, handle: impl FnOnce()) {
        let record: OutboxRecord = {
            let mut conn = self
                .pool
                .acquire()
                .await
                .expect("the pool lends a connection");
            sqlx::query_as(self.fetch.text)
                .bind(id)
                .fetch_one(&mut *conn)
                .await
                .expect("the record is unprocessed")
        };
        handle();
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("the pool lends a connection");
        sqlx::query(self.mark.text)
            .bind(record.id)
            .execute(&mut *conn)
            .await
            .expect("the record is marked");
    }

    /// Inserts the record of a reply, as the crate's `Publish` of the record inserts it, and
    /// returns its id.
    pub async fn record(pool: &PgPool, body: &[u8]) -> i64 {
        let mut conn = pool.acquire().await.expect("the pool lends a connection");
        sqlx::query_scalar(OUTBOX_INSERT)
            .bind(TRACKED)
            .bind(body)
            .fetch_one(&mut *conn)
            .await
            .expect("the record is written")
    }
}

/// The header a tracked message carries its record's id in, as the crate writes it.
pub fn record_header(id: i64) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(OUTBOX_ID_HEADER, id.to_string());
    headers
}

/// The record id a delivery carries, as the hand-written sink reads it.
fn carried_id(headers: &HeaderMap) -> i64 {
    headers
        .get(OUTBOX_ID_HEADER)
        .and_then(|value| str::from_utf8(value).ok()?.parse().ok())
        .expect("the delivery carries its record's id")
}

/// The reply body the hand-written relay records: what the codec encodes for the same reply.
const REPLY_BODY: &[u8] = br#"{"id":1}"#;

/// Mounts `$handler` with `$workers` workers: one is the plain mount.
macro_rules! include_with {
    ($b:ident, $handler:ident, $workers:expr $(, $($then:tt)+)?) => {{
        let workers: NonZeroUsize = $workers;
        if workers.get() == 1 {
            $b.include($handler)$($($then)+)?;
        } else {
            $b.include($handler.workers(workers))$($($then)+)?;
        }
    }};
}

/// The handlers and the apps of the three variants over one broker. `input` and `output` are the
/// subscriptions of the relay and the sink, `broker` builds the broker, `reply` is the policy
/// replies and republished records go out through, and `egress` the one a command, or a message
/// published from outside a handler, goes out through.
macro_rules! variants {
    (
        input = [$($input:tt)*],
        output = [$($output:tt)*],
        broker = $broker_type:ty, $broker:expr,
        reply = $reply:expr,
        egress = $egress_type:ty, $egress:expr,
    ) => {
        use std::convert::Infallible;
        use std::hint::black_box;
        use std::num::NonZeroUsize;

        use ruststream::runtime::Bound;
        use ruststream_sqlx::prelude::*;
        use sqlx::PgPool;

        use crate::common::Latch;
        use crate::common::outbox::{
            ByHand, Command, REPLY_BODY, Reply, Tracking, carried_id, tracking,
        };

        /// The token a scenario publishes its commands or messages through.
        pub type Egress = Bound<$broker_type, $egress_type>;

        #[subscriber($($input)*, reply)]
        async fn relay(cmd: &Command) -> Reply {
            Reply { id: cmd.id }
        }

        #[subscriber($($output)*)]
        async fn sink(reply: &Reply, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
            black_box(reply.id);
            ctx.state().arrived();
            HandlerOutcome::ack()
        }

        /// The relay by hand: the record inserted with the record's own insert, and its id
        /// answered in the reply.
        #[subscriber($($input)*, reply)]
        async fn relay_by_hand(_cmd: &Command, ctx: &mut Context<'_, (), ByHand>) -> Reply {
            Reply {
                id: ByHand::record(&ctx.state().pool, REPLY_BODY).await,
            }
        }

        /// The sink by hand: the record the reply names taken into work, counted, and marked.
        #[subscriber($($output)*)]
        async fn sink_by_hand(reply: &Reply, ctx: &mut Context<'_, (), ByHand>) -> HandlerOutcome {
            let state = ctx.state();
            state
                .fetched_and_marked(reply.id, || {
                    black_box(reply.id);
                    state.latch.arrived();
                })
                .await;
            HandlerOutcome::ack()
        }

        /// A tracked delivery by hand: the record its header names taken into work, counted, and
        /// marked.
        #[subscriber($($output)*)]
        async fn sink_by_header(
            reply: &Reply,
            ctx: &mut Context<'_, (), ByHand>,
        ) -> HandlerOutcome {
            let id = carried_id(ctx.headers());
            let state = ctx.state();
            state
                .fetched_and_marked(id, || {
                    black_box(reply.id);
                    state.latch.arrived();
                })
                .await;
            HandlerOutcome::ack()
        }

        /// A command answered and the answer consumed, with no outbox.
        pub fn round_trip(latch: Latch, workers: NonZeroUsize) -> (impl App, Egress) {
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(latch))
                .with_broker(broker, |b| {
                    include_with!(b, relay, workers, .out_reply($reply));
                    include_with!(b, sink, workers);
                });
            (app, egress)
        }

        /// The same with the outbox written by hand in its handlers.
        pub fn round_trip_by_hand(
            pool: PgPool,
            latch: Latch,
            workers: NonZeroUsize,
        ) -> (impl App, Egress) {
            let state = ByHand::new(pool, latch);
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(state))
                .with_broker(broker, |b| {
                    include_with!(b, relay_by_hand, workers, .out_reply($reply));
                    include_with!(b, sink_by_hand, workers);
                });
            (app, egress)
        }

        /// The same with this crate's outbox recording `name`: the reply is tracked when `name`
        /// is the reply's, and every message passes untracked otherwise.
        pub fn round_trip_outbox(
            pool: PgPool,
            latch: Latch,
            name: &'static str,
            workers: NonZeroUsize,
        ) -> (impl App, Egress) {
            let tracking = tracking(pool, name);
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(latch))
                .layer(tracking.layer())
                .publish_layer(tracking.publish_layer())
                .with_broker(broker, |b| {
                    include_with!(b, relay, workers, .out_reply($reply));
                    include_with!(b, sink, workers);
                    b.after_startup($reply, tracking.republish());
                });
            (app, egress)
        }

        /// A sink consuming tracked messages, with no outbox: the record's header is carried and
        /// read by nobody.
        pub fn delivery(latch: Latch) -> (impl App, Egress) {
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(latch))
                .with_broker(broker, |b| {
                    b.include(sink);
                });
            (app, egress)
        }

        /// The same sink, taking the record into work and marking it by hand.
        pub fn delivery_by_hand(pool: PgPool, latch: Latch) -> (impl App, Egress) {
            let state = ByHand::new(pool, latch);
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(state))
                .with_broker(broker, |b| {
                    b.include(sink_by_header);
                });
            (app, egress)
        }

        /// The same sink under this crate's outbox, which takes the record into work and marks
        /// it.
        pub fn delivery_outbox(pool: PgPool, latch: Latch) -> (impl App, Egress) {
            let tracking = tracking(pool, crate::common::outbox::TRACKED);
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(latch))
                .layer(tracking.layer())
                .publish_layer(tracking.publish_layer())
                .with_broker(broker, |b| {
                    b.include(sink);
                    b.after_startup($reply, tracking.republish());
                });
            (app, egress)
        }

        /// An app that publishes from outside a handler, with no outbox, and the hand-written
        /// variant, which records through its own pool before the same publish.
        pub fn publishing() -> (impl App, Egress) {
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker, |_b| {});
            (app, egress)
        }

        /// The same app under this crate's outbox, and the registry its publisher is wrapped with.
        pub fn publishing_outbox(pool: PgPool) -> (impl App, Egress, Tracking) {
            let tracking = tracking(pool, crate::common::outbox::TRACKED);
            let broker = $broker.bindable();
            let egress = broker.bind($egress);
            let republish = tracking.republish();
            let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
                .layer(tracking.layer())
                .publish_layer(tracking.publish_layer())
                .with_broker(broker, |b| {
                    b.after_startup($reply, republish);
                });
            (app, egress, tracking)
        }
    };
}

/// The variants over `MemoryBroker`, which the code-cost benchmarks count.
pub mod memory {
    use ruststream::memory::{MemoryBroker, MemoryPublish};

    variants! {
        input = ["in"],
        output = ["out"],
        broker = MemoryBroker, MemoryBroker::new(),
        reply = MemoryPublish,
        egress = MemoryPublish, MemoryPublish,
    }
}

/// The variants over Redis Pub/Sub, where the outbox's example runs and the wall clock times
/// them. Every subscription's buffer holds what the feeders keep in flight.
pub mod redis {
    use ruststream_fred::{RedisBroker, RedisPubSub, RedisPubSubPublish};

    use crate::common::outbox::{PUBSUB_BUFFER, redis_url};

    variants! {
        input = [RedisPubSub::new("in").buffer(PUBSUB_BUFFER)],
        output = [RedisPubSub::new("out").buffer(PUBSUB_BUFFER)],
        broker = RedisBroker, RedisBroker::standalone(redis_url()),
        reply = RedisPubSubPublish::default(),
        egress = RedisPubSubPublish, RedisPubSubPublish::default(),
    }
}

/// The messages a wall-clock feeder keeps in flight: Redis Pub/Sub drops what a subscription's
/// buffer cannot hold, so the feeder waits once this many are unconsumed.
pub const IN_FLIGHT: usize = 1_024;

/// Room for every message a feeder keeps in flight, twice over.
pub const PUBSUB_BUFFER: NonZeroUsize = NonZeroUsize::new(2 * IN_FLIGHT).expect("a buffer");

/// The variable that names the stand's Redis, the one the outbox example reads.
pub const REDIS: &str = "REDIS_URL";

/// The URL the stand's Redis answers on.
///
/// # Panics
///
/// Panics when the variable is not set, with the recipe that sets it.
pub fn redis_url() -> String {
    env::var(REDIS).unwrap_or_else(|_| {
        panic!("{REDIS} names the Redis to measure against; `just bench` sets it")
    })
}

/// How long a feeder parks before it looks at the consumer again.
const STEP: Duration = Duration::from_micros(200);

/// Waits until no more than [`IN_FLIGHT`] of the `sent` messages are unconsumed.
pub async fn throttle(sent: usize, latch: &Latch) {
    while sent - (latch.total() - latch.remaining()) > IN_FLIGHT {
        sleep(STEP).await;
    }
}

/// Starts an outbox app over Redis, publishes one command per expected reply from a worker of
/// the runtime, and waits until the sink has consumed every reply.
pub async fn feed_round_trip((app, egress): (impl App, redis::Egress), latch: &Latch) {
    let running = app.start().await.expect("the service starts");
    let publisher = running
        .publisher(egress)
        .await
        .expect("the publisher pairs");
    let state = latch.clone();
    tokio::spawn(async move {
        for sent in 0..state.total() {
            throttle(sent, &state).await;
            publisher
                .message(&Command { id: 1 })
                .publish()
                .await
                .expect("the command is published");
        }
    })
    .await
    .expect("the feeder ends");
    latch.drained_or_stalled("outbox").await;
    running.shutdown().await.expect("the service stops");
}
