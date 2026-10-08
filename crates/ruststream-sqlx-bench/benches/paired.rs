// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a database fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate, and then the runtime above it, cost over the sqlx statements they run.
//!
//! Every scenario runs three times over, and the three runs differ in one thing each: what
//! carries the messages.
//!
//! - `raw` drives sqlx directly: a hand-written loop that runs the statements the broker runs for
//!   the same table, in the order it runs them, on a pool built the same way.
//! - `adapter` drives this crate and nothing else - `SqlxBroker` connected, the table's
//!   `InboxQueue` as a subscription source, the stream of deliveries it yields and each
//!   delivery's own `ack`, or `Repository` and `Routed` paired into publishers. A loop here pulls
//!   the stream, decodes, reads a field and settles. No handler, no app, no dispatch.
//! - `framework` is the service a user writes: a `#[subscriber]` handler, the app, the runtime.
//!
//! `adapter` against `raw` is what this crate's own subscription and publishers cost over the
//! statements they run. `framework` against `adapter` is what the runtime costs on top of them.
//!
//! The outbox is a plugin rather than a broker, so its scenarios compare one app over one broker,
//! Redis Pub/Sub through `ruststream-fred`, in three variants that take the three places: the app
//! with no outbox (`raw`), the same app with the outbox written by hand in raw sqlx (`adapter`),
//! and the same app with this crate's outbox (`framework`). The outbox against no outbox is the
//! plugin's whole cost; against the hand-written one it is what the crate's machinery adds.
//!
//! The procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! A run recreates its table and fills it, then drains it. The window runs from the first
//! handled delivery to the last, so starting the service and connecting the pool sit outside it.
//! A publish scenario writes its rows instead, and its window runs from the first publish to the
//! last. An outbox scenario feeds its app from a worker and keeps at most `IN_FLIGHT` messages
//! unconsumed, because Redis Pub/Sub drops what a subscription's buffer cannot hold.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The three are interleaved round after round (raw, adapter, framework, raw, adapter,
//! framework) and each reports its best, median and worst round. The best is the headline: noise
//! only ever slows a run down. A difference smaller than the spread between the rounds of either
//! loop is reported as indistinguishable, never as a figure.
//!
//! # What the numbers do not say
//!
//! A row is marked broker-bound when the raw loop spent the run waiting on the database rather
//! than working. That is decided from a measurement: the probe times one statement the server
//! answers, outside every pair, and the row is marked when the round trips a message costs
//! ([`Scenario::round_trips`]) come to at least half the time a message took. The figure is
//! published with the results, so the arithmetic can be checked.

mod common;

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::future::Future;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Duration;

use common::outbox::{self, Reply, TRACKED, UNTRACKED, record_header};
use common::raw::{self, Statements, read_payload};
use common::stand::{Table, postgres_fill, postgres_pool, postgres_table};
use common::tables::{AdvisoryJob, LeaseJob, NamedJob, OrderRow, RowLockJob};
use common::timing::{
    Stats, against, broker_bound, describe_round_trip, number, rate, round_trip, runtime,
};
use common::{BATCH, LEASE, Latch, OrderPlaced, adapter, framework};
use framework::Mount;
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};
use ruststream_sqlx::prelude::*;
use serde::Serialize;
use sqlx::PgPool;

/// Messages the probe run takes to measure the raw loop's rate.
const PROBE_MESSAGES: usize = 2_000;
/// The shortest a measured run may last, unless `RUSTSTREAM_BENCH_SECONDS` says otherwise.
const SECONDS: f64 = 5.0;
/// Headroom on the probed count: a probe that ran a little fast still leaves every run at least
/// [`SECONDS`] long.
const MARGIN: f64 = 1.25;
/// The most messages a run takes, whatever the probe measured.
const MAX_MESSAGES: usize = 500_000;
/// Rounds run, unless `RUSTSTREAM_BENCH_PAIRS` says otherwise.
const PAIRS: usize = 3;
/// Connections every loop's pool may open: the claim's, the settlement's or the publish's, and
/// room to spare.
const POOL: u32 = 4;

/// Starts `$app`, pairs its publisher and turns it into what publishes (`$wrap`), and publishes
/// `$message` once per expected delivery from a worker, counting each.
macro_rules! publish_from_outside {
    ($app:expr, $egress:expr, $latch:expr, $message:expr, |$live:ident| $wrap:expr) => {{
        let running = $app.start().await.expect("the service starts");
        let $live = running
            .publisher($egress)
            .await
            .expect("the publisher pairs");
        let publisher = $wrap;
        let state: Latch = Latch::clone($latch);
        on_a_worker(async move {
            for _ in 0..state.total() {
                publisher
                    .message(&$message)
                    .publish()
                    .await
                    .expect("the publish goes out");
                state.arrived();
            }
        })
        .await;
        running.shutdown().await.expect("the service stops");
    }};
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Consume,
    Reply,
    Batch,
    Lease,
    Advisory,
    ByName,
    RowMode,
    Repository,
    Routed,
    OutboxUntracked,
    OutboxPublish,
    OutboxDelivery,
    OutboxRoundTrip,
}

impl Scenario {
    const ALL: [Self; 13] = [
        Self::Consume,
        Self::Reply,
        Self::Batch,
        Self::Lease,
        Self::Advisory,
        Self::ByName,
        Self::RowMode,
        Self::Repository,
        Self::Routed,
        Self::OutboxUntracked,
        Self::OutboxPublish,
        Self::OutboxDelivery,
        Self::OutboxRoundTrip,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Consume => "row lock claim, JSON decode into a small struct, delete each",
            Self::Reply => {
                "row lock claim, reply inserted through this crate's default publisher, delete each"
            }
            Self::Batch => "row lock claim in batches of 64, delete each",
            Self::Lease => "lease claim, JSON decode into a small struct, delete each",
            Self::Advisory => "advisory lock claim, JSON decode into a small struct, delete each",
            Self::ByName => "by-name subscription, row lock claim, delete each",
            Self::RowMode => "row mode, the row decoded by the driver, delete each",
            Self::Repository => "repository publish, one insert each",
            Self::Routed => "routed publish, one insert each",
            Self::OutboxUntracked => "outbox plugin, an untracked message through both layers",
            Self::OutboxPublish => "outbox plugin, a tracked publish from outside a handler",
            Self::OutboxDelivery => "outbox plugin, a tracked delivery fetched and marked",
            Self::OutboxRoundTrip => {
                "outbox plugin, a tracked round trip: recorded, delivered, fetched and marked"
            }
        }
    }

    /// Whether the three loops are variants of one app with a plugin, and which plugin.
    const fn plugin(self) -> Option<&'static str> {
        match self {
            Self::OutboxUntracked
            | Self::OutboxPublish
            | Self::OutboxDelivery
            | Self::OutboxRoundTrip => Some("outbox"),
            _ => None,
        }
    }

    const fn table(self) -> Table {
        match self {
            Self::Consume | Self::Reply | Self::Batch => Table::RowLock,
            Self::Lease => Table::Lease,
            Self::Advisory => Table::Advisory,
            Self::ByName | Self::Repository | Self::Routed => Table::Named,
            Self::RowMode => Table::RowMode,
            Self::OutboxUntracked
            | Self::OutboxPublish
            | Self::OutboxDelivery
            | Self::OutboxRoundTrip => Table::Outbox,
        }
    }

    /// Whether the run drains a table it filled before the window opens.
    const fn drains(self) -> bool {
        self.plugin().is_none() && !matches!(self, Self::Repository | Self::Routed)
    }

    /// Whether the scenario has a middle loop. Written by hand, an untracked message costs
    /// nothing, so that row's hand-written app is the one with no outbox.
    const fn has_adapter(self) -> bool {
        !matches!(self, Self::OutboxUntracked)
    }

    /// Round trips to the database a message costs the raw loop, which is what decides whether
    /// the run was paced by the database rather than by the code being measured.
    ///
    /// Every statement is one, `BEGIN` and `COMMIT` included, and so is every connection the loop
    /// takes from the pool: sqlx's pool pings a connection before it lends it. The row lock form
    /// takes a connection, begins, claims, deletes and commits: five, and a reply adds a
    /// connection and an insert. A batch takes a connection, begins, claims and commits once per
    /// batch. The lease form claims on one connection and deletes on another: four. The advisory
    /// lock form takes a connection, finds a candidate, locks its key, takes the row, deletes it
    /// and unlocks: six. A publish takes a connection and inserts. The outbox rows' raw loop is
    /// the app with no outbox, which makes no round trip to the database.
    fn round_trips(self) -> f64 {
        let batch = BATCH.get() as f64;
        match self {
            Self::Consume | Self::ByName | Self::RowMode => 5.0,
            Self::Reply => 7.0,
            Self::Batch => (4.0 + batch) / batch,
            Self::Lease => 4.0,
            Self::Advisory => 6.0,
            Self::Repository | Self::Routed => 2.0,
            Self::OutboxUntracked
            | Self::OutboxPublish
            | Self::OutboxDelivery
            | Self::OutboxRoundTrip => 0.0,
        }
    }

    /// One loop of one run: a fresh table, filled where the scenario drains it, then the loop.
    async fn run(self, kind: Loop, messages: usize) -> Duration {
        let pool = postgres_pool(POOL);
        postgres_table(&pool, self.table()).await;
        if matches!(self, Self::Reply) {
            postgres_table(&pool, Table::Replies).await;
        }
        if self.drains() {
            postgres_fill(&pool, self.table(), messages).await;
        }
        let latch = Latch::new(messages);
        match kind {
            Loop::Raw => self.raw(pool.clone(), latch.clone()).await,
            Loop::Adapter => self.adapter(pool.clone(), latch.clone()).await,
            Loop::Framework => self.framework(pool.clone(), latch.clone()).await,
        }
        pool.close().await;
        latch.window()
    }

    /// sqlx driven by hand, on a worker of the runtime; for an outbox row, the app with no
    /// outbox.
    async fn raw(self, pool: PgPool, latch: Latch) {
        match self {
            Self::Consume | Self::Batch => {
                let statements =
                    Statements::row_lock(&Postgres, &RowLockJob::TABLE.spec(), ClaimShape::Rows);
                let limit = if matches!(self, Self::Batch) {
                    BATCH.get()
                } else {
                    1
                };
                on_a_worker(async move {
                    raw::row_lock(&pool, &statements, limit, &latch, |job: &RowLockJob| {
                        read_payload(&job.payload);
                    })
                    .await;
                })
                .await;
            }
            Self::Reply => {
                let statements =
                    Statements::row_lock(&Postgres, &RowLockJob::TABLE.spec(), ClaimShape::Rows);
                on_a_worker(async move { raw::reply(&pool, &statements, &latch).await }).await;
            }
            Self::Lease => {
                let statements = Statements::lease(&Postgres, &LeaseJob::TABLE.spec());
                on_a_worker(async move {
                    raw::lease(&pool, &statements, 1, LEASE, &latch, |job: &LeaseJob| {
                        read_payload(&job.payload);
                    })
                    .await;
                })
                .await;
            }
            Self::Advisory => {
                let statements = Statements::advisory(&Postgres, &AdvisoryJob::TABLE.spec());
                on_a_worker(async move {
                    raw::advisory(&pool, &statements, &latch, |job: &AdvisoryJob| {
                        read_payload(&job.payload);
                    })
                    .await;
                })
                .await;
            }
            Self::ByName => {
                let statements =
                    Statements::row_lock(&Postgres, &NamedJob::TABLE.spec(), ClaimShape::Roles);
                on_a_worker(async move {
                    raw::row_lock(&pool, &statements, 1, &latch, |job: &NamedJob| {
                        read_payload(&job.payload);
                    })
                    .await;
                })
                .await;
            }
            Self::RowMode => {
                let statements =
                    Statements::row_lock(&Postgres, &OrderRow::TABLE.spec(), ClaimShape::Rows);
                on_a_worker(async move {
                    raw::row_lock(&pool, &statements, 1, &latch, |order: &OrderRow| {
                        black_box((order.customer.len(), order.quantity));
                    })
                    .await;
                })
                .await;
            }
            Self::Repository | Self::Routed => {
                on_a_worker(async move { raw::publish(&pool, &latch).await }).await;
            }
            Self::OutboxUntracked | Self::OutboxRoundTrip => {
                drop(pool);
                outbox::feed_round_trip(
                    outbox::redis::round_trip(latch.clone(), NonZeroUsize::MIN),
                    &latch,
                )
                .await;
            }
            Self::OutboxPublish => {
                drop(pool);
                let (app, egress) = outbox::redis::publishing();
                publish_from_outside!(app, egress, &latch, Reply { id: 1 }, |publisher| publisher);
            }
            Self::OutboxDelivery => {
                let (app, egress) = outbox::redis::delivery(latch.clone());
                deliver(app, egress, &pool, &latch).await;
            }
        }
    }

    /// This crate driven by hand, on a worker of the runtime; for an outbox row, the app with
    /// the outbox written by hand.
    async fn adapter(self, pool: PgPool, latch: Latch) {
        match self {
            Self::Consume => on_a_worker(adapter::row_lock(pool, latch)).await,
            Self::Reply => on_a_worker(adapter::reply(pool, latch)).await,
            Self::Batch => on_a_worker(adapter::batch(pool, latch)).await,
            Self::Lease => on_a_worker(adapter::lease(pool, latch)).await,
            Self::Advisory => on_a_worker(adapter::advisory(pool, latch)).await,
            Self::ByName => on_a_worker(adapter::by_name(pool, latch)).await,
            Self::RowMode => on_a_worker(adapter::row_mode(pool, latch)).await,
            Self::Repository => on_a_worker(adapter::repository(pool, latch)).await,
            Self::Routed => on_a_worker(adapter::routed(pool, latch)).await,
            Self::OutboxUntracked => unreachable!("the untracked row has no hand-written variant"),
            Self::OutboxPublish => {
                let (app, egress) = outbox::redis::publishing();
                publish_by_hand(app, egress, pool, &latch).await;
            }
            Self::OutboxDelivery => {
                let (app, egress) = outbox::redis::delivery_by_hand(pool.clone(), latch.clone());
                deliver(app, egress, &pool, &latch).await;
            }
            Self::OutboxRoundTrip => {
                let built =
                    outbox::redis::round_trip_by_hand(pool, latch.clone(), NonZeroUsize::MIN);
                outbox::feed_round_trip(built, &latch).await;
            }
        }
    }

    /// The service a user writes; for an outbox row, the app with this crate's outbox.
    async fn framework(self, pool: PgPool, latch: Latch) {
        match self {
            Self::Consume => {
                let app = framework::postgres_row_lock(pool, latch.clone(), Mount::SEQUENTIAL);
                drain(app, &latch).await;
            }
            Self::Reply => drain(framework::postgres_reply(pool, latch.clone()), &latch).await,
            Self::Batch => {
                let how = Mount {
                    batch: Some(BATCH),
                    ..Mount::SEQUENTIAL
                };
                drain(
                    framework::postgres_row_lock(pool, latch.clone(), how),
                    &latch,
                )
                .await;
            }
            Self::Lease => {
                let app = framework::postgres_lease(pool, latch.clone(), Mount::SEQUENTIAL);
                drain(app, &latch).await;
            }
            Self::Advisory => {
                drain(framework::postgres_advisory(pool, latch.clone()), &latch).await;
            }
            Self::ByName => drain(framework::postgres_by_name(pool, latch.clone()), &latch).await,
            Self::RowMode => {
                drain(framework::postgres_row_mode(pool, latch.clone()), &latch).await;
            }
            Self::Repository => {
                let (app, egress) = framework::postgres_repository(pool);
                publish_from_outside!(app, egress, &latch, OrderPlaced::fixed(), |publisher| {
                    publisher
                });
            }
            Self::Routed => {
                let (app, egress) = framework::postgres_routed(pool);
                publish_from_outside!(app, egress, &latch, OrderPlaced::fixed(), |publisher| {
                    publisher
                });
            }
            Self::OutboxUntracked | Self::OutboxRoundTrip => {
                let name = if matches!(self, Self::OutboxRoundTrip) {
                    TRACKED
                } else {
                    UNTRACKED
                };
                let built =
                    outbox::redis::round_trip_outbox(pool, latch.clone(), name, NonZeroUsize::MIN);
                outbox::feed_round_trip(built, &latch).await;
            }
            Self::OutboxPublish => {
                let (app, egress, tracking) = outbox::redis::publishing_outbox(pool);
                publish_from_outside!(app, egress, &latch, Reply { id: 1 }, |publisher| {
                    tracking.wrap(publisher)
                });
            }
            Self::OutboxDelivery => {
                let (app, egress) = outbox::redis::delivery_outbox(pool.clone(), latch.clone());
                deliver(app, egress, &pool, &latch).await;
            }
        }
    }
}

/// Which of a scenario's three loops a run drives.
#[derive(Clone, Copy, Debug)]
enum Loop {
    Raw,
    Adapter,
    Framework,
}

/// Runs `work` on a worker of the runtime, where a service's subscription runs too, rather than
/// on the thread that drives the benchmark.
async fn on_a_worker(work: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(work).await.expect("the measured task ends");
}

/// Starts `app` and waits until its subscription has handled every row of the table.
async fn drain(app: impl App, latch: &Latch) {
    let running = app.start().await.expect("the service starts");
    latch.drained_or_stalled("framework").await;
    running.shutdown().await.expect("the service stops");
}

/// Starts an outbox app that consumes tracked messages, fills the outbox table with one
/// unprocessed record per message once the app runs (so its republish at startup finds none),
/// and publishes one message per record, carrying the record's id.
async fn deliver(app: impl App, egress: outbox::redis::Egress, pool: &PgPool, latch: &Latch) {
    let running = app.start().await.expect("the service starts");
    postgres_fill(pool, Table::Outbox, latch.total()).await;
    let publisher = running
        .publisher(egress)
        .await
        .expect("the publisher pairs");
    let state = latch.clone();
    on_a_worker(async move {
        for sent in 0..state.total() {
            outbox::throttle(sent, &state).await;
            publisher
                .message(&Reply { id: 1 })
                .with_headers(record_header(i64::try_from(sent).expect("an id fits") + 1))
                .publish()
                .await
                .expect("the message is published");
        }
    })
    .await;
    latch.drained_or_stalled("outbox").await;
    running.shutdown().await.expect("the service stops");
}

/// The hand-written tracked publish: the record inserted through the service's own pool, then
/// the message sent with the record's id in a header.
async fn publish_by_hand(
    app: impl App,
    egress: outbox::redis::Egress,
    pool: PgPool,
    latch: &Latch,
) {
    let running = app.start().await.expect("the service starts");
    let publisher = running
        .publisher(egress)
        .await
        .expect("the publisher pairs");
    let state = latch.clone();
    on_a_worker(async move {
        let mut body = Vec::new();
        for _ in 0..state.total() {
            body.clear();
            serde_json::to_writer(&mut body, &Reply { id: 1 }).expect("the message encodes");
            let id = outbox::ByHand::record(&pool, &body).await;
            publisher
                .message(&Reply { id: 1 })
                .with_headers(record_header(id))
                .publish()
                .await
                .expect("the message is published");
            state.arrived();
        }
    })
    .await;
    running.shutdown().await.expect("the service stops");
}

/// One scenario's line of the document, in the family's schema.
#[derive(Debug, Serialize)]
struct Measured {
    name: &'static str,
    unit: &'static str,
    /// The plugin whose variants the three loops are, for an outbox row.
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin: Option<&'static str>,
    messages: usize,
    pairs: usize,
    raw: Stats,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapter: Option<Stats>,
    framework: Stats,
    /// The framework loop against the raw one: what a reader pays over writing the loop by hand;
    /// for an outbox row, the plugin's whole cost.
    overhead_percent: f64,
    verdict: &'static str,
    /// The adapter against the raw loop: the number this repository owns; for an outbox row,
    /// what an outbox written by hand costs.
    #[serde(skip_serializing_if = "Option::is_none")]
    adapter_overhead_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapter_verdict: Option<&'static str>,
    /// For an outbox row, this crate's outbox against the one written by hand: what the crate's
    /// machinery adds.
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_overhead_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_verdict: Option<&'static str>,
    broker_bound: bool,
}

#[derive(Debug, Serialize)]
struct Document {
    round_trip_us: f64,
    scenarios: Vec<Measured>,
}

async fn measure(scenario: Scenario, pairs: usize, seconds: f64, round_trip: Duration) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = rate(
        PROBE_MESSAGES,
        scenario.run(Loop::Raw, PROBE_MESSAGES).await,
    );
    let messages = ((probe * seconds * MARGIN) as usize).clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({probe:.0} msg/s probed)",
        scenario.name()
    );
    let loops: &[Loop] = if scenario.has_adapter() {
        &[Loop::Raw, Loop::Adapter, Loop::Framework]
    } else {
        &[Loop::Raw, Loop::Framework]
    };
    let mut raws = Vec::with_capacity(pairs);
    let mut adapters = Vec::with_capacity(pairs);
    let mut frameworks = Vec::with_capacity(pairs);
    for round in 1..=pairs {
        let mut line = format!("  round {round:>2}:");
        for &kind in loops {
            let measured = rate(messages, scenario.run(kind, messages).await);
            let (label, rates) = match kind {
                Loop::Raw => ("raw", &mut raws),
                Loop::Adapter => ("adapter", &mut adapters),
                Loop::Framework => ("framework", &mut frameworks),
            };
            write!(line, " {label} {measured:>10.0}").expect("a string takes the text");
            rates.push(measured);
        }
        println!("{line} msg/s");
    }
    let raw = Stats::of(&raws);
    let adapter = (!adapters.is_empty()).then(|| Stats::of(&adapters));
    let framework = Stats::of(&frameworks);
    let (overhead_percent, verdict) = against(raw, framework);
    let adapter_against = adapter.map(|adapter| against(raw, adapter));
    let plugin_against = adapter
        .filter(|_| scenario.plugin().is_some())
        .map(|adapter| against(adapter, framework));
    Measured {
        name: scenario.name(),
        unit: "msg/s",
        plugin: scenario.plugin(),
        messages,
        pairs,
        raw,
        adapter,
        framework,
        overhead_percent,
        verdict,
        adapter_overhead_percent: adapter_against.map(|(percent, _)| percent),
        adapter_verdict: adapter_against.map(|(_, verdict)| verdict),
        plugin_overhead_percent: plugin_against.map(|(percent, _)| percent),
        plugin_verdict: plugin_against.map(|(_, verdict)| verdict),
        broker_bound: broker_bound(scenario.round_trips(), round_trip, raw),
    }
}

fn main() {
    let pairs = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    // Outside every pair, and once: what a statement the loop waits for costs on this database.
    let round_trip = runtime.block_on(async {
        let pool = postgres_pool(1);
        let measured = round_trip(&pool).await;
        pool.close().await;
        measured
    });
    println!("round trip: {}", describe_round_trip(round_trip));
    let measured: Vec<Measured> = Scenario::ALL
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, pairs, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        let adapter = match (
            row.adapter,
            row.adapter_overhead_percent,
            row.adapter_verdict,
        ) {
            (Some(adapter), Some(percent), Some(verdict)) => {
                format!(", adapter {:.0} ({percent:.1}%, {verdict})", adapter.best)
            }
            _ => String::new(),
        };
        println!(
            "{}: raw {:.0}{adapter}, framework {:.0} ({:.1}%, {}) msg/s{}",
            row.name,
            row.raw.best,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }
    let document = Document {
        round_trip_us: round_trip.as_secs_f64() * 1e6,
        scenarios: measured,
    };
    let text = serde_json::to_string_pretty(&document).expect("the summary serializes");
    fs::write(&out, text + "\n").expect("the summary is written");
    println!("\nwrote {out}");
}
