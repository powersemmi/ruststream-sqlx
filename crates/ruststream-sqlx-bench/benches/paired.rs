// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a database fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(missing_docs, unreachable_pub, unused_qualifications)]
//! What this crate costs over a raw sqlx loop, in wall-clock time on a multi-threaded runtime.
//!
//! Every scenario of the code-cost benchmarks, run two ways against the stand's Postgres. **Raw**
//! is a hand-written sqlx loop running the statements the broker runs, on a task of the runtime.
//! **Service** is the application a user writes, started with `RustStream::start`. The two share
//! the table, the fill, the pool's settings, the decode, the runtime and the binary.
//!
//! # What a run is
//!
//! A run recreates its table and fills it with [`ROWS`] rows (or `RUSTSTREAM_BENCH_ROWS`), then
//! drains it. The window runs from the first handled delivery to the last, so starting the
//! service and connecting the pool sit outside it. A publish scenario has no table to drain: its
//! window runs from the first publish to the last. An outbox scenario publishes its commands into
//! `MemoryBroker`, and its window runs from the first reply consumed to the last.
//!
//! The halves are interleaved - raw, service, round after round - and each reports its best,
//! median and worst round. The best is the headline: noise only ever slows a run down. A
//! difference smaller than the spread between the rounds of either half is reported as
//! indistinguishable, never as a figure.

mod common;

use std::env;
use std::fs;
use std::future::Future;
use std::hint::black_box;
use std::time::Duration;

use common::raw::{self, Statements, read_payload};
use common::services::{self, Command, Mount, TRACKED};
use common::stand::{Table, postgres_fill, postgres_pool, postgres_table};
use common::tables::{AdvisoryJob, LeaseJob, NamedJob, OrderRow, RowLockJob};
use common::timing::{Stats, against, number, rate, round_trip, runtime};
use common::{BATCH, LEASE, Latch, OrderPlaced};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::Bound;
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};
use ruststream_sqlx::prelude::*;
use serde::Serialize;
use sqlx::PgPool;

/// Rows a run drains, unless `RUSTSTREAM_BENCH_ROWS` says otherwise.
const ROWS: usize = 10_000;
/// Rounds run, unless `RUSTSTREAM_BENCH_PAIRS` says otherwise.
const PAIRS: usize = 3;
/// Connections either half's pool may open: the claim's, the settlement's or the publish's, and
/// room to spare.
const POOL: u32 = 4;

#[derive(Clone, Copy, Debug)]
enum Scenario {
    RowLock,
    Lease,
    Advisory,
    ByName,
    Batch,
    RowMode,
    Repository,
    Routed,
    OutboxTracked,
    OutboxUntracked,
}

impl Scenario {
    const ALL: [Self; 10] = [
        Self::RowLock,
        Self::Lease,
        Self::Advisory,
        Self::ByName,
        Self::Batch,
        Self::RowMode,
        Self::Repository,
        Self::Routed,
        Self::OutboxTracked,
        Self::OutboxUntracked,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::RowLock => "Row lock claim and delete, JSON payload",
            Self::Lease => "Lease claim and delete, JSON payload",
            Self::Advisory => "Advisory lock claim and delete, JSON payload",
            Self::ByName => "By-name subscription, row lock claim and delete",
            Self::Batch => "Row lock batches of 16, delete each",
            Self::RowMode => "Row mode, row lock claim and delete",
            Self::Repository => "Repository publish, one insert each",
            Self::Routed => "Routed publish, one insert each",
            Self::OutboxTracked => "Outbox: a reply recorded, fetched and marked",
            Self::OutboxUntracked => "Outbox: both middlewares, nothing tracked",
        }
    }

    const fn table(self) -> Table {
        match self {
            Self::RowLock | Self::Batch => Table::RowLock,
            Self::Lease => Table::Lease,
            Self::Advisory => Table::Advisory,
            Self::ByName | Self::Repository | Self::Routed => Table::Named,
            Self::RowMode => Table::RowMode,
            Self::OutboxTracked | Self::OutboxUntracked => Table::Outbox,
        }
    }

    /// Whether the run drains a filled table, rather than producing what it measures.
    const fn drains(self) -> bool {
        !matches!(
            self,
            Self::Repository | Self::Routed | Self::OutboxTracked | Self::OutboxUntracked
        )
    }

    /// One half of one run: a fresh table, filled where the scenario drains it, then the half.
    async fn run(self, service: bool, rows: usize) -> Duration {
        let pool = postgres_pool(POOL);
        postgres_table(&pool, self.table()).await;
        if self.drains() {
            postgres_fill(&pool, self.table(), rows).await;
        }
        let latch = Latch::new(rows);
        if service {
            self.service(pool.clone(), &latch).await;
        } else {
            self.raw(pool.clone(), &latch).await;
        }
        pool.close().await;
        latch.window()
    }

    async fn service(self, pool: PgPool, latch: &Latch) {
        let state = latch.clone();
        match self {
            Self::RowLock => {
                drain(
                    services::postgres_row_lock(pool, state, Mount::SEQUENTIAL),
                    latch,
                )
                .await;
            }
            Self::Lease => {
                drain(
                    services::postgres_lease(pool, state, Mount::SEQUENTIAL),
                    latch,
                )
                .await;
            }
            Self::Advisory => drain(services::postgres_advisory(pool, state), latch).await,
            Self::ByName => drain(services::postgres_by_name(pool, state), latch).await,
            Self::Batch => {
                let how = Mount {
                    batch: Some(BATCH),
                    ..Mount::SEQUENTIAL
                };
                drain(services::postgres_row_lock(pool, state, how), latch).await;
            }
            Self::RowMode => drain(services::postgres_row_mode(pool, state), latch).await,
            Self::Repository => {
                let (app, egress) = services::postgres_repository(pool);
                let running = app.start().await.expect("the service starts");
                let publisher = running
                    .publisher(egress)
                    .await
                    .expect("the publisher pairs");
                on_a_worker(async move {
                    for _ in 0..state.total() {
                        publisher
                            .message(&OrderPlaced::fixed())
                            .publish()
                            .await
                            .expect("the publish writes a row");
                        state.arrived();
                    }
                })
                .await;
                running.shutdown().await.expect("the service stops");
            }
            Self::Routed => {
                let (app, egress) = services::postgres_routed(pool);
                let running = app.start().await.expect("the service starts");
                let publisher = running
                    .publisher(egress)
                    .await
                    .expect("the publisher pairs");
                on_a_worker(async move {
                    for _ in 0..state.total() {
                        publisher
                            .message(&OrderPlaced::fixed())
                            .publish()
                            .await
                            .expect("the publish writes a row");
                        state.arrived();
                    }
                })
                .await;
                running.shutdown().await.expect("the service stops");
            }
            Self::OutboxTracked => relay(services::outbox(pool, state, TRACKED), latch).await,
            Self::OutboxUntracked => {
                relay(services::outbox(pool, state, "elsewhere"), latch).await;
            }
        }
    }

    async fn raw(self, pool: PgPool, latch: &Latch) {
        let state = latch.clone();
        match self {
            Self::RowLock | Self::Batch => {
                let statements =
                    Statements::row_lock(&Postgres, &RowLockJob::TABLE.spec(), ClaimShape::Rows);
                let limit = if matches!(self, Self::Batch) {
                    BATCH.get()
                } else {
                    1
                };
                on_a_worker(async move {
                    raw::row_lock(&pool, &statements, limit, &state, |job: &RowLockJob| {
                        read_payload(&job.payload);
                    })
                    .await;
                })
                .await;
            }
            Self::Lease => {
                let statements = Statements::lease(&Postgres, &LeaseJob::TABLE.spec());
                on_a_worker(async move {
                    raw::lease(&pool, &statements, 1, LEASE, &state, |job: &LeaseJob| {
                        read_payload(&job.payload);
                    })
                    .await;
                })
                .await;
            }
            Self::Advisory => {
                let statements = Statements::advisory(&Postgres, &AdvisoryJob::TABLE.spec());
                on_a_worker(async move {
                    raw::advisory(&pool, &statements, &state, |job: &AdvisoryJob| {
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
                    raw::row_lock(&pool, &statements, 1, &state, |job: &NamedJob| {
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
                    raw::row_lock(&pool, &statements, 1, &state, |order: &OrderRow| {
                        black_box((order.customer.len(), order.quantity));
                    })
                    .await;
                })
                .await;
            }
            Self::Repository | Self::Routed => {
                on_a_worker(async move { raw::publish(&pool, &state).await }).await;
            }
            Self::OutboxTracked => relay(services::outbox_by_hand(pool, state), latch).await,
            Self::OutboxUntracked => relay(services::outbox_bare(state), latch).await,
        }
    }
}

/// Runs `work` on a worker of the runtime, where a service's subscription runs too, rather than
/// on the thread that drives the benchmark.
async fn on_a_worker(work: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(work).await.expect("the measured task ends");
}

/// Starts `app` and waits until its subscription has handled every row of the table.
async fn drain(app: impl App, latch: &Latch) {
    let running = app.start().await.expect("the service starts");
    latch.drained_or_stalled("service").await;
    running.shutdown().await.expect("the service stops");
}

/// Starts an outbox service, publishes one command per expected reply from a worker, and waits
/// until the sink has consumed every reply.
async fn relay((app, egress): (impl App, Bound<MemoryBroker, MemoryPublish>), latch: &Latch) {
    let running = app.start().await.expect("the service starts");
    let publisher = running
        .publisher(egress)
        .await
        .expect("the publisher pairs");
    let total = latch.total();
    on_a_worker(async move {
        for _ in 0..total {
            publisher
                .message(&Command { id: 1 })
                .publish()
                .await
                .expect("the command is published");
        }
    })
    .await;
    latch.drained_or_stalled("outbox").await;
    running.shutdown().await.expect("the service stops");
}

/// One scenario's line of the document, in the family's schema.
#[derive(Debug, Serialize)]
struct Measured {
    name: &'static str,
    unit: &'static str,
    messages: usize,
    pairs: usize,
    raw: Stats,
    framework: Stats,
    overhead_percent: f64,
    verdict: &'static str,
}

#[derive(Debug, Serialize)]
struct Document {
    round_trip: String,
    scenarios: Vec<Measured>,
}

async fn measure(scenario: Scenario, rows: usize, rounds: usize) -> Measured {
    // A warm-up of each half, thrown away: the server's caches and the binary's first calls.
    let warm = (rows / 10).max(1);
    scenario.run(false, warm).await;
    scenario.run(true, warm).await;
    let mut raw_rates = Vec::with_capacity(rounds);
    let mut service_rates = Vec::with_capacity(rounds);
    for round in 1..=rounds {
        let raw = rate(rows, scenario.run(false, rows).await);
        let service = rate(rows, scenario.run(true, rows).await);
        println!("  round {round:>2}: raw {raw:>9.0}, service {service:>9.0} msg/s");
        raw_rates.push(raw);
        service_rates.push(service);
    }
    let raw = Stats::of(&raw_rates);
    let framework = Stats::of(&service_rates);
    let (overhead_percent, verdict) = against(raw, framework);
    Measured {
        name: scenario.name(),
        unit: "msg/s",
        messages: rows,
        pairs: rounds,
        raw,
        framework,
        overhead_percent,
        verdict,
    }
}

fn main() {
    let rows = number("RUSTSTREAM_BENCH_ROWS", ROWS);
    let rounds = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    let round_trip = runtime.block_on(async {
        let pool = postgres_pool(1);
        let measured = round_trip(&pool).await;
        pool.close().await;
        measured
    });
    println!("round trip: {round_trip}");
    let mut scenarios = Vec::new();
    for scenario in Scenario::ALL {
        println!("{}: {rows} messages per run", scenario.name());
        scenarios.push(runtime.block_on(measure(scenario, rows, rounds)));
    }

    println!();
    for row in &scenarios {
        println!(
            "{}: raw {:.0}, service {:.0} msg/s, {:.1}% ({})",
            row.name, row.raw.best, row.framework.best, row.overhead_percent, row.verdict
        );
    }
    let document = Document {
        round_trip,
        scenarios,
    };
    let text = serde_json::to_string_pretty(&document).expect("the summary serializes");
    fs::write(&out, text + "\n").expect("the summary is written");
    println!("\nwrote {out}");
}
