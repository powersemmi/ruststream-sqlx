// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a database fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(missing_docs, unreachable_pub, unused_qualifications)]
//! How many messages a second a queue table drains at, on Postgres, MySQL and SQLite, through a
//! raw sqlx loop, through this crate driven by hand, and through the service, with the same
//! concurrency; and how many tracked round trips a second the outbox carries over Redis Pub/Sub,
//! in the three variants of the app the wall clock compares.
//!
//! # What a run is
//!
//! A run recreates its table, fills it with [`ROWS`] rows (or `RUSTSTREAM_BENCH_THROUGHPUT_ROWS`),
//! and drains it on a pool of [`POOL`] connections. The service mounts its handler with
//! `workers(n)` (one worker is the plain mount, which handles a delivery before it pulls the
//! next), single deliveries or batches of 64. The raw loop runs n tasks on one pool, each a loop
//! that claims, reads and deletes until its claim comes back empty: the same statements, the
//! same concurrency. The adapter runs n tasks on one pool, each a `SqlxBroker` of its own reading
//! a subscription through `InboxQueue`: the crate's code with the raw loop's shape of
//! concurrency, where the service claims for all of its workers through one subscription.
//!
//! The outbox cells mount the relay and the sink with `workers(n)` in each variant: no outbox,
//! the outbox written by hand, and this crate's outbox.
//!
//! Postgres and MySQL take the row lock form. SQLite takes the lease form: it has no row locks,
//! and a lease claim is one statement there.
//!
//! The window runs from the first handled delivery to the last. Every handled delivery counts a
//! latch down, in the handler and in the raw loop alike, and the last count wakes the waiter
//! through a `Notify`: the run knows it drained without polling the table or sleeping. The loops
//! are interleaved, round after round, and each reports its best, median and worst round.

mod common;

use std::env;
use std::fs;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use common::framework::{self, JOBS, Mount};
use common::outbox::{self, TRACKED};
use common::raw::{self, Statements, read_payload};
use common::stand::{
    Table, mysql_fill, mysql_pool, mysql_table, postgres_fill, postgres_pool, postgres_table,
    sqlite_fill, sqlite_pool,
};
use common::tables::{LeaseJob, RowLockJob};
use common::timing::{Stats, against, number, rate, runtime};
use common::{BATCH, LEASE, Latch, adapter};
use futures::future::join_all;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, SubscriptionSource};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{self, ClaimShape};
use ruststream_sqlx::prelude::*;
use serde::Serialize;
use sqlx::SqlitePool;

/// Rows a run drains, unless `RUSTSTREAM_BENCH_THROUGHPUT_ROWS` says otherwise.
const ROWS: usize = 20_000;
/// Rounds run, unless `RUSTSTREAM_BENCH_PAIRS` says otherwise.
const PAIRS: usize = 3;
/// The pool both halves drain on: room for eight workers in the row lock form, each holding the
/// connection of its claim, with the subscription's next claim beside them.
const POOL: u32 = 16;
/// The worker counts a run is taken at.
const WORKERS: [usize; 3] = [1, 4, 8];

/// Which of a cell's three loops a run drives; for an outbox cell, which variant of the app.
#[derive(Clone, Copy, Debug)]
enum Loop {
    Raw,
    Adapter,
    Framework,
}

/// Opens `$tasks` subscriptions of `$row`'s table over `$pool`, each read on a task of its own,
/// single deliveries or batches of `$batch`, until `$latch` is drained; the tasks still waiting
/// on an empty table are then stopped.
macro_rules! adapter_drain {
    ($pool:expr, $row:ty, $tasks:expr, $batch:expr, $latch:expr) => {{
        // A broker serves one subscription per name, so each reader connects a broker of its
        // own on the shared pool: n claimers, as the raw loop runs n.
        let mut brokers = Vec::with_capacity($tasks);
        let mut readers = Vec::with_capacity($tasks);
        for _ in 0..$tasks {
            let connected = SqlxBroker::new($pool.clone())
                .connect()
                .await
                .expect("the broker connects");
            let subscriber = InboxQueue::<$row>::new(JOBS)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            brokers.push(connected);
            let latch: Latch = Latch::clone(&$latch);
            let batch: Option<NonZeroUsize> = $batch;
            readers.push(tokio::spawn(async move {
                match batch {
                    None => {
                        adapter::consume(subscriber, &latch, |delivery| {
                            read_payload(delivery.payload());
                        })
                        .await;
                    }
                    Some(size) => adapter::consume_batches(subscriber, size, &latch).await,
                }
            }));
        }
        $latch.drained_or_stalled("adapter").await;
        for reader in &readers {
            reader.abort();
        }
        for reader in readers {
            // A reader that saw the last delivery returned; the others were stopped waiting.
            let _ = reader.await;
        }
        for connected in brokers {
            connected.shutdown().await.expect("the broker shuts down");
        }
    }};
}

#[derive(Clone, Copy, Debug, Serialize)]
enum Database {
    Postgres,
    MySql,
    Sqlite,
}

impl Database {
    const ALL: [Self; 3] = [Self::Postgres, Self::MySql, Self::Sqlite];

    const fn name(self) -> &'static str {
        match self {
            Self::Postgres => "Postgres",
            Self::MySql => "MySQL",
            Self::Sqlite => "SQLite",
        }
    }

    const fn form(self) -> &'static str {
        match self {
            Self::Postgres | Self::MySql => "row lock",
            Self::Sqlite => "lease",
        }
    }
}

/// One cell of the grid: a database, a worker count, single deliveries or batches.
#[derive(Clone, Copy, Debug)]
struct Cell {
    database: Database,
    workers: NonZeroUsize,
    batch: Option<NonZeroUsize>,
}

impl Cell {
    fn limit(self) -> usize {
        self.batch.map_or(1, NonZeroUsize::get)
    }

    /// One loop of one run: a fresh table, filled, then drained.
    async fn run(self, kind: Loop, rows: usize) -> Duration {
        let latch = Latch::new(rows);
        let how = Mount {
            workers: self.workers,
            batch: self.batch,
        };
        let tasks = self.workers.get();
        match self.database {
            Database::Postgres => {
                let pool = postgres_pool(POOL);
                postgres_table(&pool, Table::RowLock).await;
                postgres_fill(&pool, Table::RowLock, rows).await;
                if matches!(kind, Loop::Framework) {
                    drain(
                        framework::postgres_row_lock(pool.clone(), latch.clone(), how),
                        &latch,
                    )
                    .await;
                } else if matches!(kind, Loop::Adapter) {
                    adapter_drain!(pool, RowLockJob, tasks, self.batch, latch);
                } else {
                    let statements = Statements::row_lock(
                        &dialect::Postgres,
                        &RowLockJob::TABLE.spec(),
                        ClaimShape::Rows,
                    );
                    join_all((0..tasks).map(|_| {
                        let (pool, statements, latch) =
                            (pool.clone(), statements.clone(), latch.clone());
                        tokio::spawn(async move {
                            raw::row_lock(&pool, &statements, self.limit(), &latch, read).await;
                        })
                    }))
                    .await;
                }
                pool.close().await;
            }
            Database::MySql => {
                let pool = mysql_pool(POOL).await;
                mysql_table(&pool, Table::RowLock).await;
                mysql_fill(&pool, Table::RowLock, rows).await;
                if matches!(kind, Loop::Framework) {
                    drain(
                        framework::mysql_row_lock(pool.clone(), latch.clone(), how),
                        &latch,
                    )
                    .await;
                } else if matches!(kind, Loop::Adapter) {
                    adapter_drain!(pool, RowLockJob, tasks, self.batch, latch);
                } else {
                    let statements = Statements::row_lock(
                        &dialect::MySql,
                        &RowLockJob::TABLE.spec(),
                        ClaimShape::Rows,
                    );
                    join_all((0..tasks).map(|_| {
                        let (pool, statements, latch) =
                            (pool.clone(), statements.clone(), latch.clone());
                        tokio::spawn(async move {
                            raw::row_lock(&pool, &statements, self.limit(), &latch, read).await;
                        })
                    }))
                    .await;
                }
                pool.close().await;
            }
            Database::Sqlite => {
                let pool = sqlite_pool(POOL, Table::Lease).await;
                sqlite_fill(&pool, Table::Lease, rows).await;
                if matches!(kind, Loop::Framework) {
                    drain(
                        framework::sqlite_lease(pool.clone(), latch.clone(), how),
                        &latch,
                    )
                    .await;
                } else if matches!(kind, Loop::Adapter) {
                    adapter_drain!(pool, LeaseJob, tasks, self.batch, latch);
                } else {
                    sqlite_raw(&pool, tasks, self.limit(), &latch).await;
                }
                pool.close().await;
            }
        }
        assert_eq!(latch.remaining(), 0, "the run drained the table");
        latch.window()
    }
}

/// The raw loop on SQLite: `tasks` lease loops on one pool, each claiming up to `limit` rows.
async fn sqlite_raw(pool: &SqlitePool, tasks: usize, limit: usize, latch: &Latch) {
    let statements = Statements::lease(&dialect::Sqlite, &LeaseJob::TABLE.spec());
    join_all((0..tasks).map(|_| {
        let (pool, statements, latch) = (pool.clone(), statements.clone(), latch.clone());
        tokio::spawn(async move {
            raw::lease(
                &pool,
                &statements,
                limit,
                LEASE,
                &latch,
                |job: &LeaseJob| {
                    read_payload(&job.payload);
                },
            )
            .await;
        })
    }))
    .await;
}

/// What the raw loop does with a row lock delivery: what the handler does.
fn read(job: &RowLockJob) {
    read_payload(&job.payload);
}

/// Starts `app` and waits until its subscription has handled every row of the table.
async fn drain(app: impl App, latch: &Latch) {
    let running = app.start().await.expect("the service starts");
    latch.drained_or_stalled("service").await;
    running.shutdown().await.expect("the service stops");
}

/// An outbox cell: the tracked round trip over Redis Pub/Sub with the relay and the sink mounted
/// with `workers` workers, in each variant of the app.
#[derive(Clone, Copy, Debug)]
struct OutboxCell {
    workers: NonZeroUsize,
}

impl OutboxCell {
    /// One variant of one run: a fresh outbox table, then the round trips.
    async fn run(self, kind: Loop, rows: usize) -> Duration {
        let pool = postgres_pool(POOL);
        postgres_table(&pool, Table::Outbox).await;
        let latch = Latch::new(rows);
        let state = latch.clone();
        match kind {
            Loop::Raw => {
                outbox::feed_round_trip(outbox::redis::round_trip(state, self.workers), &latch)
                    .await;
            }
            Loop::Adapter => {
                let built = outbox::redis::round_trip_by_hand(pool.clone(), state, self.workers);
                outbox::feed_round_trip(built, &latch).await;
            }
            Loop::Framework => {
                let built =
                    outbox::redis::round_trip_outbox(pool.clone(), state, TRACKED, self.workers);
                outbox::feed_round_trip(built, &latch).await;
            }
        }
        pool.close().await;
        latch.window()
    }
}

/// One cell's line of the document's throughput section.
#[derive(Debug, Serialize)]
struct Measured {
    database: &'static str,
    form: &'static str,
    /// The plugin whose variants the three loops are, for an outbox cell.
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin: Option<&'static str>,
    /// The batch size, or nothing for single deliveries.
    batch: Option<usize>,
    workers: usize,
    pool: u32,
    rows: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    overhead_percent: f64,
    verdict: &'static str,
    adapter_overhead_percent: f64,
    adapter_verdict: &'static str,
    /// For an outbox cell, this crate's outbox against the one written by hand.
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_overhead_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plugin_verdict: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct Document {
    throughput: Vec<Measured>,
}

/// The three loops' rates over `rounds` interleaved rounds, after a warm-up of each.
async fn rounds<Run, Ran>(rows: usize, rounds: usize, run: Run) -> [Stats; 3]
where
    Run: Fn(Loop, usize) -> Ran,
    Ran: Future<Output = Duration>,
{
    const LOOPS: [Loop; 3] = [Loop::Raw, Loop::Adapter, Loop::Framework];
    // A warm-up of each loop, thrown away: the server's caches and the binary's first calls.
    let warm = (rows / 10).max(1);
    for kind in LOOPS {
        run(kind, warm).await;
    }
    let mut rates: [Vec<f64>; 3] = Default::default();
    for round in 1..=rounds {
        for (index, kind) in LOOPS.into_iter().enumerate() {
            rates[index].push(rate(rows, run(kind, rows).await));
        }
        println!(
            "  round {round:>2}: raw {:>9.0}, adapter {:>9.0}, framework {:>9.0} msg/s",
            rates[0][round - 1],
            rates[1][round - 1],
            rates[2][round - 1]
        );
    }
    rates.map(|measured| Stats::of(&measured))
}

async fn measure(cell: Cell, rows: usize, rounds_run: usize) -> Measured {
    let [raw, adapter, framework] =
        rounds(rows, rounds_run, |kind, rows| cell.run(kind, rows)).await;
    let (overhead_percent, verdict) = against(raw, framework);
    let (adapter_overhead_percent, adapter_verdict) = against(raw, adapter);
    Measured {
        database: cell.database.name(),
        form: cell.database.form(),
        plugin: None,
        batch: cell.batch.map(NonZeroUsize::get),
        workers: cell.workers.get(),
        pool: POOL,
        rows,
        rounds: rounds_run,
        raw,
        adapter,
        framework,
        overhead_percent,
        verdict,
        adapter_overhead_percent,
        adapter_verdict,
        plugin_overhead_percent: None,
        plugin_verdict: None,
    }
}

async fn measure_outbox(cell: OutboxCell, rows: usize, rounds_run: usize) -> Measured {
    let [raw, adapter, framework] =
        rounds(rows, rounds_run, |kind, rows| cell.run(kind, rows)).await;
    let (overhead_percent, verdict) = against(raw, framework);
    let (adapter_overhead_percent, adapter_verdict) = against(raw, adapter);
    let (plugin_overhead_percent, plugin_verdict) = against(adapter, framework);
    Measured {
        database: "Postgres",
        form: "outbox round trip over Redis Pub/Sub",
        plugin: Some("outbox"),
        batch: None,
        workers: cell.workers.get(),
        pool: POOL,
        rows,
        rounds: rounds_run,
        raw,
        adapter,
        framework,
        overhead_percent,
        verdict,
        adapter_overhead_percent,
        adapter_verdict,
        plugin_overhead_percent: Some(plugin_overhead_percent),
        plugin_verdict: Some(plugin_verdict),
    }
}

fn label(row: &Measured) -> String {
    let batch = row
        .batch
        .map_or_else(|| "single".to_owned(), |size| format!("batch of {size}"));
    format!(
        "{} {}, {batch}, {} workers",
        row.database, row.form, row.workers
    )
}

fn main() {
    let rows = number("RUSTSTREAM_BENCH_THROUGHPUT_ROWS", ROWS);
    let rounds_run = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let out =
        env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-throughput.json".to_owned());

    let runtime = runtime();
    let mut throughput = Vec::new();
    for database in Database::ALL {
        for batch in [None, Some(BATCH)] {
            for workers in WORKERS {
                let cell = Cell {
                    database,
                    workers: NonZeroUsize::new(workers).expect("a worker count is positive"),
                    batch,
                };
                println!(
                    "{} {}, {}, {workers} workers: {rows} rows per run",
                    database.name(),
                    database.form(),
                    batch.map_or_else(|| "single".to_owned(), |size| format!("batch of {size}")),
                );
                throughput.push(runtime.block_on(measure(cell, rows, rounds_run)));
            }
        }
    }
    for workers in WORKERS {
        let cell = OutboxCell {
            workers: NonZeroUsize::new(workers).expect("a worker count is positive"),
        };
        println!("outbox round trip over Redis Pub/Sub, {workers} workers: {rows} per run");
        throughput.push(runtime.block_on(measure_outbox(cell, rows, rounds_run)));
    }

    println!();
    for row in &throughput {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:.1}%, {}), framework {:.0} ({:.1}%, {}) msg/s",
            label(row),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.adapter_verdict,
            row.framework.best,
            row.overhead_percent,
            row.verdict
        );
    }
    let text =
        serde_json::to_string_pretty(&Document { throughput }).expect("the summary serializes");
    fs::write(&out, text + "\n").expect("the summary is written");
    println!("\nwrote {out}");
}
