// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a database fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(missing_docs, unreachable_pub, unused_qualifications)]
//! How many messages a second a queue table drains at, on Postgres, MySQL and SQLite, through
//! this crate and through a raw sqlx loop with the same concurrency.
//!
//! # What a run is
//!
//! A run recreates its table, fills it with [`ROWS`] rows (or `RUSTSTREAM_BENCH_THROUGHPUT_ROWS`),
//! and drains it on a pool of [`POOL`] connections. The service mounts its handler with
//! `workers(n)` (one worker is the plain mount, which handles a delivery before it pulls the
//! next), single deliveries or batches of 16. The raw half runs n tasks on one pool, each a loop
//! that claims, reads and deletes until its claim comes back empty: the same statements, the
//! same concurrency.
//!
//! Postgres and MySQL take the row lock form. SQLite takes the lease form: it has no row locks,
//! and a lease claim is one statement there.
//!
//! The window runs from the first handled delivery to the last. Every handled delivery counts a
//! latch down, in the handler and in the raw loop alike, and the last count wakes the waiter
//! through a `Notify`: the run knows it drained without polling the table or sleeping. The halves
//! are interleaved, round after round, and each reports its best, median and worst round.

mod common;

use std::env;
use std::fs;
use std::num::NonZeroUsize;
use std::time::Duration;

use common::raw::{self, Statements, read_payload};
use common::services::{self, Mount};
use common::stand::{
    Table, mysql_fill, mysql_pool, mysql_table, postgres_fill, postgres_pool, postgres_table,
    sqlite_fill, sqlite_pool,
};
use common::tables::{LeaseJob, RowLockJob};
use common::timing::{Stats, against, number, rate, runtime};
use common::{BATCH, LEASE, Latch};
use futures::future::join_all;
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{self, ClaimShape};
use ruststream_sqlx::prelude::*;
use serde::Serialize;

/// Rows a run drains, unless `RUSTSTREAM_BENCH_THROUGHPUT_ROWS` says otherwise.
const ROWS: usize = 20_000;
/// Rounds run, unless `RUSTSTREAM_BENCH_PAIRS` says otherwise.
const PAIRS: usize = 3;
/// The pool both halves drain on: room for eight workers in the row lock form, each holding the
/// connection of its claim, with the subscription's next claim beside them.
const POOL: u32 = 16;
/// The worker counts a run is taken at.
const WORKERS: [usize; 3] = [1, 4, 8];

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

    /// One half of one run: a fresh table, filled, then drained.
    async fn run(self, service: bool, rows: usize) -> Duration {
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
                if service {
                    drain(
                        services::postgres_row_lock(pool.clone(), latch.clone(), how),
                        &latch,
                    )
                    .await;
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
                if service {
                    drain(
                        services::mysql_row_lock(pool.clone(), latch.clone(), how),
                        &latch,
                    )
                    .await;
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
                if service {
                    drain(
                        services::sqlite_lease(pool.clone(), latch.clone(), how),
                        &latch,
                    )
                    .await;
                } else {
                    let statements = Statements::lease(&dialect::Sqlite, &LeaseJob::TABLE.spec());
                    join_all((0..tasks).map(|_| {
                        let (pool, statements, latch) =
                            (pool.clone(), statements.clone(), latch.clone());
                        tokio::spawn(async move {
                            raw::lease(
                                &pool,
                                &statements,
                                self.limit(),
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
                pool.close().await;
            }
        }
        assert_eq!(latch.remaining(), 0, "the run drained the table");
        latch.window()
    }
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

/// One cell's line of the document's throughput section.
#[derive(Debug, Serialize)]
struct Measured {
    database: &'static str,
    form: &'static str,
    /// The batch size, or nothing for single deliveries.
    batch: Option<usize>,
    workers: usize,
    pool: u32,
    rows: usize,
    rounds: usize,
    raw: Stats,
    framework: Stats,
    overhead_percent: f64,
    verdict: &'static str,
}

#[derive(Debug, Serialize)]
struct Document {
    throughput: Vec<Measured>,
}

async fn measure(cell: Cell, rows: usize, rounds: usize) -> Measured {
    // A warm-up of each half, thrown away: the server's caches and the binary's first calls.
    let warm = (rows / 10).max(1);
    cell.run(false, warm).await;
    cell.run(true, warm).await;
    let mut raw_rates = Vec::with_capacity(rounds);
    let mut service_rates = Vec::with_capacity(rounds);
    for round in 1..=rounds {
        let raw = rate(rows, cell.run(false, rows).await);
        let service = rate(rows, cell.run(true, rows).await);
        println!("  round {round:>2}: raw {raw:>9.0}, service {service:>9.0} msg/s");
        raw_rates.push(raw);
        service_rates.push(service);
    }
    let raw = Stats::of(&raw_rates);
    let framework = Stats::of(&service_rates);
    let (overhead_percent, verdict) = against(raw, framework);
    Measured {
        database: cell.database.name(),
        form: cell.database.form(),
        batch: cell.batch.map(NonZeroUsize::get),
        workers: cell.workers.get(),
        pool: POOL,
        rows,
        rounds,
        raw,
        framework,
        overhead_percent,
        verdict,
    }
}

fn main() {
    let rows = number("RUSTSTREAM_BENCH_THROUGHPUT_ROWS", ROWS);
    let rounds = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
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
                throughput.push(runtime.block_on(measure(cell, rows, rounds)));
            }
        }
    }

    println!();
    for row in &throughput {
        println!(
            "{} {}, {}, {} workers: raw {:.0}, service {:.0} msg/s, {:.1}% ({})",
            row.database,
            row.form,
            row.batch
                .map_or_else(|| "single".to_owned(), |size| format!("batch of {size}")),
            row.workers,
            row.raw.best,
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
