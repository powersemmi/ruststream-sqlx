// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Batches of 64 in the row lock form on Postgres: one claim locks up to 64 rows in a
//! transaction, the handler is handed a slice it decodes, and every row is deleted in the same
//! transaction before it commits. Beside it, the raw sqlx loop that runs the same statements.

mod common;

use std::num::NonZeroUsize;

use common::code::{Pending, start_and_drain};
use common::framework::{self, Mount};
use common::raw::{Statements, read_payload, row_lock};
use common::stand::Table;
use common::tables::RowLockJob;
use common::{BATCH, MESSAGES};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};

/// Connections both halves' pools may open: the claim's transaction and room to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::RowLock, messages, POOL, |pool, latch| {
        let how = Mount {
            workers: NonZeroUsize::MIN,
            batch: Some(BATCH),
        };
        framework::postgres_row_lock(pool, latch, how)
    })
}

fn raw_run(messages: usize) -> Pending {
    let statements = Statements::row_lock(&Postgres, &RowLockJob::TABLE.spec(), ClaimShape::Rows);
    Pending::raw(
        Table::RowLock,
        messages,
        POOL,
        move |runtime, pool, latch| {
            runtime.block_on(row_lock(
                pool,
                &statements,
                BATCH.get(),
                latch,
                |job: &RowLockJob| {
                    read_payload(&job.payload);
                },
            ));
        },
    )
}

// At most 14.40 allocations per delivery and 356 once per run, over two smoke runs of 32
// deliveries, a whole number of batches. The limit is that with two percent of headroom on the
// steady rate and five on the once-per-run part; one allocation more per delivery breaches it at
// the default count.
#[library_benchmark(config = common::config_every(14_695, 1_000, 374))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// At most 14.31 allocations per delivery and 267 once per run, over two smoke runs of 32
// deliveries, a whole number of batches. The limit is that with two percent of headroom on the
// steady rate and five on the once-per-run part; one allocation more per delivery breaches it at
// the default count.
#[library_benchmark(config = common::config_every(14_599, 1_000, 281))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = batch_group; benchmarks = service, raw);
main!(library_benchmark_groups = batch_group);
