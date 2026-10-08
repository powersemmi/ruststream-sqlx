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
//! The row lock form on Postgres, one delivery at a time: the claim locks a row in a transaction,
//! the handler decodes a small JSON body into a struct, and the acknowledgement deletes the row
//! and commits. Beside it, the raw sqlx loop that runs the same statements.

mod common;

use common::MESSAGES;
use common::code::{Pending, start_and_drain};
use common::framework::{self, Mount};
use common::raw::{Statements, read_payload, row_lock};
use common::stand::Table;
use common::tables::RowLockJob;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};

/// Connections both halves' pools may open: the claim's transaction and room to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::RowLock, messages, POOL, |pool, latch| {
        framework::postgres_row_lock(pool, latch, Mount::SEQUENTIAL)
    })
}

fn raw_run(messages: usize) -> Pending {
    let statements = Statements::row_lock(&Postgres, &RowLockJob::TABLE.spec(), ClaimShape::Rows);
    Pending::raw(
        Table::RowLock,
        messages,
        POOL,
        move |runtime, pool, latch| {
            runtime.block_on(row_lock(pool, &statements, 1, latch, |job: &RowLockJob| {
                read_payload(&job.payload);
            }));
        },
    )
}

// Twice MESSAGES deliveries allocated 64,460 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 65 blocks.
#[library_benchmark(config = common::config_every(32_230, 1_000, 65))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 66,373 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 67 blocks.
#[library_benchmark(config = common::config_every(33_187, 1_000, 67))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = consume_group; benchmarks = service, raw);
main!(library_benchmark_groups = consume_group);
