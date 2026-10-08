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
//! Row mode in the row lock form on Postgres: a table with no payload column, whose handler takes
//! the row the driver decoded, a text and an integer, one delivery at a time. Beside it, the raw
//! sqlx loop that runs the same statements and decodes the same row.

mod common;

use std::hint::black_box;

use common::MESSAGES;
use common::code::{Pending, start_and_drain};
use common::framework;
use common::raw::{Statements, row_lock};
use common::stand::Table;
use common::tables::OrderRow;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};

/// Connections both halves' pools may open: the claim's transaction and room to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::RowMode, messages, POOL, |pool, latch| {
        framework::postgres_row_mode(pool, latch)
    })
}

fn raw_run(messages: usize) -> Pending {
    let statements = Statements::row_lock(&Postgres, &OrderRow::TABLE.spec(), ClaimShape::Rows);
    Pending::raw(
        Table::RowMode,
        messages,
        POOL,
        move |runtime, pool, latch| {
            runtime.block_on(row_lock(pool, &statements, 1, latch, |order: &OrderRow| {
                black_box((order.customer.len(), order.quantity));
            }));
        },
    )
}

// Twice MESSAGES deliveries allocated 64,467 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 65 blocks.
#[library_benchmark(config = common::config_every(32_234, 1_000, 65))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 66,377 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 67 blocks.
#[library_benchmark(config = common::config_every(33_189, 1_000, 67))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = row_mode_group; benchmarks = service, raw);
main!(library_benchmark_groups = row_mode_group);
