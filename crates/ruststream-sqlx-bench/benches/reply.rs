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
//! Replying: the row lock form on Postgres, one delivery at a time, and the handler returns a
//! value the runtime encodes and hands to the connected broker's default publisher, `Routed`,
//! which inserts it into the table the reply type's name leads to. Then the acknowledgement
//! deletes the row and commits. Beside it, the raw sqlx loop that runs the same statements.

mod common;

use common::MESSAGES;
use common::code::{Pending, start_and_drain};
use common::framework;
use common::raw::{Statements, reply};
use common::stand::Table;
use common::tables::RowLockJob;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};

/// Connections both halves' pools may open: the claim's transaction, the reply's insert and room
/// to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::RowLock, messages, POOL, framework::postgres_reply).also(Table::Replies)
}

fn raw_run(messages: usize) -> Pending {
    let statements = Statements::row_lock(&Postgres, &RowLockJob::TABLE.spec(), ClaimShape::Rows);
    Pending::raw(
        Table::RowLock,
        messages,
        POOL,
        move |runtime, pool, latch| {
            runtime.block_on(reply(pool, &statements, latch));
        },
    )
    .also(Table::Replies)
}

// A placeholder floor, replaced by the measured one.
#[library_benchmark(config = common::config_every(60_000, 1_000, 1_000))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// A placeholder floor, replaced by the measured one.
#[library_benchmark(config = common::config_every(60_000, 1_000, 1_000))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = reply_group; benchmarks = service, raw);
main!(library_benchmark_groups = reply_group);
