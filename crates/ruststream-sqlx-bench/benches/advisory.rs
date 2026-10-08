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
//! The advisory lock form on Postgres, one delivery at a time: the claim finds a row whose key
//! nobody holds, the connection takes the lock on the key and the row, the handler decodes a
//! small JSON body into a struct, and the acknowledgement deletes the row and releases the key.
//! Beside it, the raw sqlx loop that runs the same statements on one connection.

mod common;

use common::MESSAGES;
use common::code::{Pending, start_and_drain};
use common::raw::{Statements, advisory, read_payload};
use common::services;
use common::stand::Table;
use common::tables::AdvisoryJob;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::Postgres;

/// Connections both halves' pools may open: the one that holds the delivery, and room to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::Advisory, messages, POOL, |pool, latch| {
        services::postgres_advisory(pool, latch)
    })
}

fn raw_run(messages: usize) -> Pending {
    let statements = Statements::advisory(&Postgres, &AdvisoryJob::TABLE.spec());
    Pending::raw(
        Table::Advisory,
        messages,
        POOL,
        move |runtime, pool, latch| {
            runtime.block_on(advisory(pool, &statements, latch, |job: &AdvisoryJob| {
                read_payload(&job.payload);
            }));
        },
    )
}

// At most 47.95 allocations per delivery and 599 once per run, over four smoke runs of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// once-per-run part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(48_430, 1_000, 629))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// At most 47 allocations per delivery and 443 once per run, over four smoke runs of 20 deliveries.
// The limit is that with a percent of headroom on the steady rate and five on the once-per-run
// part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(47_470, 1_000, 466))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = advisory_group; benchmarks = service, raw);
main!(library_benchmark_groups = advisory_group);
