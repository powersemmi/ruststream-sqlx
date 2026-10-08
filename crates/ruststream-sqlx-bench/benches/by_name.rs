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
//! A by-name subscription on Postgres: `#[subscriber("jobs")]` on a broker whose route leads the
//! name into a row lock table. The subscription reads the rows by the columns their roles name,
//! one delivery at a time. Beside it, the raw sqlx loop that runs the same statements.

mod common;

use common::MESSAGES;
use common::code::{Pending, start_and_drain};
use common::framework;
use common::raw::{Statements, read_payload, row_lock};
use common::stand::Table;
use common::tables::NamedJob;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{ClaimShape, Postgres};

/// Connections both halves' pools may open: the claim's transaction and room to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::Named, messages, POOL, |pool, latch| {
        framework::postgres_by_name(pool, latch)
    })
}

// A by-name subscription claims the columns under their roles' names, which is the claim this
// loop runs.
fn raw_run(messages: usize) -> Pending {
    let statements = Statements::row_lock(&Postgres, &NamedJob::TABLE.spec(), ClaimShape::Roles);
    Pending::raw(Table::Named, messages, POOL, move |runtime, pool, latch| {
        runtime.block_on(row_lock(pool, &statements, 1, latch, |job: &NamedJob| {
            read_payload(&job.payload);
        }));
    })
}

// At most 32 allocations per delivery and 382 once per run, over four smoke runs of 20 deliveries.
// The limit is that with a percent of headroom on the steady rate and five on the once-per-run
// part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(32_320, 1_000, 402))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// At most 33 allocations per delivery and 292 once per run, over four smoke runs of 20 deliveries.
// The limit is that with a percent of headroom on the steady rate and five on the once-per-run
// part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(33_330, 1_000, 307))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = by_name_group; benchmarks = service, raw);
main!(library_benchmark_groups = by_name_group);
