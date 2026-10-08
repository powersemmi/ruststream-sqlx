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
//! The lease form on Postgres, one delivery at a time: the claim writes a lease into a row and
//! commits, the handler decodes a small JSON body into a struct with no transaction open, and the
//! acknowledgement deletes the row while it still holds the lease. Beside it, the raw sqlx loop
//! that runs the same statements.

mod common;

use common::code::{Pending, start_and_drain};
use common::framework::{self, Mount};
use common::raw::{Statements, lease, read_payload};
use common::stand::Table;
use common::tables::LeaseJob;
use common::{LEASE, MESSAGES};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::Postgres;

/// Connections both halves' pools may open: the claim's and the settlement's, and room to spare.
const POOL: u32 = 4;

fn service_run(messages: usize) -> Pending {
    Pending::service(Table::Lease, messages, POOL, |pool, latch| {
        framework::postgres_lease(pool, latch, Mount::SEQUENTIAL)
    })
}

fn raw_run(messages: usize) -> Pending {
    let statements = Statements::lease(&Postgres, &LeaseJob::TABLE.spec());
    Pending::raw(Table::Lease, messages, POOL, move |runtime, pool, latch| {
        runtime.block_on(lease(
            pool,
            &statements,
            1,
            LEASE,
            latch,
            |job: &LeaseJob| {
                read_payload(&job.payload);
            },
        ));
    })
}

// Twice MESSAGES deliveries allocated 56,462 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 57 blocks.
#[library_benchmark(config = common::config_every(28_231, 1_000, 57))]
#[bench::first(service_run(1))]
#[bench::base(service_run(MESSAGES))]
#[bench::twice(service_run(2 * MESSAGES))]
fn service(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 56,311 to 56,338 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 57 blocks.
#[library_benchmark(config = common::config_every(28_169, 1_000, 57))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = lease_group; benchmarks = service, raw);
main!(library_benchmark_groups = lease_group);
