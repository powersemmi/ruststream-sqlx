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
//! Publishing into a table from a running service: a `Repository`, whose table is named at
//! compile time, and `Routed`, which finds the table by the message's name. The codec encodes a
//! small struct, and the table's own insert writes the row. Beside them, the raw sqlx loop that
//! encodes the same struct and runs the same insert.

mod common;

use common::code::{Pending, Started, connect, start_and_drain};
use common::raw::publish;
use common::services;
use common::stand::{Table, postgres_pool};
use common::{MESSAGES, OrderPlaced};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;
use tokio::runtime::Runtime;

/// Connections both halves' pools may open: a publish takes one for its insert.
const POOL: u32 = 2;

/// Starts `$app`, pairs the publisher from `$egress` in the start region, and publishes one
/// message per expected delivery in the drain region.
macro_rules! publishing {
    ($messages:expr, $build:expr) => {
        Pending::new(Table::Named, false, $messages, |latch| {
            let (app, egress) = $build;
            Box::new(move |runtime: &Runtime| {
                let (running, publisher) = runtime.block_on(async {
                    let running = app.start().await.expect("the service starts");
                    let publisher = running
                        .publisher(egress)
                        .await
                        .expect("the publisher pairs");
                    (running, publisher)
                });
                let drive = move |runtime: &Runtime| {
                    runtime.block_on(async {
                        for _ in 0..latch.total() {
                            publisher
                                .message(&OrderPlaced::fixed())
                                .publish()
                                .await
                                .expect("the publish writes a row");
                            latch.arrived();
                        }
                    });
                };
                Started::Driving(Box::new(drive), Some(running))
            })
        })
    };
}

fn repository_run(messages: usize) -> Pending {
    publishing!(messages, services::postgres_repository(postgres_pool(POOL)))
}

fn routed_run(messages: usize) -> Pending {
    publishing!(messages, services::postgres_routed(postgres_pool(POOL)))
}

fn raw_run(messages: usize) -> Pending {
    Pending::new(Table::Named, false, messages, |latch| {
        let pool = postgres_pool(POOL);
        Box::new(move |runtime: &Runtime| {
            runtime.block_on(connect(&pool));
            Started::Driving(
                Box::new(move |runtime| runtime.block_on(publish(&pool, &latch))),
                None,
            )
        })
    })
}

// 15 allocations per delivery and 121 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(15_150, 1_000, 128))]
#[bench::first(repository_run(1))]
#[bench::base(repository_run(MESSAGES))]
#[bench::twice(repository_run(2 * MESSAGES))]
fn repository(run: Pending) {
    start_and_drain(run);
}

// 15 allocations per delivery and 122 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(15_150, 1_000, 129))]
#[bench::first(routed_run(1))]
#[bench::base(routed_run(MESSAGES))]
#[bench::twice(routed_run(2 * MESSAGES))]
fn routed(run: Pending) {
    start_and_drain(run);
}

// 12 allocations per delivery and 104 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(12_120, 1_000, 110))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = publish_group; benchmarks = repository, routed, raw);
main!(library_benchmark_groups = publish_group);
