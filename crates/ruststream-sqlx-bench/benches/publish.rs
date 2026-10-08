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

use common::code::{Pending, Started, start_and_drain, warm};
use common::framework;
use common::raw::publish;
use common::stand::{Table, postgres_pool};
use common::{Latch, MESSAGES, OrderPlaced};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;
use tokio::runtime::Runtime;

/// Connections both halves' pools may open: a publish takes one for its insert.
const POOL: u32 = 2;

/// Starts `$app`, pairs the publisher from `$egress` in the start region, and publishes one
/// message per expected delivery in the drain region.
macro_rules! publishing {
    ($messages:expr, |$pool:ident| $build:expr) => {
        Pending::new(Table::Named, false, Latch::new($messages), |latch| {
            let pool = postgres_pool(POOL);
            let (app, egress) = {
                let $pool = pool.clone();
                $build
            };
            Box::new(move |runtime: &Runtime| {
                let (running, publisher) = runtime.block_on(async {
                    warm(&pool, POOL).await;
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
    publishing!(messages, |pool| framework::postgres_repository(pool))
}

fn routed_run(messages: usize) -> Pending {
    publishing!(messages, |pool| framework::postgres_routed(pool))
}

fn raw_run(messages: usize) -> Pending {
    Pending::new(Table::Named, false, Latch::new(messages), |latch| {
        let pool = postgres_pool(POOL);
        Box::new(move |runtime: &Runtime| {
            runtime.block_on(warm(&pool, POOL));
            Started::Driving(
                Box::new(move |runtime| runtime.block_on(publish(&pool, &latch))),
                None,
            )
        })
    })
}

// Twice MESSAGES deliveries allocated 30,141 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 31 blocks.
#[library_benchmark(config = common::config_every(15_071, 1_000, 31))]
#[bench::first(repository_run(1))]
#[bench::base(repository_run(MESSAGES))]
#[bench::twice(repository_run(2 * MESSAGES))]
fn repository(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 30,142 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 31 blocks.
#[library_benchmark(config = common::config_every(15_071, 1_000, 31))]
#[bench::first(routed_run(1))]
#[bench::base(routed_run(MESSAGES))]
#[bench::twice(routed_run(2 * MESSAGES))]
fn routed(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 24,124 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 25 blocks.
#[library_benchmark(config = common::config_every(12_062, 1_000, 25))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = publish_group; benchmarks = repository, routed, raw);
main!(library_benchmark_groups = publish_group);
