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
//! The transactional outbox on Postgres, over `MemoryBroker`: a relay answers each command with a
//! reply, and a sink consumes the reply. Tracked, the publish middleware records the reply and
//! sends it with the record's id, and the subscription middleware fetches the record and marks it
//! processed once the sink acknowledges. Untracked, both middlewares let every message pass.
//! Beside them, the service with no middleware, and the same service with the record written,
//! fetched and marked by hand in its handlers.

mod common;

use common::MESSAGES;
use common::code::{Pending, Started, start_and_drain};
use common::services::{self, Command, TRACKED};
use common::stand::{Table, postgres_pool};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;
use tokio::runtime::Runtime;

/// Connections both halves' pools may open: the record's insert, the fetch and the mark.
const POOL: u32 = 4;

/// Starts `$build`'s service and pairs its command publisher in the start region; publishes one
/// command per expected delivery and waits for the sink to count them all in the drain region.
macro_rules! relaying {
    ($messages:expr, |$latch:ident| $build:expr) => {
        Pending::new(Table::Outbox, false, $messages, |$latch| {
            let latch = $latch.clone();
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
                                .message(&Command { id: 1 })
                                .publish()
                                .await
                                .expect("the command is published");
                        }
                        latch.drained().await;
                    });
                };
                Started::Driving(Box::new(drive), Some(running))
            })
        })
    };
}

fn tracked_run(messages: usize) -> Pending {
    relaying!(messages, |latch| services::outbox(
        postgres_pool(POOL),
        latch,
        TRACKED
    ))
}

fn untracked_run(messages: usize) -> Pending {
    relaying!(messages, |latch| services::outbox(
        postgres_pool(POOL),
        latch,
        "elsewhere"
    ))
}

fn bare_run(messages: usize) -> Pending {
    relaying!(messages, |latch| services::outbox_bare(latch))
}

fn raw_run(messages: usize) -> Pending {
    relaying!(messages, |latch| services::outbox_by_hand(
        postgres_pool(POOL),
        latch
    ))
}

// 41.05 allocations per delivery and 194 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(41_461, 1_000, 204))]
#[bench::first(tracked_run(1))]
#[bench::base(tracked_run(MESSAGES))]
#[bench::twice(tracked_run(2 * MESSAGES))]
fn tracked(run: Pending) {
    start_and_drain(run);
}

// 40.15 allocations per delivery and 191 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(40_552, 1_000, 201))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

// 6.1 allocations per delivery and 45 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(6_161, 1_000, 48))]
#[bench::first(untracked_run(1))]
#[bench::base(untracked_run(MESSAGES))]
#[bench::twice(untracked_run(2 * MESSAGES))]
fn untracked(run: Pending) {
    start_and_drain(run);
}

// 6.1 allocations per delivery and 45 for the start, seen on a smoke run of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// start; one allocation more per delivery breaches it.
#[library_benchmark(config = common::config_every(6_161, 1_000, 48))]
#[bench::first(bare_run(1))]
#[bench::base(bare_run(MESSAGES))]
#[bench::twice(bare_run(2 * MESSAGES))]
fn bare(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = outbox_group; benchmarks = tracked, raw, untracked, bare);
main!(library_benchmark_groups = outbox_group);
