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

use common::code::{Pending, Started, start_and_drain, warm};
use common::services::{self, Command, TRACKED};
use common::stand::{Table, postgres_pool};
use common::{Latch, MESSAGES};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;
use tokio::runtime::Runtime;

/// Connections both halves' pools may open: the record's insert, the fetch and the mark.
const POOL: u32 = 4;

/// Starts `$build`'s service and pairs its command publisher in the start region; in the drain
/// region publishes one command per expected delivery, each once the sink counted the one before.
macro_rules! relaying {
    ($messages:expr, |$latch:ident, $pool:ident| $build:expr) => {
        Pending::new(Table::Outbox, false, Latch::stepping($messages), |$latch| {
            let latch = $latch.clone();
            let pool = postgres_pool(POOL);
            let (app, egress) = {
                let $pool = pool.clone();
                $build
            };
            Box::new(move |runtime: &Runtime| {
                let (running, publisher) = runtime.block_on(async {
                    // Every connection the relay and the sink can hold at once is opened here, so
                    // the drain never opens one: how many a run opens otherwise depends on how the
                    // two handlers interleave, and every opening allocates.
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
                        // One command in flight at a time: the relay and the sink never contend
                        // for a connection, so a run allocates the same whatever the scheduling.
                        for left in (0..latch.total()).rev() {
                            publisher
                                .message(&Command { id: 1 })
                                .publish()
                                .await
                                .expect("the command is published");
                            latch.reached(left).await;
                        }
                    });
                };
                Started::Driving(Box::new(drive), Some(running))
            })
        })
    };
}

fn tracked_run(messages: usize) -> Pending {
    relaying!(messages, |latch, pool| services::outbox(
        pool, latch, TRACKED
    ))
}

fn untracked_run(messages: usize) -> Pending {
    relaying!(messages, |latch, pool| services::outbox(
        pool,
        latch,
        "elsewhere"
    ))
}

// The service with no middleware opens no connection; the pool is opened all the same, so the
// start region of every outbox scenario does the same work.
fn bare_run(messages: usize) -> Pending {
    relaying!(messages, |latch, pool| {
        drop(pool);
        services::outbox_bare(latch)
    })
}

fn raw_run(messages: usize) -> Pending {
    relaying!(messages, |latch, pool| services::outbox_by_hand(
        pool, latch
    ))
}

// At most 42.1 allocations per delivery and 355 once per run, over four smoke runs of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// once-per-run part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(42_521, 1_000, 373))]
#[bench::first(tracked_run(1))]
#[bench::base(tracked_run(MESSAGES))]
#[bench::twice(tracked_run(2 * MESSAGES))]
fn tracked(run: Pending) {
    start_and_drain(run);
}

// At most 39.1 allocations per delivery and 375 once per run, over four smoke runs of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// once-per-run part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(39_491, 1_000, 394))]
#[bench::first(raw_run(1))]
#[bench::base(raw_run(MESSAGES))]
#[bench::twice(raw_run(2 * MESSAGES))]
fn raw(run: Pending) {
    start_and_drain(run);
}

// At most 6.1 allocations per delivery and 210 once per run, over four smoke runs of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// once-per-run part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(6_161, 1_000, 221))]
#[bench::first(untracked_run(1))]
#[bench::base(untracked_run(MESSAGES))]
#[bench::twice(untracked_run(2 * MESSAGES))]
fn untracked(run: Pending) {
    start_and_drain(run);
}

// At most 6.1 allocations per delivery and 210 once per run, over four smoke runs of 20
// deliveries. The limit is that with a percent of headroom on the steady rate and five on the
// once-per-run part; one allocation more per delivery breaches it at the default count.
#[library_benchmark(config = common::config_every(6_161, 1_000, 221))]
#[bench::first(bare_run(1))]
#[bench::base(bare_run(MESSAGES))]
#[bench::twice(bare_run(2 * MESSAGES))]
fn bare(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = outbox_group; benchmarks = tracked, raw, untracked, bare);
main!(library_benchmark_groups = outbox_group);
