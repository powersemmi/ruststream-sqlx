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
//! The outbox as a plugin, a full tracked round trip over `MemoryBroker` on Postgres: a relay
//! answers each command with a reply, and a sink consumes the reply. Three variants of the same
//! app: with no outbox, with the outbox written by hand in its handlers, and with this crate's
//! outbox, whose publish layer records the reply and whose subscription layer takes the record
//! into work and marks it once the sink acknowledges.

mod common;

use std::num::NonZeroUsize;

use common::MESSAGES;
use common::code::{Pending, feeding, start_and_drain};
use common::outbox::{Command, TRACKED, memory};
use common::stand::Table;
use futures::FutureExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;

/// Connections every variant's pool may open: the record's insert, the fetch and the mark.
const POOL: u32 = 4;

macro_rules! round_trip {
    ($messages:expr, |$latch:ident, $pool:ident| $build:expr) => {
        feeding!(
            $messages,
            table = Table::Outbox,
            fill = false,
            connections = POOL,
            |$latch, $pool| {
                let (app, egress) = $build;
                (app, egress, ())
            },
            wrap = |live, ()| live,
            |publisher, _index| publisher
                .message(&Command { id: 1 })
                .publish()
                .map(|published| {
                    published.expect("the command is published");
                }),
        )
    };
}

// The app with no outbox opens no connection; the pool is opened all the same, so the start
// region of every variant does the same work.
fn none_run(messages: usize) -> Pending {
    round_trip!(messages, |latch, pool| {
        drop(pool);
        memory::round_trip(latch, NonZeroUsize::MIN)
    })
}

fn by_hand_run(messages: usize) -> Pending {
    round_trip!(messages, |latch, pool| memory::round_trip_by_hand(
        pool,
        latch,
        NonZeroUsize::MIN
    ))
}

fn outbox_run(messages: usize) -> Pending {
    round_trip!(messages, |latch, pool| memory::round_trip_outbox(
        pool,
        latch,
        TRACKED,
        NonZeroUsize::MIN
    ))
}

// Twice MESSAGES deliveries allocated 12,207 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 13 blocks.
#[library_benchmark(config = common::config_every(6_104, 1_000, 13))]
#[bench::first(none_run(1))]
#[bench::base(none_run(MESSAGES))]
#[bench::twice(none_run(2 * MESSAGES))]
fn none(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 78,464 to 78,474 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 79 blocks.
#[library_benchmark(config = common::config_every(39_237, 1_000, 79))]
#[bench::first(by_hand_run(1))]
#[bench::base(by_hand_run(MESSAGES))]
#[bench::twice(by_hand_run(2 * MESSAGES))]
fn by_hand(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 84,495 to 84,507 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 85 blocks.
#[library_benchmark(config = common::config_every(42_254, 1_000, 85))]
#[bench::first(outbox_run(1))]
#[bench::base(outbox_run(MESSAGES))]
#[bench::twice(outbox_run(2 * MESSAGES))]
fn outbox(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = outbox_group; benchmarks = none, by_hand, outbox);
main!(library_benchmark_groups = outbox_group);
