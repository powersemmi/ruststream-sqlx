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
//! The outbox as a plugin, an untracked message: the same round trip over `MemoryBroker` as
//! `outbox`, with the registry recording a name no message carries, so both layers let every
//! message pass. Against it, the app with no outbox; written by hand, an untracked message costs
//! nothing, so that app is the hand-written variant too.

mod common;

use std::num::NonZeroUsize;

use common::MESSAGES;
use common::code::{Pending, feeding, start_and_drain};
use common::outbox::{Command, UNTRACKED, memory};
use common::stand::Table;
use futures::FutureExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;

/// Connections every variant's pool may open, the same as the tracked round trip's.
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

fn none_run(messages: usize) -> Pending {
    round_trip!(messages, |latch, pool| {
        drop(pool);
        memory::round_trip(latch, NonZeroUsize::MIN)
    })
}

fn outbox_run(messages: usize) -> Pending {
    round_trip!(messages, |latch, pool| memory::round_trip_outbox(
        pool,
        latch,
        UNTRACKED,
        NonZeroUsize::MIN
    ))
}

// A placeholder floor, replaced by the measured one.
#[library_benchmark(config = common::config_every(60_000, 1_000, 1_000))]
#[bench::first(none_run(1))]
#[bench::base(none_run(MESSAGES))]
#[bench::twice(none_run(2 * MESSAGES))]
fn none(run: Pending) {
    start_and_drain(run);
}

// A placeholder floor, replaced by the measured one.
#[library_benchmark(config = common::config_every(60_000, 1_000, 1_000))]
#[bench::first(outbox_run(1))]
#[bench::base(outbox_run(MESSAGES))]
#[bench::twice(outbox_run(2 * MESSAGES))]
fn outbox(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = outbox_untracked_group; benchmarks = none, outbox);
main!(library_benchmark_groups = outbox_untracked_group);
