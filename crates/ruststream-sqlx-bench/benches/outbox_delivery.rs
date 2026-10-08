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
//! The outbox as a plugin, a tracked delivery over `MemoryBroker` on Postgres: the table holds an
//! unprocessed record per message, and each message carries its record's id in a header. With no
//! outbox the sink handles the message and the header is read by nobody. By hand the sink takes
//! the record into work and marks it. This crate's subscription layer does the same around the
//! same sink.

mod common;

use common::MESSAGES;
use common::code::{Pending, feeding, start_and_drain};
use common::outbox::{Reply, memory, record_header};
use common::stand::Table;
use futures::FutureExt;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;

/// Connections every variant's pool may open: the fetch, the mark and room to spare.
const POOL: u32 = 4;

/// Publishes the message of the record `$index + 1`, the id the fill gave it.
macro_rules! delivering {
    ($messages:expr, |$latch:ident, $pool:ident| $build:expr) => {
        feeding!(
            $messages,
            table = Table::Outbox,
            fill = true,
            connections = POOL,
            |$latch, $pool| {
                let (app, egress) = $build;
                (app, egress, ())
            },
            wrap = |live, ()| live,
            |publisher, index| publisher
                .message(&Reply { id: 1 })
                .with_headers(record_header(i64::try_from(index).expect("an id fits") + 1))
                .publish()
                .map(|published| {
                    published.expect("the message is published");
                }),
        )
    };
}

fn none_run(messages: usize) -> Pending {
    delivering!(messages, |latch, pool| {
        drop(pool);
        memory::delivery(latch)
    })
}

fn by_hand_run(messages: usize) -> Pending {
    delivering!(messages, |latch, pool| memory::delivery_by_hand(
        pool, latch
    ))
}

fn outbox_run(messages: usize) -> Pending {
    delivering!(messages, |latch, pool| memory::delivery_outbox(pool, latch))
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
#[bench::first(by_hand_run(1))]
#[bench::base(by_hand_run(MESSAGES))]
#[bench::twice(by_hand_run(2 * MESSAGES))]
fn by_hand(run: Pending) {
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

library_benchmark_group!(name = outbox_delivery_group; benchmarks = none, by_hand, outbox);
main!(library_benchmark_groups = outbox_delivery_group);
