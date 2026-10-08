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
//! The outbox as a plugin, a tracked publish from outside a handler over `MemoryBroker` on
//! Postgres, the way an HTTP endpoint publishes. With no outbox the message goes straight to the
//! broker. By hand the service inserts the record first and sends the message with the record's
//! id in a header. This crate's outbox does the same through the publisher `Outbox::wrap` returns.

mod common;

use common::MESSAGES;
use common::code::{Pending, feeding, start_and_drain};
use common::outbox::{ByHand, Reply, memory, record_header};
use common::stand::Table;
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_sqlx::prelude::*;

/// Connections every variant's pool may open: the record's insert and room to spare.
const POOL: u32 = 2;

/// The body the hand-written variant records: what the codec encodes for [`Reply`].
const BODY: &[u8] = br#"{"id":1}"#;

fn none_run(messages: usize) -> Pending {
    feeding!(
        messages,
        table = Table::Outbox,
        fill = false,
        connections = POOL,
        |latch, pool| {
            drop(pool);
            let (app, egress) = memory::publishing();
            (app, egress, latch)
        },
        wrap = |live, latch| (live, latch),
        |publisher, _index| async {
            let (publisher, latch) = &publisher;
            publisher
                .message(&Reply { id: 1 })
                .publish()
                .await
                .expect("the message is published");
            latch.arrived();
        },
    )
}

fn by_hand_run(messages: usize) -> Pending {
    feeding!(
        messages,
        table = Table::Outbox,
        fill = false,
        connections = POOL,
        |latch, pool| {
            let (app, egress) = memory::publishing();
            (app, egress, (latch, pool))
        },
        wrap = |live, (latch, pool)| (live, latch, pool),
        |publisher, _index| async {
            let (publisher, latch, pool) = &publisher;
            let id = ByHand::record(pool, BODY).await;
            publisher
                .message(&Reply { id: 1 })
                .with_headers(record_header(id))
                .publish()
                .await
                .expect("the message is published");
            latch.arrived();
        },
    )
}

fn outbox_run(messages: usize) -> Pending {
    feeding!(
        messages,
        table = Table::Outbox,
        fill = false,
        connections = POOL,
        |latch, pool| {
            let (app, egress, tracking) = memory::publishing_outbox(pool);
            (app, egress, (tracking, latch))
        },
        wrap = |live, (tracking, latch)| (tracking.wrap(live), latch),
        |publisher, _index| async {
            let (publisher, latch) = &publisher;
            publisher
                .message(&Reply { id: 1 })
                .publish()
                .await
                .expect("the message is recorded and published");
            latch.arrived();
        },
    )
}

// Twice MESSAGES deliveries allocated 4,095 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 5 blocks.
#[library_benchmark(config = common::config_every(2_048, 1_000, 5))]
#[bench::first(none_run(1))]
#[bench::base(none_run(MESSAGES))]
#[bench::twice(none_run(2 * MESSAGES))]
fn none(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 32,175 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 33 blocks.
#[library_benchmark(config = common::config_every(16_088, 1_000, 33))]
#[bench::first(by_hand_run(1))]
#[bench::base(by_hand_run(MESSAGES))]
#[bench::twice(by_hand_run(2 * MESSAGES))]
fn by_hand(run: Pending) {
    start_and_drain(run);
}

// Twice MESSAGES deliveries allocated 30,201 blocks over 5 runs. The floor is the
// highest, stated over a thousand deliveries, plus a 0.1% margin of 31 blocks.
#[library_benchmark(config = common::config_every(15_101, 1_000, 31))]
#[bench::first(outbox_run(1))]
#[bench::base(outbox_run(MESSAGES))]
#[bench::twice(outbox_run(2 * MESSAGES))]
fn outbox(run: Pending) {
    start_and_drain(run);
}

library_benchmark_group!(name = outbox_publish_group; benchmarks = none, by_hand, outbox);
main!(library_benchmark_groups = outbox_publish_group);
