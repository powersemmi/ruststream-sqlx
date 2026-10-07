//! Shared parts of the crate's benchmarks: the tables, the services and the raw sqlx loops they
//! are measured against, the stand they run on, and how a run knows it is over.
//!
//! # What a scenario is
//!
//! Every scenario is written twice. The service is what a user writes: a `#[derive(Inbox)]`
//! table, a `#[subscriber(..)]` handler, and `RustStream::new(..).with_broker(SqlxBroker::new(pool),
//! ..)` started like any app ([`services`]). The raw half is a hand-written sqlx loop that runs the
//! statements the broker runs for that table, in the order it runs them, with the same pool and
//! the same decode ([`raw`]). The statements are rendered once, at setup, by the dialect the
//! broker renders them with, so the two halves cannot drift apart ([`raw::Statements`]).
//!
//! The three kinds of benchmark take the same scenarios:
//!
//! - `just bench-code` counts instructions (callgrind) and allocations (DHAT) on a
//!   single-threaded runtime ([`code`]);
//! - `just bench` times them on a multi-threaded runtime, in interleaved rounds (`paired.rs`),
//!   and drains a filled table with n workers on Postgres, MySQL and SQLite (`throughput.rs`).
//!
//! # How a run knows it is over
//!
//! A run fills its table with a known number of rows first, so the count of deliveries a drain
//! takes is known before it starts. Every handled delivery counts down a [`Latch`] the handler
//! reaches as the application state, and the raw loop counts down the same latch at the same
//! point: after the decode, before the settlement. The last count wakes the waiter through a
//! `Notify`, so a run waits on no timer and polls nothing. The latch also notes when the first
//! and the last delivery were handled, which is the window the wall clock reads: what starting
//! the service costs stays outside it.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

pub mod code;
pub mod raw;
pub mod services;
pub mod stand;
pub mod tables;
pub mod timing;

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, LibraryBenchmarkConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::timeout;

/// Deliveries per measured run of the code-cost benchmarks.
///
/// The default is large enough that entering and leaving the region is lost in the per-message
/// number, and small enough that a scenario stays within a minute of valgrind time.
/// `RUSTSTREAM_BENCH_MESSAGES` at build time overrides it (`just bench-code 5000`); the published
/// document is measured at the default, and the allocation limits scale with the count through
/// [`config`]. The recipe hands the same count to `scripts/bench_results.py`, which divides by it.
pub const MESSAGES: usize = messages(option_env!("RUSTSTREAM_BENCH_MESSAGES"));

/// The count a run measures when nothing names one.
const DEFAULT_MESSAGES: usize = 1_000;

/// The configured count, or the default; a value that is not a positive number is a build error
/// naming the variable, so a typo cannot silently measure the default.
const fn messages(configured: Option<&str>) -> usize {
    let Some(text) = configured else {
        return DEFAULT_MESSAGES;
    };
    let bytes = text.as_bytes();
    let mut count = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        let digit = bytes[index];
        assert!(
            digit.is_ascii_digit(),
            "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
        );
        count = count * 10 + (digit - b'0') as usize;
        index += 1;
    }
    assert!(
        count > 0,
        "RUSTSTREAM_BENCH_MESSAGES must be a positive number of deliveries"
    );
    count
}

/// The measurement configuration every code-cost scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service (or connecting the raw loop's pool) and taking the first delivery allocate once;
/// together they are the hard limit the longest run of the scenario (twice [`MESSAGES`]
/// deliveries) is held to. The instruction limit is relative, and `just bench-code` sets it only
/// for a run against a named baseline.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come a whole number per delivery: `steady`
/// blocks per `per` deliveries.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        // The runner clears the environment of the measured process, and the scenario needs to
        // know where the database is.
        .pass_through_env(stand::POSTGRES)
        .tool(callgrind())
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a statement inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=160"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// The lease the lease form's scenarios take: the broker's default, which the raw loop writes too.
pub const LEASE: Duration = Duration::from_secs(30);

/// The size of a batch in the batch scenarios.
pub const BATCH: NonZeroUsize = NonZeroUsize::new(16).expect("a batch holds a row");

/// The values every payload carries. Fixed, so every delivery of a run costs the same.
pub const ID: u64 = 1_000_000;
pub const QUANTITY: u32 = 37;
/// The customer a row-mode row carries, the same text the payload's neighbours carry as bytes.
pub const CUSTOMER: &str = "ops@example.com";

/// The payload every payload-mode scenario decodes: two integer fields, so a decode allocates
/// nothing and the number is about the crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// The JSON body every payload row carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// The name a publish scenario sends under.
pub const ORDERS: &str = "orders";

/// What a publish scenario sends: the same two fields, encoded by the codec on every publish.
#[derive(Debug, Serialize, ruststream::Outgoing)]
#[outgoing(name = "orders")]
pub struct OrderPlaced {
    pub id: u64,
    pub quantity: u32,
}

impl OrderPlaced {
    /// The one value every publish sends.
    pub const fn fixed() -> Self {
        Self {
            id: ID,
            quantity: QUANTITY,
        }
    }
}

/// Counts deliveries down, wakes the waiter on the last one, and notes when the first and the
/// last were handled.
///
/// Handlers reach it as the application state, and the raw loops hold a clone. What a delivery
/// pays for it is one relaxed decrement and two comparisons; the first and the last also read
/// the clock.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    total: usize,
    remaining: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Latch {
    /// A latch for a run of `total` deliveries.
    pub fn new(total: usize) -> Self {
        Self(Arc::new(Inner {
            total,
            remaining: AtomicUsize::new(total),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        let before = self.0.remaining.fetch_sub(1, Ordering::Relaxed);
        if before == self.0.total {
            let _ = self.0.first.set(Instant::now());
        }
        if before == 1 {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// The deliveries the run expects.
    pub fn total(&self) -> usize {
        self.0.total
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.remaining() > 0 {
            // `notify_one` keeps a permit for a waiter that has not arrived yet, so the last
            // count cannot be lost between the check and the wait.
            self.0.drained.notified().await;
        }
    }

    /// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
    pub async fn drained_or_stalled(&self, half: &str) {
        let mut left = self.remaining();
        loop {
            if timeout(STALL, self.drained()).await.is_ok() {
                return;
            }
            let now = self.remaining();
            assert!(
                now < left,
                "{half}: {now} of {} deliveries left and nothing moved for {STALL:?}",
                self.0.total
            );
            left = now;
        }
    }

    /// The measured window: the first delivery to the end of the last one's handling.
    pub fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);
