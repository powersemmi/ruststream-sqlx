//! The transactional outbox as a service runs it: an app on `MemoryBroker` whose publishes are
//! recorded in a real database's outbox table, whose deliveries take and settle their records, and
//! whose startup republishes what no consumer processed.
//!
//! A test of tracking runs only with `RUSTSTREAM_SQLX_OUTBOX=on`, and the test of a build that
//! leaves the outbox off runs only without it: a test build reads the switch once per process, so
//! the suite cannot turn it on or off for itself.

#![cfg(all(
    feature = "outbox",
    feature = "inbox",
    feature = "json",
    feature = "testing",
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite"
))]

use std::env;

#[path = "../live/mod.rs"]
mod live;

mod records;
mod registry;
mod stand;
mod tracked;

/// The variable that turns the outbox on in a test build.
const SWITCH: &str = "RUSTSTREAM_SQLX_OUTBOX";

/// Whether the outbox tracks in this process, or `false` to skip a test of tracking.
///
/// # Panics
///
/// Panics when the switch is off and [`live::REQUIRE_LIVE`] is set: a job that asked for the live
/// suites would pass the outbox's without running them.
fn tracking_on() -> bool {
    if env::var(SWITCH).as_deref() == Ok("on") {
        return true;
    }
    assert!(
        env::var(live::REQUIRE_LIVE).map_or(true, |value| value.is_empty()),
        "{} is set, so the outbox suite must run, but {SWITCH} is not `on`",
        live::REQUIRE_LIVE,
    );
    eprintln!("{SWITCH} is not `on`; skipping the outbox suite");
    false
}
