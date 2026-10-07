//! The wake-up: a publish from the same process wakes the subscriptions it wrote for, with no
//! poll interval in between, on every stand and in every form.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod same_process;

use std::time::Duration;

/// A poll interval no test outlives: a row a test sees handled was claimed because a publish woke
/// its subscription.
const INTERVAL: Duration = Duration::from_secs(3600);

/// How long the harness waits for what a test published to be handled.
const WOKEN: Duration = Duration::from_secs(1);

/// How long a test lets a subscription run before it writes: the claim each subscription runs as
/// it opens finds the table empty, so the subscription waits its interval.
const IDLE: Duration = Duration::from_millis(100);

/// How long a test watches a subscription it expects to stay asleep.
const ASLEEP: Duration = Duration::from_millis(300);
