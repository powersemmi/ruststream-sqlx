//! The headers layout: a headers struct describes the queue table, and the message a handler takes
//! flattens it beside data of its own, on every stand and in every form. The handler takes the
//! assembled row; the delivery's headers hold the headers struct's fields without a role, built on
//! the first read, in a single delivery and in a batch; the lease and the advisory lock hold the
//! row as they hold a flat table's; a message column the table lacks stops the subscription at
//! startup. A fetch of the service's own joins other tables into the message, and a claimed id it
//! finds no row for settles by the decode policy with empty headers.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod forms;
mod joined;
mod layout;
mod lazy;
mod values;

use std::time::Duration;

/// How long a subscription waits after a claim that found its queue short.
const POLL: Duration = Duration::from_millis(20);

/// How long a test lets the claim loop run: every row it wrote settles in it, a retried one twice.
const SETTLED: Duration = Duration::from_millis(300);
