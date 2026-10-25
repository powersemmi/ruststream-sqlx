//! Row mode: a table without a payload field hands its handler the row itself, `&Row`, on every
//! stand and in every form, transactional mode included, or a batch of them as one slice,
//! `&[Row]`; a delivery with no row to lend settles by the decode policy; a `Repository` over a
//! `Publish` of the service's own writes its rows.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod batch;
mod publish;
mod refused;
mod single;
mod transactional;

use std::time::Duration;

/// How long a subscription waits after a claim that found its queue short.
const POLL: Duration = Duration::from_millis(20);

/// How long a test lets the claim loop run: every row it wrote settles in it, a retried one twice.
const SETTLED: Duration = Duration::from_millis(300);
