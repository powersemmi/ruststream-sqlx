//! How each form claims, holds and settles its rows: the lease form, batches, FIFO groups, two
//! claimers on one table, the advisory lock form, and a claim cut short at shutdown.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod advisory;
mod batches;
mod claims;
mod concurrency;
mod fifo;
mod lease;
mod shutdown;
