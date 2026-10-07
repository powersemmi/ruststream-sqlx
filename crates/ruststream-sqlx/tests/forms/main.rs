//! How each form claims, holds and settles its rows: the lease form, batches, FIFO groups, two
//! claimers on one table, and the advisory lock form.

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
mod concurrency;
mod fifo;
mod lease;
