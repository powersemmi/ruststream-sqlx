//! How each form claims, holds and settles its rows: the lease form, batches, FIFO groups, two
//! claimers on one table, the advisory lock form, and rows written before the app started.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod advisory;
mod backlog;
mod batches;
mod claims;
mod concurrency;
mod fifo;
mod lease;
