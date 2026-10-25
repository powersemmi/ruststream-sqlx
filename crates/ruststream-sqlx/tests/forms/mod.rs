//! How each form claims, holds and settles its rows: the lease form, batches, FIFO groups, two
//! claimers on one table, the advisory lock form, rows written before the app started, and a
//! claim cut short at shutdown.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod advisory;
mod backlog;
mod batches;
mod claims;
mod concurrency;
mod fifo;
mod lease;
mod shutdown;
