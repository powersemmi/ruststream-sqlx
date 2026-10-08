//! Subscriptions on dedicated threads, run as a service on live databases: `threads(n)` settles
//! on the threads' runtimes, the handlers' connections outlive them, transactional mode and the
//! outbox cross between the runtimes, `threads(n, by_key)` keeps a key's order, and shutdown
//! leaves nothing locked.
//!
//! The test harness runs a subscription's threads as workers on the test's own runtime, so these
//! suites start the service itself.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod locks;
mod probe;
mod runtimes;
