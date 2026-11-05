//! The core's conformance suites, what a struct describes, and the wake-up.
//!
//! One binary of the integration suite; the manifest says how the suite is split.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]

// Every module that reaches a stand serves the inbox.
#[cfg(feature = "inbox")]
mod live;

mod conformance;
mod describe;
mod wake;
