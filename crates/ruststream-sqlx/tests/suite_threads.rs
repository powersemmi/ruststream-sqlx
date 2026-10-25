//! Subscriptions on dedicated threads, and the transactional outbox.
//!
//! One binary of the integration suite; the manifest says how the suite is split.

// Every module that reaches a stand serves the inbox.
#[cfg(feature = "inbox")]
mod live;

mod outbox;
mod threads;
