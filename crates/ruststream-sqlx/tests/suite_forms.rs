//! How each form claims, holds and settles its rows.
//!
//! One binary of the integration suite; the manifest says how the suite is split.

// Every module that reaches a stand serves the inbox.
#[cfg(feature = "inbox")]
mod live;

mod forms;
