//! Publishing into tables, routes and by-name subscriptions, and the headers layout.
//!
//! One binary of the integration suite; the manifest says how the suite is split.

// Every module that reaches a stand serves the inbox.
#[cfg(feature = "inbox")]
mod live;

mod headers;
mod names;
