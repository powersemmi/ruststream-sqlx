//! What a handler is handed, what each outcome and failure does, and tables described by hand.
//!
//! One binary of the integration suite; the manifest says how the suite is split.

// Every module that reaches a stand serves the inbox.
#[cfg(feature = "inbox")]
mod live;

mod delivery;
mod failures;
mod manual;
