//! What a subscription does when a statement fails or a row does not decode: the claim loop goes
//! on, and the row settles by the policy that covers it.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

#[path = "../live/mod.rs"]
mod live;

mod decoding;
mod failures;
