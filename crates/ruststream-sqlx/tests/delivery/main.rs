//! What a handler is handed and what each outcome does to its row: acknowledgements, retries and
//! dead letters, statements that fail, rows that do not decode, events a service implements
//! itself, and the database's clock.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

#[path = "../live/mod.rs"]
mod live;

mod database_clock;
mod decoding;
mod failures;
mod outcomes;
mod own_events;
mod retries;
