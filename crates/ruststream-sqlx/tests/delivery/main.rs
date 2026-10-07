//! What a handler is handed and what each outcome does to its row: acknowledgements, events a
//! service implements itself, and the database's clock.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

#[path = "../live/mod.rs"]
mod live;

mod database_clock;
mod outcomes;
mod own_events;
