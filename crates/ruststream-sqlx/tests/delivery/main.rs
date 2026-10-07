//! What a handler is handed and what each outcome does to its row: acknowledgements, retries and
//! dead letters, statements that fail, rows that do not decode, events a service implements
//! itself, the database's clock, and the row itself where a table has no payload field.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

#[path = "../live/mod.rs"]
mod live;

mod database_clock;
mod decoding;
mod failures;
mod outcomes;
mod own_events;
mod retries;
mod row_mode;
