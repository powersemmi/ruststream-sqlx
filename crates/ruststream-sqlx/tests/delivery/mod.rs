//! What a handler is handed and what each outcome does to its row: acknowledgements, events a
//! service implements itself, and the clocks a table reads.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "testing"))]

mod database_clock;
mod outcomes;
mod own_clock;
mod own_events;
