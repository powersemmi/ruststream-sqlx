//! What a struct and its subscription describe, without a database: what `#[derive(Inbox)]` reads
//! from a struct and hands the broker, the statements Postgres builds from it, and what a
//! subscription adds to the `AsyncAPI` document.

#![cfg(feature = "inbox")]

mod asyncapi;
mod events;
mod statements;
mod table;
