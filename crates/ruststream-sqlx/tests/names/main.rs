//! Publishing into tables, routes and by-name subscriptions: the broker's publishing side, a
//! handler mounted by name, and a by-name row that settles through the service's own code.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "json"))]

#[path = "../live/mod.rs"]
mod live;

mod by_name;
mod own_ack;
mod publish;
