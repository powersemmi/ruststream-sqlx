//! What each database serves, and a dialect of the service's own: an `AnyPool`, what MySQL and
//! MariaDB add, the names SQLite quotes, and a dialect that wraps the built-in one.

#![cfg(feature = "inbox")]

#[path = "../live/mod.rs"]
mod live;

mod any_pool;
mod mysql_family;
mod own_dialect;
mod sqlite_names;
