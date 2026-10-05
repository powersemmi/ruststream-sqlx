//! SQL databases for the [RustStream](https://github.com/powersemmi/ruststream) messaging
//! framework, through [`sqlx`](https://docs.rs/sqlx).
//!
//! The crate brings two components to a service: a transactional outbox over any RustStream
//! broker, and task queues in Postgres, MySQL/MariaDB and SQLite tables.

#![forbid(unsafe_code)]
