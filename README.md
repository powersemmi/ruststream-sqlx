<h1 align="center">ruststream-sqlx</h1>

<p align="center">
  <i>SQL databases for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework through sqlx: a transactional outbox over any RustStream broker, and task queues in Postgres, MySQL/MariaDB and SQLite tables.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-sqlx/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-sqlx/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-sqlx"><img src="https://img.shields.io/crates/v/ruststream-sqlx.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-sqlx"><img src="https://img.shields.io/crates/dr/ruststream-sqlx" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-sqlx"><img src="https://img.shields.io/docsrs/ruststream-sqlx" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-sqlx/">Documentation</a></b>
</p>

---

`ruststream-sqlx` brings SQL databases into a RustStream service through
[`sqlx`](https://crates.io/crates/sqlx). Handlers, routing, codecs and middleware come from the
framework; this crate connects them to the database.

## Components

- **A transactional outbox over any RustStream broker:** the messages a handler publishes are
  stored in the transaction that holds its own writes, and reach the broker once it commits.
- **Task queues in database tables:** Postgres, MySQL/MariaDB and SQLite, over the tables and
  structs the service owns.

## Crates

- `ruststream-sqlx`: the crate a service depends on.
- `ruststream-sqlx-macros`: its procedural macros.
- `ruststream-sqlx-dialect`: the SQL text generation shared by the macros and the runtime.

## Documentation

- This crate: <https://docs.rs/ruststream-sqlx>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
