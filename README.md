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

- **The transactional outbox, over any RustStream broker:** `#[derive(Outbox)]` describes the
  service's outbox table, and `outbox!` registers it under the names it tracks. A publish
  middleware records each tracked message before it is sent, with the record's id in a header. A
  subscription middleware takes the record into work by that id and marks it processed when the
  handler acknowledges. Unprocessed records are published again at startup, so each message is
  delivered at least once.
- **The inbox, task queues in database tables:** `SqlxBroker` serves them from the tables and
  structs the service owns, on Postgres, MySQL 8.0.1 and later, MariaDB 10.6 and later, and
  SQLite. A subscription takes rows by row lock, in a transaction open while the handler runs; by
  lease, with the claim committed at once and the lease extended while the handler works; or by an
  advisory lock on the row's key. SQLite tables take the lease or the advisory lock form.

Each table is described by a derive on the service's struct, or by hand through a trait and a
typed builder that the compiler checks as strictly.

## When to use which

The inbox fits work that belongs to the service's data: a task written in the same transaction as
the data it serves, a queue in the database the service already runs. The outbox fits messages a
service publishes on another broker and must not lose. A service may use both: the outbox's
middlewares wrap inbox handlers as they wrap any other.

## Crates

- `ruststream-sqlx`: the crate a service depends on.
- `ruststream-sqlx-macros`: its procedural macros.
- `ruststream-sqlx-dialect`: the SQL text generation shared by the macros and the runtime.

## Documentation

- This crate: <https://docs.rs/ruststream-sqlx>
- A service on each component: [`crates/ruststream-sqlx/examples`](./crates/ruststream-sqlx/examples)
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.95**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
