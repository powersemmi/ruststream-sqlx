# SQL databases

`ruststream-sqlx` brings SQL databases into a [RustStream](https://powersemmi.github.io/ruststream/)
service through [`sqlx`](https://docs.rs/sqlx). It has two components:

- A transactional outbox over any RustStream broker. The messages a handler publishes are stored
  in the transaction that holds its own writes, and the broker receives them once the transaction
  commits.
- Task queues in Postgres, MySQL/MariaDB and SQLite tables that the service owns.

## Where the rest is

The crate's reference is on docs.rs: [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx).

Handlers, routers, codecs and middleware come from the framework, whose own entry pages start at
[the RustStream site](https://powersemmi.github.io/ruststream/).
