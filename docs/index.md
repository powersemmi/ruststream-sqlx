# SQL databases

`ruststream-sqlx` brings SQL databases into a [RustStream](https://powersemmi.github.io/ruststream/)
service through [`sqlx`](https://docs.rs/sqlx). It has two components:

- A transactional outbox over any RustStream broker. A publish records the message in a table
  the service owns, and the message carries the record's id. The subscription takes the task into
  work by that id and marks it processed on acknowledgement. Unprocessed records are published
  again at startup.
- Task queues in Postgres, MySQL/MariaDB and SQLite tables that the service owns.

A subscription takes its rows in one of two forms. In the row lock form it locks a row in a
transaction that stays open until the handler settles the row. In the lease form it writes a lease
into the row and commits at once, so a long handler holds no transaction, and the subscription
extends the lease while the handler works. SQLite tables take the lease form.

The service owns its queue tables. At startup a subscription checks that its table has the
columns its struct names. The column types are the service's to get right, and a row that does
not decode is settled by the subscription's decode-failure policy.

## Where the rest is

The crate's reference is on docs.rs: [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx).

Handlers, routers, codecs and middleware come from the framework, whose own entry pages start at
[the RustStream site](https://powersemmi.github.io/ruststream/).
