# SQL databases

`ruststream-sqlx` brings SQL databases into a [RustStream](https://powersemmi.github.io/ruststream/)
service through [`sqlx`](https://docs.rs/sqlx). It has two components:

- the inbox: task queues in the service's own Postgres, MySQL/MariaDB and SQLite tables, served
  as a RustStream broker;
- the transactional outbox: what a service publishes on any RustStream broker stays recorded in a
  table of its own until a consumer has processed it.

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "outbox", "postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
```

The `inbox` and `outbox` features turn the components on, each independently of the other. A
driver feature picks the database: `postgres`, `mysql` (MySQL and MariaDB), `sqlite`, or `any` for
an `AnyPool`.

## Task queues in the service's tables

```rust
--8<-- "crates/ruststream-sqlx/examples/inbox.rs:service"
```

`#[derive(Inbox)]` describes the queue table: its name, and the role each column plays.
`SqlxBroker` serves the queues of the service's pool. A subscription claims rows, and the
handler's outcome settles them: an acknowledgement deletes the row, a retry returns it to the
queue. A publish writes a row through the service's own statement.

A subscription claims its rows in one of three forms: by row lock, in a transaction open while the
handler runs; by lease, with the claim committed at once; or by an advisory lock on the row's key.
A handler mounted with `.transactional()` writes through its delivery's transaction, and the
acknowledgement commits its writes together with the row. Each role a column plays turns on one
behaviour: groups, the order rows are taken in, delayed retries, an attempt cap with a dead letter,
a mark that keeps a processed row.

The manual API is the other path to the same table, beside the derive: the struct implements a
trait, and a typed builder holds every setting. The compiler checks it as strictly as the derive,
and both give the same statements at the same cost.

A team that checks its SQL at compile time adds `checked` to the derive, and `cargo sqlx prepare`
then checks the derive's statements together with the service's own queries. A test runs the
service's own app in `TestApp` against a real database, since the service's SQL is part of what it
checks; SQLite needs no server for it.

## The transactional outbox

```rust
--8<-- "crates/ruststream-sqlx/examples/outbox.rs:service"
```

`#[derive(Outbox)]` describes the service's outbox table, and `outbox!` registers it under the
names it tracks. The publish middleware records each tracked message before it is sent, with the
record's id in a header. The subscription middleware takes the record into work by that id and
marks it processed when the handler acknowledges. At startup the republish sends every unprocessed
record again.

A message is delivered at least once. Two instances of a service that start together may both
send the same record again, so a consumer that must not process a message twice checks that
itself.

Inside `#[ruststream::app]` the app is built before the Tokio runtime starts, and sqlx builds a
pool only inside the runtime. The registry therefore starts without a pool, and `on_startup`
builds one and hands it over with `set_pool`. A test build leaves the outbox off unless the
environment sets `RUSTSTREAM_SQLX_OUTBOX=on`.

The outbox's middlewares wrap inbox handlers as they wrap any other. A record is written on a
connection of its own, so it commits even when the delivery's transaction rolls back. A message
tracked into an inbox table keeps its record id where the table has a `headers` column that the
service's publish statement writes. The rest is in
[the transactional outbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-transactional-outbox).

## Which one to use

The inbox fits work that belongs to the service's data: a task written in the same transaction as
the data it serves, a queue in the database the service already runs. The outbox fits messages a
service publishes on another broker and must not lose. A service may use both.

## Where the rest is

The reference on docs.rs opens with the crate's own textbook, one section per topic:

- [The inbox broker](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-inbox-broker):
  a queue table, its subscriptions, and what a handler's outcome does to a row.
- [Macro or manual](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#macro-or-manual):
  a queue table described by hand, through `InboxTable` and its typed builder.
- [Roles](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#roles) and
  [time](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#time): what each column
  turns on, and where "now" comes from.
- [Row locks, leases or advisory locks](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#row-locks-leases-or-advisory-locks)
  and
  [transactional mode](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#transactional-mode):
  how a subscription claims rows, and a handler that writes through its delivery's transaction.
- [Row mode](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#row-mode),
  [the headers layout](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-headers-layout)
  and [batches](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#batches): what a
  handler takes from a row.
- [Databases](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#databases) and
  [a dialect of the service's own](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#a-dialect-of-the-services-own):
  what each database runs, and a dialect for another database.
- [Statements checked at compile time](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#statements-checked-at-compile-time):
  the `checked` mode.
- [Waking a subscription](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#waking-a-subscription):
  the poll interval, and `LISTEN/NOTIFY` on Postgres.
- [Testing a service on the inbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#testing-a-service-on-the-inbox):
  the service's own app in `TestApp`.
- [The transactional outbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-transactional-outbox):
  the guarantee, the record and its registry, the middlewares, the republish, the pool, testing
  and costs.

What the crate costs per message and how many messages it moves per second, beside a raw sqlx loop
doing the same work, is on the [benchmarks page](benchmarks.md).

Handlers, routers, codecs and middleware come from the framework, whose own entry pages start at
[the RustStream site](https://powersemmi.github.io/ruststream/).
