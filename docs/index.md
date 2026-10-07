# SQL databases

`ruststream-sqlx` brings SQL databases into a [RustStream](https://powersemmi.github.io/ruststream/)
service through [`sqlx`](https://docs.rs/sqlx). It has two components:

- A transactional outbox over any RustStream broker. A publish records the message in a table
  the service owns, and the message carries the record's id. The subscription takes the task into
  work by that id and marks it processed on acknowledgement. Unprocessed records are published
  again at startup.
- Task queues in Postgres, MySQL/MariaDB and SQLite tables that the service owns.

A subscription takes its rows in one of three forms. In the row lock form it locks a row in a
transaction that stays open until the handler settles the row. In the lease form it writes a lease
into the row and commits at once, so a long handler holds no transaction, and the subscription
extends the lease while the handler works. In the advisory lock form it locks the row's key in the
database session of the connection that serves the delivery. No transaction stays open, and rows
that share a key go into work one at a time. SQLite tables take the lease or the advisory lock
form.

A handler takes a row's message, which a codec decodes from the table's payload column. A table
without a payload column is in row mode: the handler takes the row itself, the service's own
struct as sqlx read it, with no codec in between. A batch handler takes the rows of one claim as
one slice.

A handler mounted with `.transactional()` writes through its delivery's transaction, in every
form. Acknowledgement commits the handler's writes and finishes the row in one transaction. Every
other outcome rolls the writes back. While a delivery is in work, its transaction holds a
connection of the pool.

A subscription caps the deliveries of a message with `max_attempts(n)` and names, with
`dead_letter(..)`, the group or the table a row moves to once its attempts are spent. The two are
declared together. With `max_attempts(1)`, every failure moves the row at once.

Postgres, MySQL/MariaDB and SQLite come with dialects built into the crate. A database whose sqlx
driver comes in a crate of its own, or a statement the service writes its own way, takes a dialect
of the service's own. Such a dialect implements a trait for each form its tables take, and one for
subscriptions by name; a table in a form its dialect lacks does not compile.

The service owns its queue tables. At startup a subscription checks that its table has the
columns its struct names. The column types are the service's to get right, and a row that does
not decode is settled by the subscription's decode-failure policy.

## Where the rest is

The crate's reference is on docs.rs: [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx).

Handlers, routers, codecs and middleware come from the framework, whose own entry pages start at
[the RustStream site](https://powersemmi.github.io/ruststream/).
