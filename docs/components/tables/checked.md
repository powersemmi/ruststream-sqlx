---
sidebar_position: 6
title: checked
description: "#[inbox(checked, db = ..)] puts the statements a struct determines into sqlx's compile-time check against DATABASE_URL or the .sqlx offline data."
---

# checked

`#[inbox(checked, db = ..)]` puts the statements a struct determines into sqlx's compile-time check against `DATABASE_URL` or the `.sqlx` offline data.

`db` names the built-in dialect the statements are built with: `postgres`, `mysql` or `sqlite`.
The derive builds them with the code the broker runs when a subscription opens, and wraps each in `sqlx::query!` inside a function nothing calls.
The service's build then checks each statement against its database: a column the struct names and the table lacks fails the build.
The subscription runs the statements it runs without `checked`, so a message costs the same.

`checked` needs three things of the service:

- its own sqlx under the name `sqlx`, with the `macros` feature and the database's driver;
- `DATABASE_URL` at build time, or the `.sqlx` data that `cargo sqlx prepare` writes, committed beside the crate; `SQLX_OFFLINE=true` makes a build read that data even where `DATABASE_URL` is set, and `cargo sqlx prepare --check` fails a CI run whose data is stale;
- the feature of the dialect `db` names, on `ruststream-sqlx`.

Each statement meets sqlx's own rules: with both `chrono` and `time` on, `sqlx.toml` names the preferred crate for time columns ([sqlx's compile-time macros](https://docs.rs/sqlx/0.9/sqlx/macro.query.html)).

The check covers every statement the struct determines: the claims, the guard of a [FIFO group](../subscriptions/fifo.md), the fetch after a claim of the service's own, the settlements, the dead letter of a group, the lease's extension and stamp, the advisory lock form's lock, unlock and take, and the generated insert.
The [startup check](../broker/startup.md) still runs, and still holds the move into a dead-letter table, which the mount names.
In the [headers layout](../headers/inbox-headers.md) `checked` goes on the headers struct, and the message reads its rows in a [`Fetch`](../headers/fetch.md) of its own, checked with `sqlx::query!`.

A struct `checked` cannot cover does not compile, and the error points at the attribute or the field: `checked` without `db` or the reverse, a `db` whose feature is off, a `#[sqlx(flatten)]` field, a clock that is a parameter of the struct, the row lock form on SQLite, and `checked` on a message assembled from a headers struct.
A [dialect of the service's own](../databases/dialect.md) keeps the startup check, because a procedural macro cannot run it.

## Example

The email queue of a Postgres service, its statements checked against the offline data in `.sqlx`.
Its test runs on the Postgres stand that `POSTGRES_TEST_URL` names, and skips without it.

{/* single-form: checked is an attribute of the derive; a table described by hand checks its own statements with sqlx::query! and keeps the startup check */}
```rust reference="examples/sqlx_tables_checked.rs#table"
```
