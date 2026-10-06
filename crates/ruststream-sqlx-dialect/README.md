# ruststream-sqlx-dialect

SQL text generation for [`ruststream-sqlx`](https://github.com/powersemmi/ruststream-sqlx), the SQL
database crate of the [RustStream](https://github.com/powersemmi/ruststream) messaging framework.

The procedural macros of `ruststream-sqlx` and its runtime build their statements here, so both
produce the same SQL for a table. The crate generates plain text, free of any database driver, so a
procedural macro can run it at compile time.

A `TableSpec` describes a queue table, and a `Dialect` turns it into the statement each queue event
runs. Three dialects are built in, each behind its feature: Postgres (`postgres`) and MySQL with
MariaDB (`mysql`) build the row lock, lease and advisory lock forms, and SQLite (`sqlite`) builds
the lease and advisory lock forms. A database without a built-in dialect is served by a type of
the service's own that implements `Dialect` and the trait of each form it builds.

## License

Apache-2.0.
