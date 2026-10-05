# ruststream-sqlx-dialect

SQL text generation for [`ruststream-sqlx`](https://github.com/powersemmi/ruststream-sqlx), the SQL
database crate of the [RustStream](https://github.com/powersemmi/ruststream) messaging framework.

The procedural macros of `ruststream-sqlx` and its runtime build their statements here, so both
produce the same SQL for a table. The crate generates plain text, free of any database driver, so a
procedural macro can run it at compile time.

## License

Apache-2.0.
