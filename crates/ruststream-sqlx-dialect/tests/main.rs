//! The integration suite, one test binary: each file or directory under `tests/` is a module
//! of it.

mod insert;
mod mysql;
mod postgres;
mod sqlite;
mod table_spec;
