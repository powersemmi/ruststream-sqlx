//! SQL text generation for [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx), the SQL database
//! crate of the [RustStream](https://github.com/powersemmi/ruststream) messaging framework.
//!
//! The procedural macros of `ruststream-sqlx` and its runtime build their statements here, so both
//! produce the same SQL for a table. The crate generates plain text, free of any database driver,
//! so a procedural macro can run it at compile time.
//!
//! A [`TableSpec`] describes a queue table: its name, the [`Column`] that identifies a row, one
//! slot per [`Role`], the columns of the message's data, and the [`Form`] in which rows are
//! claimed. The id is part of the constructor, every role has one slot, and the form carries its
//! own columns, so a table without an id, with a role played twice or with two forms cannot be
//! described.

#![forbid(unsafe_code)]

mod spec;

pub use spec::{Column, Form, KeyPart, Role, TableSpec};
