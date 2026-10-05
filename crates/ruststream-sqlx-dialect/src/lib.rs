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
//! described. A [`Dialect`] turns the description into the [`Statement`] each queue event runs; a
//! statement carries the [`Param`]s its placeholders bind, in order.
//!
//! [`Postgres`] is built in, behind the `postgres` feature, and builds the statements of the row
//! lock form. A database without a built-in dialect is served by a type of the service's own
//! that implements [`Dialect`].
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "postgres")] {
//! use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Postgres, TableSpec};
//!
//! const JOBS: TableSpec<'static> =
//!     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
//!
//! let claim = Postgres.claim(&JOBS, ClaimShape::Rows)?;
//! assert_eq!(
//!     claim.sql(),
//!     r#"SELECT "job_id", "payload" FROM "jobs" ORDER BY "job_id" LIMIT $1 FOR UPDATE SKIP LOCKED"#,
//! );
//! # }
//! # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
//! ```

#![forbid(unsafe_code)]

mod dialect;
#[cfg(feature = "postgres")]
mod postgres;
mod spec;
mod statement;
mod table_name;
#[cfg(feature = "postgres")]
mod writer;

pub use dialect::Dialect;
#[cfg(feature = "postgres")]
pub use postgres::Postgres;
pub use spec::{Column, Form, KeyPart, Role, TableSpec};
pub use statement::{ClaimShape, Param, Statement, StatementError};
pub use table_name::{ParseTableNameError, TableName};
