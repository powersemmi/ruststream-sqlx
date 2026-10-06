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
//! In the lease form a claim writes the lease's expiry ([`Param::Lease`]) into each row it takes
//! and commits, and skips every row whose lease has not ended by [`Param::LeaseNow`]. The expiry
//! it wrote is the delivery's ownership token ([`Param::Held`]): a settlement, or an extension
//! that moves the expiry forward ([`Dialect::extend`]), passes only while the row still holds it.
//! A dialect whose claim only selects the rows says so ([`Dialect::claim_writes_lease`]), and each
//! claimed row is then stamped with its lease ([`Dialect::stamp`]).
//!
//! [`Postgres`], [`MySql`] and [`Sqlite`] are built in, behind the `postgres`, `mysql` and
//! `sqlite` features. [`Postgres`] and [`MySql`] build the statements of the row lock and lease
//! forms, and [`MySql`] serves MariaDB too; [`Sqlite`] builds the lease form. A database without a
//! built-in dialect is served by a type of the service's own that implements [`Dialect`].
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

mod column;
mod dialect;
mod form;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
mod role;
mod spec;
#[cfg(feature = "sqlite")]
mod sqlite;
mod statement;
mod table_name;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod writer;

pub use column::Column;
pub use dialect::Dialect;
pub use form::{Form, KeyPart};
#[cfg(feature = "mysql")]
pub use mysql::MySql;
#[cfg(feature = "postgres")]
pub use postgres::Postgres;
pub use role::Role;
pub use spec::TableSpec;
#[cfg(feature = "sqlite")]
pub use sqlite::Sqlite;
pub use statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
pub use table_name::{ParseTableNameError, TableName};
