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
//! described. A dialect turns the description into the [`Statement`] each queue event runs; a
//! statement carries the [`Param`]s its placeholders bind, in order.
//!
//! A dialect is a set of traits. [`Dialect`] quotes names, numbers placeholders and builds the
//! statements every form runs to settle a row. Each form of claiming is a trait over it:
//! [`RowLock`] builds the claim that locks rows for the transaction a handler settles in, and
//! [`Lease`] the claim that writes a lease into each row and commits, with the lease's extension.
//! A dialect implements the traits of the forms its database serves, and a table in a form its
//! dialect lacks does not compile.
//!
//! In the lease form a claim writes the lease's expiry ([`Param::Lease`]) into each row it takes
//! and commits, and skips every row whose lease has not ended by [`Param::LeaseNow`]. The expiry
//! it wrote is the delivery's ownership token ([`Param::Held`]): a settlement, or an extension
//! that moves the expiry forward ([`Lease::extend`]), passes only while the row still holds it. A
//! dialect whose claim only selects the rows says so ([`Lease::claim_writes_lease`]), and each
//! claimed row is then stamped with its lease ([`Lease::stamp`]).
//!
//! A table may open its transactions at an isolation level ([`Isolation`]) or, on SQLite, in a
//! mode ([`Mode`]): its [`Opening`], set with [`TableSpec::isolation`] and [`TableSpec::mode`].
//! [`Dialect::begin`] gives the statement that opens a transaction at it, and refuses an opening
//! the database lacks. A dialect also states the openings it serves as types, one [`Opens`]
//! implementation per [`level`] type, so a table that names a level its dialect lacks does not
//! compile.
//!
//! [`Postgres`], [`MySql`] and [`Sqlite`] are built in, behind the `postgres`, `mysql` and
//! `sqlite` features. [`Postgres`] and [`MySql`] implement [`RowLock`] and [`Lease`], and
//! [`MySql`] serves MariaDB too; [`Sqlite`] implements [`Lease`]. A database without a built-in
//! dialect, or a service that writes a statement its own way, takes a type of the service's own:
//! it implements [`Dialect`], the trait of each form it builds and [`Opens`] for each level it
//! opens, with each statement its own or delegated to a built-in dialect it wraps.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "postgres")] {
//! use ruststream_sqlx_dialect::{ClaimShape, Column, Form, Postgres, RowLock, TableSpec};
//!
//! const JOBS: TableSpec<'static> =
//!     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
//!
//! let claim = Postgres.lock_claim(&JOBS, ClaimShape::Rows)?;
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
mod lease;
#[cfg(feature = "mysql")]
mod mysql;
mod opening;
#[cfg(feature = "postgres")]
mod postgres;
mod role;
mod row_lock;
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
pub use lease::Lease;
#[cfg(feature = "mysql")]
pub use mysql::MySql;
pub use opening::{Isolation, Mode, Opening, Opens, level};
#[cfg(feature = "postgres")]
pub use postgres::Postgres;
pub use role::Role;
pub use row_lock::RowLock;
pub use spec::TableSpec;
#[cfg(feature = "sqlite")]
pub use sqlite::Sqlite;
pub use statement::{ClaimShape, NameLimit, Param, Statement, StatementError};
pub use table_name::{ParseTableNameError, TableName};
