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
//! [`RowLock`] builds the claim that locks rows for the transaction a handler settles in,
//! [`Lease`] the claim that writes a lease into each row and commits, with the lease's extension,
//! and [`Advisory`] the claim of candidates with their lock keys, the lock and the unlock of a key,
//! and the take of a row whose key the session holds. A dialect implements the traits of the forms
//! its database serves, and a table in a form its dialect lacks does not compile.
//!
//! In the lease form a claim writes the lease's expiry ([`Param::Lease`]) into each row it takes
//! and commits, and skips every row whose lease has not ended by [`Param::LeaseNow`]. The expiry
//! it wrote is the delivery's ownership token ([`Param::Held`]): a settlement, or an extension
//! that moves the expiry forward ([`Lease::extend`]), passes only while the row still holds it. A
//! dialect whose claim only selects the rows says so ([`Lease::claim_writes_lease`]), and each
//! claimed row is then stamped with its lease ([`Lease::stamp`]).
//!
//! In the advisory lock form a session lock on each row's key holds the row. The key's parts
//! ([`KeyPart`]) name literal text and columns, and the database renders each row's key from them
//! as text. A claim selects candidates with their keys ([`Advisory::advisory_claim`]), takes the
//! lock on a key ([`Advisory::lock`], bound as [`Param::Key`]), then the row while it is still
//! claimable ([`Advisory::take`]); the unlock ([`Advisory::unlock`]) follows the settlement. The
//! settlements name the row alone, and a retry needs no statement: the take counted the attempt,
//! and the unlock frees the row.
//!
//! A table with FIFO groups ([`TableSpec::fifo_group`]) keeps one row of a group in work. Its
//! claim takes the group's head, the first unfinished row of the group in claim order, or
//! nothing. The claim's transaction first takes the group with the statement
//! [`Dialect::fifo_guard`] gives, so a row that enters the group ahead of the head in work waits
//! for it; a lease claim also takes nothing while a row of the group holds a lease.
//!
//! A table may open its transactions at an isolation level ([`Isolation`]) or, on SQLite, in a
//! mode ([`Mode`]): its [`Opening`], set with [`TableSpec::isolation`] and [`TableSpec::mode`].
//! [`Dialect::begin`] gives the statement that opens a transaction at it, and refuses an opening
//! the database lacks. A dialect also states the openings it serves as types, one [`Opens`]
//! implementation per [`level`] type, so a table that names a level its dialect lacks does not
//! compile.
//!
//! [`Postgres`], [`MySql`] and [`Sqlite`] are built in, behind the `postgres`, `mysql` and
//! `sqlite` features. [`Postgres`] and [`MySql`] implement [`RowLock`], [`Lease`] and
//! [`Advisory`], and [`MySql`] serves MariaDB too; [`Sqlite`] implements [`Lease`] and
//! [`Advisory`]. A database without a built-in dialect takes a type of the service's own: it
//! implements [`Dialect`], the trait of each form it builds and [`Opens`] for each level it opens,
//! and writes every statement in its database's SQL. A service on a built-in dialect that writes
//! one statement its own way writes it as an event of its inbox: `#[inbox(custom(ack))]` and an
//! `Ack` implementation of its own, in
//! [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#an-event-of-the-services-own).
//!
//! # Examples
//!
//! A dialect for SQL Server, a database without a built-in one, in the row lock form. The service
//! mounts it with `SqlxBroker::with_dialect`, as the [`ruststream-sqlx`
//! overview](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/#a-dialect-of-the-services-own)
//! shows.
//!
//! ```
//! # use std::num::NonZeroUsize;
//! use ruststream_sqlx_dialect::{
//!     ClaimShape, Dialect, Opening, Param, RowLock, Statement, StatementError, TableSpec,
//! };
//! # use ruststream_sqlx_dialect::TableName;
//!
//! /// SQL Server: bracket-quoted names, `@p1` placeholders, rows claimed under an update lock that
//! /// skips the rows another claim holds.
//! #[derive(Debug)]
//! pub struct Mssql;
//!
//! impl Dialect for Mssql {
//!     fn name(&self) -> &'static str {
//!         "mssql"
//!     }
//!
//!     fn quote_into(&self, ident: &str, out: &mut String) {
//!         out.push('[');
//!         out.push_str(&ident.replace(']', "]]"));
//!         out.push(']');
//!     }
//!
//!     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
//!         out.push_str("@p");
//!         out.push_str(&index.to_string());
//!     }
//!
//!     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
//!         let mut sql = String::from("DELETE FROM ");
//!         self.quote_into(spec.table(), &mut sql);
//!         sql.push_str(" WHERE ");
//!         self.quote_into(spec.id().name(), &mut sql);
//!         sql.push_str(" = ");
//!         self.placeholder_into(NonZeroUsize::MIN, &mut sql);
//!         Ok(Statement::new(sql, [Param::Id]))
//!     }
//!
//!     // SQL Server starts a transaction with `BEGIN TRANSACTION`.
//!     fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
//!         match opening {
//!             Opening::Default => Ok(Some("BEGIN TRANSACTION")),
//!             other => Err(StatementError::UnsupportedOpening {
//!                 dialect: self.name(),
//!                 opening: other.name(),
//!             }),
//!         }
//!     }
//!
//!     // The statements this service never runs refuse the table.
//! #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
//! #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
//! #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
//! #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
//! #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
//! #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
//! #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
//! }
//!
//! // The row lock form, which the service's `email_jobs` table takes.
//! impl RowLock for Mssql {
//!     fn lock_claim(
//!         &self,
//!         _spec: &TableSpec<'_>,
//!         _shape: ClaimShape,
//!     ) -> Result<Statement, StatementError> {
//!         Ok(Statement::new(
//!             "SELECT TOP (@p1) [job_id], [payload] FROM [email_jobs] \
//!              WITH (UPDLOCK, READPAST, ROWLOCK) ORDER BY [job_id]",
//!             [Param::Limit],
//!         ))
//!     }
//! }
//! ```

#![forbid(unsafe_code)]

mod advisory;
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

pub use advisory::Advisory;
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
