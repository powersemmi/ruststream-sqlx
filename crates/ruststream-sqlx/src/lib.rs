//! SQL databases for the [RustStream](https://github.com/powersemmi/ruststream) messaging
//! framework, through [`sqlx`](https://docs.rs/sqlx).
//!
//! The crate brings two components to a service: a transactional outbox over any RustStream
//! broker, and task queues in Postgres, MySQL/MariaDB and SQLite tables.
//!
//! A queue table is described by an ordinary struct of the service's own. [`Inbox`] reads the
//! table from `#[inbox(..)]`, the column names from sqlx's attributes and the role of each
//! column that runs the queue from `#[field(..)]`, and implements [`InboxRow`] (feature
//! `inbox`). The [`dialect`] module turns the description into SQL; its Postgres dialect sits
//! behind the `postgres` feature.

#![forbid(unsafe_code)]

pub use ruststream_sqlx_dialect as dialect;

#[cfg(feature = "inbox")]
mod inbox;

#[cfg(feature = "inbox")]
pub use inbox::InboxRow;

/// Describes a queue table with a struct and implements [`InboxRow`] for it.
///
/// The struct is an ordinary sqlx struct: `#[inbox(..)]` names the table, sqlx's own
/// attributes name the columns, and `#[field(..)]` marks the columns that run the queue.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx::dialect::{ClaimShape, Dialect, Postgres};
/// use ruststream_sqlx::{Inbox, InboxRow};
///
/// #[derive(Inbox)]
/// #[inbox(table = "email_jobs", schema = "app")]
/// struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(attempt)]
///     attempt: i16,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// // The statement a subscription to `SendEmail` claims its rows with.
/// let claim = Postgres.claim(&SendEmail::SPEC, ClaimShape::Rows)?;
/// assert_eq!(
///     claim.sql(),
///     r#"SELECT "job_id", "name", "attempt", "payload" FROM "app"."email_jobs" WHERE "name" = $1 ORDER BY "job_id" LIMIT $2 FOR UPDATE SKIP LOCKED"#,
/// );
/// # }
/// # Ok::<(), ruststream_sqlx::dialect::StatementError>(())
/// ```
///
/// # The table
///
/// `#[inbox(table = "..")]` names the table and is required. `schema = ".."` places it in a
/// schema. Each names one thing and holds no dot: the schema goes into `schema`, never into the
/// table's name. `advisory_lock = "jobs-{job_id}"` selects the advisory lock form, with a key built
/// from the fields named between braces. A placeholder names a field as written in Rust, without
/// `r#` (`{type}` for `r#type`), and the key reads that field's column. In that form a group
/// keeps its order through the key, as in `advisory_lock = "jobs-{name}"`, so `fifo = true` does
/// not apply there.
///
/// # Roles
///
/// `#[field(..)]` gives a field one role:
///
/// - `id`, required: the row's identity.
/// - `group`: the group a subscription reads; `#[field(group, fifo = true)]` keeps each group in
///   order.
/// - `partition_key`: the delivery's partition key.
/// - `priority`: the claim order, a smaller value first.
/// - `retry_after`: the time before which a row is not claimed.
/// - `attempt`: the attempt number.
/// - `locked_until`: the lease's expiry; it selects the lease form.
/// - `processed_at`: acknowledgement marks the row instead of deleting it.
/// - `headers` and `payload`: the delivery's headers and the message bytes.
///
/// `generated`, alone or beside a role, marks a column the database fills in.
///
/// A generic struct keeps its parameters: the impl requires `Send + Sync + 'static` of the struct
/// and of the `id` field's type.
///
/// # Column names
///
/// A column is named in one place, sqlx's attributes. `#[sqlx(rename = "..")]` names a field's
/// column as written, `#[sqlx(rename_all = "..")]` recases every other field's name, a raw
/// identifier loses its `r#`, and a `#[sqlx(skip)]` field reads no column. A
/// `#[sqlx(flatten)]` field reads columns the derive cannot see, so the statements select `*`,
/// and a dead-letter move copies the row by position: the dead-letter table has the same columns
/// in the same order.
/// The other options of `#[sqlx(..)]`, such as `json`, `try_from` and `default`, belong to sqlx's
/// own derive and pass through untouched.
///
/// # Compile errors
///
/// A struct that cannot drive a queue does not compile, and the error points at the field or
/// the name that causes it: no `id`, a role played twice, a column named twice, a role or
/// `generated` on a field without a column, `fifo` outside the `group` role, `locked_until` or
/// `fifo = true` beside `advisory_lock`, a lock key naming no field, a dot in `table` or `schema`.
#[cfg(feature = "inbox")]
pub use ruststream_sqlx_macros::Inbox;
