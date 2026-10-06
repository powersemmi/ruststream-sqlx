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
//! behind the `postgres` feature, its MySQL and MariaDB dialect behind `mysql`, and its SQLite
//! dialect behind `sqlite`. An `AnyPool` (feature `any`) takes the dialect of the database it
//! reaches, picked when the broker connects ([`BuiltInDialect`]). Its rows hold only the types
//! `sqlx::Any` carries, and no time is among them, so the lease form, `retry_after` and
//! `processed_at` are out of its reach.
//!
//! # The inbox broker
//!
//! ```no_run
//! # #[cfg(all(feature = "postgres", feature = "chrono"))]
//! # mod demo {
//! use std::time::Duration;
//!
//! use chrono::{DateTime, Utc};
//! use ruststream::OutgoingMessage;
//! use ruststream_sqlx::prelude::*;
//! use serde::Deserialize;
//! use sqlx::{PgConnection, PgPool, Postgres};
//!
//! // email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, retry_after TIMESTAMPTZ DEFAULT now(),
//! // attempt SMALLINT DEFAULT 1, processed_at TIMESTAMPTZ, payload BYTEA
//! #[derive(Inbox, sqlx::FromRow)]
//! #[inbox(table = "email_jobs")]
//! pub struct SendEmail {
//!     #[field(id, generated)]
//!     job_id: i64,
//!     #[field(group)]
//!     name: String,
//!     #[field(retry_after, generated)]
//!     retry_after: DateTime<Utc>,
//!     #[field(attempt, generated)]
//!     attempt: i16,
//!     #[field(processed_at, generated)]
//!     processed_at: Option<DateTime<Utc>>,
//!     #[field(payload)]
//!     payload: Vec<u8>,
//! }
//!
//! impl Publish<Postgres> for SendEmail {
//!     async fn publish(
//!         conn: &mut PgConnection,
//!         message: &OutgoingMessage<'_>,
//!     ) -> Result<(), sqlx::Error> {
//!         sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
//!             .bind(message.name())
//!             .bind(message.payload())
//!             .execute(conn)
//!             .await?;
//!         Ok(())
//!     }
//! }
//!
//! #[derive(Deserialize)]
//! pub struct Email {
//!     to: String,
//! }
//!
//! # async fn deliver(_: &Email) -> bool { true }
//! #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
//! async fn send(email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
//!     if deliver(email).await {
//!         return HandlerOutcome::ack();
//!     }
//!     // The handler owns the backoff: a minute per attempt.
//!     HandlerOutcome::retry_after(Duration::from_secs(60 * attempt.unwrap_or(1)))
//! }
//!
//! pub fn app(pool: PgPool) -> RustStream {
//!     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
//!         SqlxBroker::new(pool).route::<SendEmail>("emails"),
//!         |b| {
//!             b.include(send);
//!         },
//!     )
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! [`SqlxBroker`] serves the queues in the tables of the service's sqlx pool, and the pool stays
//! the service's. An [`InboxQueue`] subscription claims rows with `FOR UPDATE SKIP LOCKED`, one
//! transaction per message or per batch, and polls when the queue runs dry. On MySQL and MariaDB
//! that transaction runs at READ COMMITTED, so a claim locks no gaps between rows and holds back
//! no insert into its table. A batch's settlements take effect together, when its last delivery
//! settles. A batch that holds a row whose statement always fails rolls back on every attempt and
//! returns all of its rows, until the service's SQL or schema is fixed.
//!
//! A table with a `#[field(locked_until)]` field is claimed by lease instead ([`LeaseRow`]): the
//! claim writes the lease's expiry, counts the attempt and commits at once, so the handler runs
//! outside any transaction. The subscription extends the lease of every delivery in work each half
//! lease, so a handler may outlast its lease; after a crash the row returns once the lease runs
//! out. A settlement takes effect only while the row still holds the lease, and one that finds the
//! row under another lease fails with [`SqlxBrokerError::LeaseLost`]. A delivery dropped unsettled
//! releases its row at once. Each delivery of a batch settles on its own. SQLite has no row locks
//! ([`RowLocks`]), so its tables take the lease form: a subscription there to a table without
//! `locked_until` does not compile. Its claim is one update that returns the rows it took, in no
//! particular order within a batch.
//!
//! In the row lock form a message in work holds a connection of the pool until it settles, a
//! batch holds one for all its messages, and a publish takes one more for its insert. A
//! subscription with `workers(n)` holds up to n + 1 connections, and handlers that publish need
//! room for their inserts on top: a pool without that room makes them wait for its
//! `acquire_timeout`. In the lease form a message takes a connection only to settle, and each
//! subscription takes one each half lease to extend the leases in work. The name
//! selects a group where the table has one; without a group the table is one queue. The bytes
//! reach the codec lent from the row. What a handler answers decides the row's fate:
//!
//! - `ack()` deletes the row, or sets `processed_at` where the table has it;
//! - `retry()` releases the row at once, and its `attempt` grows;
//! - `retry_after(d)` hides the row until `retry_after` comes; a table without that column
//!   releases it at once, and the runtime logs a warning;
//! - `drop()` finishes the row as `ack()` does;
//! - `max_attempts(n)` on the mount reads `attempt`, and with `dead_letter(..)` a spent row moves
//!   to another group or to a table with the same columns; without one it is finished.
//!
//! "Now" comes from [`SystemClock`] unless the struct names another source:
//! `#[inbox(clock = DatabaseClock)]` reads the database's `now()`, and a service's own [`Clock`]
//! fits there too. Hosts that bind "now" must keep their clocks in step. SQLite keeps times as
//! text and compares them as text, which orders `chrono` times exactly and `time` values to the
//! second ([`QueueTime`]).
//!
//! A mistake stops the service as early as it can be seen. A subscription prepares its statements
//! at startup, and a column the table lacks stops it with the table and the statement named.
//! Preparing checks the names of the table and its columns, not the column types: the types are the
//! service's to get right. On MySQL and MariaDB a subscription also reads the server's version when
//! it opens: claims skip locked rows, which MySQL 8.0.1 and MariaDB 10.6 added, and an older server
//! stops it with [`SqlxBrokerError::ServerTooOld`]. A claimed row whose columns do not decode into
//! the struct reaches the subscription's `on_failure(decode = ..)` policy, which settles it (a drop
//! by default), and the subscription goes on with the next row; a batch keeps its other rows. A
//! handler that takes the bytes themselves, through a `Deserialized` type, receives an empty
//! payload for such a row. A row whose id does not decode fails the claim, and the error names the
//! subscription and the table.
//!
//! A [`Repository`] publishes into its struct's table, and a struct without [`Publish`] does not
//! compile as one. A route leads a name to a table, and a publish to a name no route leads
//! anywhere fails at publish time. A publish through a route costs one hash lookup and one
//! dynamic call. It allocates what a [`Repository`] publish does, unless the struct's `Publish`
//! future is larger than 1024 bytes ([`Routed`]). `#[subscriber("emails")]` opens through the
//! route too. A route whose struct leaves every event to the crate, with role columns of the types
//! the crate reads itself, is read by those columns: no box and no dynamic call per message, as
//! through an [`InboxQueue`]. A struct that overrides an event, or holds another column type,
//! costs one boxed delivery and one boxed settlement future per message.
//!
//! # Testing a service on the inbox
//!
//! ```no_run
//! # #[cfg(all(feature = "postgres", feature = "testing"))]
//! # mod demo {
//! # use ruststream::OutgoingMessage;
//! # use ruststream_sqlx::prelude::*;
//! # use serde::{Deserialize, Serialize};
//! # use sqlx::{PgConnection, Postgres};
//! # #[derive(Inbox, sqlx::FromRow)]
//! # #[inbox(table = "email_jobs")]
//! # pub struct SendEmail { #[field(id, generated)] job_id: i64, #[field(group)] name: String, #[field(payload)] payload: Vec<u8> }
//! # impl Publish<Postgres> for SendEmail {
//! #     async fn publish(_: &mut PgConnection, _: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> { Ok(()) }
//! # }
//! # #[derive(Serialize, Deserialize, Outgoing)]
//! # pub struct Email { to: String }
//! # #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
//! # async fn send(_: &Email) -> HandlerOutcome { HandlerOutcome::ack() }
//! # pub fn app(pool: PgPool) -> RustStream {
//! #     RustStream::new(AppInfo::new("mailer", "0.1.0"))
//! #         .with_broker(SqlxBroker::new(pool).route::<SendEmail>("emails"), |b| { b.include(send); })
//! # }
//! use std::error::Error;
//!
//! use ruststream::testing::TestApp;
//! use sqlx::PgPool;
//!
//! // `pool` reaches a database of the test's own, with the service's migrations applied.
//! pub async fn an_email_is_sent(pool: PgPool) -> Result<(), Box<dyn Error + Send + Sync>> {
//!     let tb = TestApp::start_live(app(pool)).await?;
//!     tb.broker::<SqlxBroker<Postgres>>()
//!         .message(&Email { to: "a@example.com".to_owned() })
//!         .to("emails")
//!         .publish()
//!         .await?;
//!     tb.broker::<SqlxBroker<Postgres>>()
//!         .subscriber("emails")
//!         .assert_called_once();
//!     tb.shutdown().await?;
//!     Ok(())
//! }
//! # }
//! # fn main() {}
//! ```
//!
//! A test runs the service against a real database, because the service's SQL is part of what it
//! checks. The message goes in through the route and the service's [`Publish`], as in
//! production. The clock is real as well: a paused tokio clock jumps to the next timer while a
//! database reply is in flight, so a test starts with `TestApp::start_live`, and `tb.advance(by)`
//! lets that much real time pass.

#![forbid(unsafe_code)]

pub use ruststream_sqlx_dialect as dialect;

#[cfg(feature = "inbox")]
mod inbox;
#[cfg(feature = "inbox")]
pub mod prelude;

#[cfg(feature = "inbox")]
pub use inbox::{
    Ack, AttemptColumn, BuiltInDialect, Claim, Clock, ClosedSqlxBroker, ConnectedSqlxBroker,
    DatabaseClock, DeadLetter, Discard, Extend, Fetch, HeaderColumn, InboxDelivery, InboxQueue,
    InboxRow, InboxSubscriber, Insert, KeyColumn, LeaseRow, NamedDelivery, NamedSubscriber,
    PayloadRow, Publish, QueueDatabase, QueueTime, Repository, RepositoryPublisher, Retry,
    RetryAfter, Routed, RoutedPublisher, RowLocks, SqlxBroker, SqlxBrokerError, SystemClock,
    TimeColumn, TimeSource,
};

/// What a handler reads off the delivery it handles, through `Ctx<Key>`.
#[cfg(feature = "inbox")]
pub use inbox::keys;

#[cfg(feature = "inbox")]
#[doc(hidden)]
pub mod __private {
    pub use ruststream::HeaderMap;
    pub use ruststream_sqlx_dialect::Param;
    pub use sqlx;

    pub use crate::inbox::engine::{
        Claimed, Claiming, Event, Events, IdAt, Now, Prepared, Settled, Settling, Shape, Stmt,
        TimeFor, Values, Via, ack, claim_ids, claim_rows, dead_letter, discard, expiry, extend,
        fetch_by_ids, first_header, later, match_claimed, match_rows, micros, now, put, retry,
        retry_after,
    };
    pub use crate::inbox::kinds::{Kinds, KindsOf};
    pub use crate::inbox::named::{
        NamedBytes, NamedDatabase, NamedId, NamedRow, NamedTime, RoleColumns,
    };
    pub use crate::inbox::queue::Queue;
    pub use crate::inbox::{
        AdvisoryForm, FormOn, InsertSql, LeaseForm, OnConnection, QueueDatabase, QueueRow,
        RowLockForm, no_insert,
    };
}

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
/// - `id`, required: the row's identity. Its type is `Clone`: a lease subscription keeps a copy
///   of each id in work to extend its lease.
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
/// and `Clone + Debug + Send + Sync + 'static` of the `id` field's type.
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
/// `fifo = true` beside `advisory_lock`, `extend` in `custom(..)` without `locked_until`,
/// `locked_until` on `clock = DatabaseClock`, a lock key naming no field, a dot in `table` or
/// `schema`.
#[cfg(feature = "inbox")]
pub use ruststream_sqlx_macros::Inbox;
