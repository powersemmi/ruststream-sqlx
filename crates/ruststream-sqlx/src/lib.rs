#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

pub use ruststream_sqlx_dialect as dialect;

#[cfg(feature = "inbox")]
mod inbox;
#[cfg(feature = "inbox")]
pub mod prelude;

/// Machinery behind [`InboxSettings::transactional`]; a service never names it.
#[cfg(feature = "inbox")]
#[doc(hidden)]
pub use inbox::TransactionalStep;
#[cfg(feature = "inbox")]
pub use inbox::{
    Ack, AttemptColumn, BuiltIn, BuiltInDialect, ByName, Claim, Clock, ClosedSqlxBroker,
    ConnectedSqlxBroker, DatabaseClock, DeadLetter, Discard, Extend, Fetch, HeaderColumn,
    InboxDelivery, InboxQueue, InboxRow, InboxSettings, InboxSubscriber, Insert, KeyColumn,
    LeaseRow, Lock, NamedDelivery, NamedSubscriber, NamedTime, PayloadRow, Plain, Publish,
    QueueDatabase, QueueTime, Repository, RepositoryPublisher, Retry, RetryAfter, Routed,
    RoutedPublisher, SqlxBroker, SqlxBrokerError, SystemClock, TimeColumn, TimeSource,
    Transactional, Tx, Unlock,
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

    #[cfg(feature = "any")]
    pub use crate::inbox::AnyDialect;
    pub use crate::inbox::engine::{
        Candidates, Claimed, Claiming, Event, Events, IdAt, Leasing, Now, Prepared, Savepoint,
        Settled, Settling, Shape, Stmt, TimeFor, Values, Via, ack, attempt_in, candidates,
        claim_ids, claim_rows, dead_letter, discard, extend, fetch_by_ids, first_header, later,
        lease, lock, match_claimed, match_rows, match_taken, micros, no_lease, now, put, retry,
        retry_after, take, take_id, unlock,
    };
    pub use crate::inbox::kinds::{Kinds, KindsOf};
    pub use crate::inbox::named::{NamedBytes, NamedId, NamedRow, RoleColumns};
    pub use crate::inbox::queue::Queue;
    pub use crate::inbox::{
        AdvisoryForm, FormDialect, FormOn, InboxMode, InsertSql, LeaseForm, OnConnection,
        QueueDatabase, QueueRow, RowLockForm, no_insert,
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
/// use ruststream_sqlx::dialect::{ClaimShape, Postgres, RowLock};
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
/// let claim = Postgres.lock_claim(&SendEmail::SPEC, ClaimShape::Rows)?;
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
/// not apply there. The form selects its candidates with their keys itself, so `custom(..)` does
/// not take `claim` beside it; `custom(lock, unlock)` hands the lock and the unlock of each key to
/// the service ([`Lock`], [`Unlock`]). Every built-in dialect builds the lease and advisory lock
/// forms; Postgres and MySQL build the row lock form too.
///
/// # Isolation and mode
///
/// `isolation = <level>` opens the table's transactions at an isolation level:
/// `read_uncommitted`, `read_committed`, `repeatable_read` or `serializable`. `mode = <mode>`
/// opens them in a SQLite mode instead: `deferred`, `immediate` or `exclusive`. A table names one
/// of the two, or neither and opens at its database's default (READ COMMITTED on MySQL and
/// MariaDB). The row lock claim's transaction opens at it.
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx::dialect::{Dialect, Postgres};
/// use ruststream_sqlx::{Inbox, InboxRow};
///
/// #[derive(Inbox)]
/// #[inbox(table = "ledger_jobs", isolation = repeatable_read)]
/// struct Posting {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// // The statement a subscription to `Posting` opens its claims with on Postgres.
/// let begin = Postgres.begin(Posting::SPEC.opening())?;
/// assert_eq!(begin, Some("BEGIN ISOLATION LEVEL REPEATABLE READ"));
/// # }
/// # Ok::<(), ruststream_sqlx::dialect::StatementError>(())
/// ```
///
/// The broker's dialect opens what its database keeps: Postgres `read_committed`,
/// `repeatable_read` and `serializable`, MySQL and MariaDB all four levels, SQLite the three
/// modes. A subscription to a table its dialect does not open does not compile, and the error
/// names the levels the dialect opens. An `AnyPool` reaches a database named only when the broker
/// connects, so there the subscription stops when it starts instead. On Postgres a row lock table
/// at `repeatable_read` or `serializable` fails claims with serialization errors when claims and
/// settlements of its rows run at once; `read_committed` is the practical level there.
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
/// `generated` on a field without a column, `fifo` outside the `group` role, `locked_until`,
/// `fifo = true` or `claim` in `custom(..)` beside `advisory_lock`, `extend` in `custom(..)`
/// without `locked_until`, `lock` or `unlock` in `custom(..)` without `advisory_lock` or without
/// each other, `locked_until` on `clock = DatabaseClock`, a lock key naming no field, a dot in
/// `table` or `schema`, an unknown isolation level or mode, `isolation` beside `mode`.
#[cfg(feature = "inbox")]
pub use ruststream_sqlx_macros::Inbox;
