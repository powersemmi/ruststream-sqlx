#![doc = include_str!("README.md")]
#![doc = include_str!("overview/row_mode.md")]
// The compile errors of row mode need the inbox and a driver: without them each example would fail
// for that reason alone. They render where both are on, as on docs.rs.
#![cfg_attr(
    all(feature = "inbox", feature = "postgres"),
    doc = include_str!("overview/row_mode_errors.md")
)]
#![doc = include_str!("overview/headers.md")]
#![doc = include_str!("overview/forms.md")]
#![doc = include_str!("overview/transactional.md")]
#![doc = include_str!("overview/isolation.md")]
#![doc = include_str!("overview/fifo.md")]
#![doc = include_str!("overview/databases.md")]
#![doc = include_str!("overview/own_dialect.md")]
#![doc = include_str!("overview/decoding.md")]
#![doc = include_str!("overview/batches.md")]
#![doc = include_str!("overview/names.md")]
#![doc = include_str!("overview/wake.md")]
#![doc = include_str!("overview/testing.md")]
#![cfg_attr(feature = "outbox", doc = include_str!("overview/outbox.md"))]
#![forbid(unsafe_code)]

pub use ruststream_sqlx_dialect as dialect;

#[cfg(any(feature = "inbox", feature = "outbox"))]
mod header_column;
#[cfg(feature = "outbox")]
pub mod outbox;

#[cfg(any(feature = "inbox", feature = "outbox"))]
pub use header_column::HeaderColumn;
#[cfg(feature = "outbox")]
pub use outbox::{Outbox, OutboxDatabase, OutboxRow};

#[cfg(feature = "inbox")]
mod inbox;
#[cfg(feature = "inbox")]
pub mod prelude;

/// Machinery behind [`RowBatch`]'s deliveries; a service never names it.
#[cfg(feature = "inbox")]
#[doc(hidden)]
pub use inbox::RowDeliveries;
/// Machinery behind [`InboxSettings::transactional`]; a service never names it.
#[cfg(feature = "inbox")]
#[doc(hidden)]
pub use inbox::TransactionalStep;
#[cfg(feature = "inbox")]
pub use inbox::{
    Ack, AttemptColumn, BuiltIn, BuiltInDialect, ByName, Claim, Clock, ClosedSqlxBroker,
    ConnectedSqlxBroker, DatabaseClock, DeadLetter, Discard, Extend, Fetch, HeaderField,
    InboxDelivery, InboxHeaders, InboxQueue, InboxRow, InboxSettings, InboxSubscriber, Insert,
    KeyColumn, LeaseRow, Lock, NamedDelivery, NamedSubscriber, NamedTime, Notifies, PayloadRow,
    Plain, Publish, QueueDatabase, QueueTime, Repository, RepositoryPublisher, Retry, RetryAfter,
    Routed, RoutedPublisher, RowBatch, SqlxBroker, SqlxBrokerError, SystemClock, TimeColumn,
    TimeSource, Transactional, Tx, Unlock,
};

/// What a handler reads off the delivery it handles, through `Ctx<Key>`.
#[cfg(feature = "inbox")]
pub use inbox::keys;

#[cfg(any(feature = "inbox", feature = "outbox"))]
#[doc(hidden)]
pub mod __private {
    pub use ruststream::HeaderMap;
    pub use sqlx;

    #[cfg(feature = "outbox")]
    pub use crate::outbox::{OutboxSql, no_outbox_statement};

    #[cfg(feature = "inbox")]
    pub use inbox::*;

    /// What the inbox derives expand to.
    #[cfg(feature = "inbox")]
    mod inbox {
        pub use ruststream::runtime::{Input, SoloCarried};
        pub use ruststream_sqlx_dialect::Param;

        #[cfg(feature = "any")]
        pub use crate::inbox::AnyDialect;
        pub use crate::inbox::batch::{BatchClaim, BatchLane};
        pub use crate::inbox::engine::{
            Claimed, Claiming, Event, Events, IdAt, Leasing, Now, Prepared, Savepoint, Settled,
            Settling, Shape, Stmt, TimeFor, Values, Via, ack, attempt_in, claim_ids, claim_rows,
            dead_letter, discard, extend, fetch_by_ids, first_header, later, lease, match_claimed,
            match_rows, micros, no_lease, now, put, retry, retry_after,
        };
        pub use crate::inbox::form::advisory::events::{
            Candidates, candidates, lock, match_taken, take, take_id, unlock,
        };
        pub use crate::inbox::headers::{
            Assembled, HeaderCell, HeadersLease, HeadersRow, LazyHeaders, OwnClaim, OwnExtend,
            OwnLock, put_header, unnamed_header,
        };
        pub use crate::inbox::named::kinds::{Kinds, KindsOf};
        pub use crate::inbox::named::{NamedBytes, NamedId, NamedRow, RoleColumns};
        pub use crate::inbox::queue::Queue;
        pub use crate::inbox::{
            AdvisoryForm, FormDialect, FormOn, InboxMode, InsertSql, Lane, LeaseForm, OnConnection,
            PayloadLane, QueueDatabase, QueueRow, RowLane, RowLockForm, no_insert,
        };
    }
}

/// Describes a queue table with a struct and implements [`InboxRow`] for it.
///
/// The struct is an ordinary sqlx struct: `#[inbox(..)]` names the table, sqlx's own
/// attributes name the columns, and `#[field(..)]` marks the columns that run the queue.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// // app.email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, attempt SMALLINT DEFAULT 1,
/// // payload BYTEA
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs", schema = "app")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(attempt, generated)]
///     attempt: i16,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// // The subscription claims the rows of group `emails`, oldest first, and skips the rows
/// // another worker holds.
/// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
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
/// MariaDB). The row lock claim's transaction opens at it, and so does the transaction a delivery
/// lends its handler in transactional mode, in every form.
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::{PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "ledger_jobs", advisory_lock = "ledger-{id}", isolation = repeatable_read)]
/// pub struct Posting {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Entry {
///     account: String,
///     cents: i64,
/// }
///
/// // The handler's transaction opens with `BEGIN ISOLATION LEVEL REPEATABLE READ`: the balance it
/// // checks stays as its first statement read it.
/// #[subscriber(InboxQueue::<Posting>::new("postings"))]
/// async fn post(entry: &Entry, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
///     let posted = sqlx::query(
///         "INSERT INTO ledger (account, cents) SELECT $1, $2 \
///          WHERE (SELECT sum(cents) FROM ledger WHERE account = $1) + $2 >= 0",
///     )
///     .bind(&entry.account)
///     .bind(entry.cents)
///     .execute(&mut *tx)
///     .await;
///     if posted.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(post.transactional());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// The broker's dialect opens what its database keeps: Postgres `read_committed`,
/// `repeatable_read` and `serializable`, MySQL and MariaDB all four levels, SQLite the three
/// modes. A subscription to a table its dialect does not open does not compile, and the error
/// names the levels the dialect opens. An `AnyPool` reaches a database named only when the broker
/// connects, so there the subscription stops when it starts instead. On Postgres a row lock table
/// at `repeatable_read` or `serializable` fails claims with serialization errors when claims and
/// settlements of its rows run at once; `read_committed` is the practical level there. A lease
/// table at either level on Postgres refuses transactional mode when it starts: its transaction
/// reads every row as its first statement found it, and would not see the lease extended later.
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
/// # The headers layout
///
/// A `#[field(headers)]` field with `#[sqlx(flatten)]` holds a headers struct, which derives
/// [`InboxHeaders`](derive@InboxHeaders) and describes the queue table: its attributes, the
/// fields that play a role, and the service's own headers. The struct that flattens it is the
/// message, which a handler takes itself, `&Message` or `&[Message]`, as in row mode. It takes
/// `#[inbox(custom(..))]` alone, and no role beside its headers field. Its other fields are its
/// own data: the default fetch reads them from the queue table, by name, so a column the table
/// lacks stops the subscription at startup; with `custom(fetch)` the service's [`Fetch`] reads
/// them from wherever they live. The delivery's headers hold the headers struct's fields without a
/// role, built on the first read. The headers struct gets the generated insert, the message none.
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::PgPool;
///
/// // order_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, attempt SMALLINT DEFAULT 1,
/// // tenant TEXT, note TEXT
/// #[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
/// #[inbox(table = "order_jobs")]
/// pub struct OrderHeaders {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(attempt, generated)]
///     attempt: i16,
///     tenant: String,
/// }
///
/// #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
/// pub struct OrderJob {
///     #[field(headers)]
///     #[sqlx(flatten)]
///     headers: OrderHeaders,
///     note: Option<String>,
/// }
///
/// // The header `tenant` comes from the headers struct, the note from the message's own column.
/// #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
/// async fn ship(job: &OrderJob, ctx: &mut Context<'_>) -> HandlerOutcome {
///     let tenant = ctx.headers().get_str("tenant").unwrap_or_default();
///     tracing::info!(tenant, attempt = job.headers.attempt, note = ?job.note, "shipping");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(ship);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// A struct with a `payload` field is in payload mode: its handler takes the payload, decoded by a
/// codec. A struct without one is in row mode: its handler takes the struct itself, as the driver
/// read it. A struct in row mode derives `Clone`, because the test harness keeps a copy of each
/// value. It does not derive `Deserialize`: a type that deserializes rides the codec.
///
/// A generic struct keeps its parameters: the impl requires `Send + Sync + 'static` of the struct
/// and `Clone + Debug + Send + Sync + 'static` of the `id` field's type. In row mode a handler
/// takes it where its parameters make it `Clone`.
///
/// # Column names
///
/// A column is named in one place, sqlx's attributes. `#[sqlx(rename = "..")]` names a field's
/// column as written, `#[sqlx(rename_all = "..")]` recases every other field's name, a raw
/// identifier loses its `r#`, and a `#[sqlx(skip)]` field reads no column. Outside the headers
/// layout, a `#[sqlx(flatten)]` field reads columns the derive cannot see, so the statements
/// select `*`, and a dead-letter move copies the row by position: the dead-letter table has the
/// same columns in the same order.
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
/// `table` or `schema`, an unknown isolation level or mode, `isolation` beside `mode`. In the
/// headers layout: a flattened headers field whose type does not derive `InboxHeaders`, a role or
/// a table attribute on the message, and a custom event the headers struct's form does not take.
///
/// A struct in row mode without `Clone` does not compile. The error points at its name and
/// suggests the derive:
///
/// ```compile_fail,E0277
/// use ruststream_sqlx::Inbox;
///
/// #[derive(Inbox)]
/// #[inbox(table = "email_jobs")]
/// struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     to: String,
/// }
/// ```
///
/// A struct in row mode that derives `Deserialize` does not compile either. Such a type rides the
/// codec, and rustc reports conflicting implementations of
/// [`Input`](ruststream::runtime::Input):
///
/// ```compile_fail,E0119
/// use ruststream_sqlx::Inbox;
/// use serde::Deserialize;
///
/// #[derive(Inbox, Clone, Deserialize)]
/// #[inbox(table = "email_jobs")]
/// struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     to: String,
/// }
/// ```
///
/// A handler that does not fit its table's mode meets the core's errors at the mount site: a
/// handler of rows on a table in payload mode, a batch handler of rows with a reply or `Out`
/// slots. [Row mode](crate#row-mode) shows both.
#[cfg(feature = "inbox")]
pub use ruststream_sqlx_macros::Inbox;
/// Describes the queue table of a message assembled from it, and implements [`InboxHeaders`]
/// for the struct.
///
/// The struct takes the table's attributes and the fields that play a role, as a struct deriving
/// [`Inbox`](derive@Inbox) does; every field without a role is a header (see [`InboxHeaders`]).
/// `custom(..)`, `payload` and `headers` belong to the message struct, which flattens this one
/// into its `#[field(headers)]` field.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use sqlx::PgPool;
///
/// #[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
/// #[inbox(table = "order_jobs")]
/// pub struct OrderHeaders {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     tenant: String,
/// }
///
/// #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
/// pub struct OrderJob {
///     #[field(headers)]
///     #[sqlx(flatten)]
///     headers: OrderHeaders,
///     note: Option<String>,
/// }
///
/// #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
/// async fn ship(job: &OrderJob) -> HandlerOutcome {
///     tracing::info!(tenant = %job.headers.tenant, note = ?job.note, "shipping");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(ship);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[cfg(feature = "inbox")]
pub use ruststream_sqlx_macros::InboxHeaders;

/// Describes a service's outbox table with a struct and implements its record contract
/// ([`OutboxRow`]) and the default events.
///
/// The struct is an ordinary sqlx struct: `#[outbox(..)]` names the table, sqlx's own attributes
/// name the columns, and `#[field(..)]` marks the columns the outbox reads. The service writes the
/// record of a published message itself, in [`outbox::Publish`]; the derive writes the other
/// events, each for every [`OutboxDatabase`], with its statement built at compile time.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::{Outbox, outbox};
/// use sqlx::types::Json;
/// use sqlx::{PgConnection, Postgres};
/// use std::collections::BTreeMap;
///
/// // app.outbox: id BIGSERIAL PRIMARY KEY, name TEXT, payload BYTEA, headers JSONB,
/// // processed_at TIMESTAMPTZ
/// #[derive(Outbox, sqlx::FromRow)]
/// #[outbox(table = "outbox", schema = "app")]
/// pub struct OrderEvent {
///     #[field(id)]
///     id: i64,
///     #[field(name)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
///     #[field(headers)]
///     headers: Option<Json<BTreeMap<String, String>>>,
///     #[field(processed_at)]
///     processed_at: Option<chrono::DateTime<chrono::Utc>>,
/// }
///
/// impl outbox::Publish<Postgres> for OrderEvent {
///     async fn publish(
///         conn: &mut PgConnection,
///         msg: &OutgoingMessage<'_>,
///     ) -> Result<i64, sqlx::Error> {
///         let headers: BTreeMap<String, String> = msg
///             .headers()
///             .iter()
///             .map(|(name, value)| (name.to_owned(), String::from_utf8_lossy(value).into_owned()))
///             .collect();
///         sqlx::query_scalar(
///             "INSERT INTO app.outbox (name, payload, headers) VALUES ($1, $2, $3) RETURNING id",
///         )
///         .bind(msg.name())
///         .bind(msg.payload())
///         .bind(Json(headers))
///         .fetch_one(conn)
///         .await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
///
/// # The table
///
/// `#[outbox(table = "..")]` names the table and is required; `schema = ".."` places it in a
/// schema. Each names one thing and holds no dot.
///
/// # Roles
///
/// `#[field(..)]` gives a field one role:
///
/// - `id`, required: the record's identity, which a tracked message carries in
///   [`OUTBOX_ID_HEADER`](outbox::OUTBOX_ID_HEADER) through its `Display` and `FromStr`.
/// - `name`, required: the name the record was published under, read through `AsRef<str>`.
/// - `payload`, required: the published bytes, read through `AsRef<[u8]>`.
/// - `headers`: the published headers, a [`HeaderColumn`] type.
/// - `processed_at`: the mark of a processed record, written from the database's clock, so any
///   time type fits. Without it a processed record is deleted.
///
/// Every other field is a plain column sqlx reads. Column names come from sqlx's attributes:
/// `rename`, `rename_all`, a raw identifier without `r#`, and `skip` for a field without a column;
/// a `flatten` field makes the statements select `*`.
///
/// # Events
///
/// The default `Fetch` reads the record while it is unprocessed, `Ack` and `Discard` mark it
/// processed, `Retry` leaves it as it is and runs no statement, and `Recover` reads the
/// unprocessed records of one name. `#[outbox(custom(fetch, ack, retry, discard, recover))]`
/// lists the events the service implements itself instead; `publish` has no default and is never
/// listed.
///
/// # Compile errors
///
/// A struct the outbox cannot read does not compile, and the error points at the struct, the
/// field or the word that causes it: no `id`, `name` or `payload`, a role played twice, a column
/// named twice, a role on a field without a column, an unknown role or event, a dot in `table` or
/// `schema`.
#[cfg(feature = "outbox")]
pub use ruststream_sqlx_macros::Outbox;
/// Registers outbox records under the names they track, and returns the registry.
///
/// `outbox! { pool: pool, "orders" => OrderEvent, "refunds" => RefundEvent }` is
/// `Outbox::new(pool).register::<OrderEvent>("orders").register::<RefundEvent>("refunds")`.
/// Without `pool:` the registry starts as `Outbox::deferred()`, and the pool is set once it is
/// built. A name written twice does not compile, and the error points at its second literal.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use ruststream_sqlx::{Outbox, outbox};
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Outbox, sqlx::FromRow)]
/// # #[outbox(table = "outbox")]
/// # pub struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use std::io;
///
/// /// Inside `#[ruststream::app]`: no `pool:`, and the pool arrives in `on_startup`.
/// pub fn app() -> impl App {
///     let tracking = outbox! { "orders" => OrderOutbox };
///     let registry = tracking.clone();
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .on_startup(async move |()| {
///             let pool = PgPool::connect("postgres://localhost/orders")
///                 .await
///                 .map_err(io::Error::other)?;
///             registry.set_pool(pool.clone()).map_err(io::Error::other)?;
///             Ok::<_, io::Error>(pool)
///         })
///         .layer(tracking.layer())
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///             b.after_startup(Publish, tracking.republish());
///         })
/// }
///
/// /// An app built inside a running runtime takes the pool it has.
/// pub fn app_with(pool: PgPool) -> impl App {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[cfg(feature = "outbox")]
pub use ruststream_sqlx_macros::outbox;
