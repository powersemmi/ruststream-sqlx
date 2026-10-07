//! The inbox: task queues in tables a service describes with its own structs.

pub(crate) mod batch;
mod broker;
mod columns;
mod database;
mod delivery;
pub(crate) mod engine;
mod error;
mod events;
pub(crate) mod form;
pub mod keys;
pub(crate) mod named;
mod publish;
pub(crate) mod queue;
mod subscriber;
#[cfg(feature = "testing")]
mod testing;
mod time;
mod transactional;
mod tx;

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream_sqlx_dialect::TableSpec;

pub use batch::{RowBatch, RowDeliveries};
pub use broker::{ClosedSqlxBroker, ConnectedSqlxBroker, SqlxBroker};
pub use columns::{AttemptColumn, HeaderColumn, KeyColumn};
#[cfg(feature = "any")]
pub use database::AnyDialect;
pub use database::{BuiltIn, BuiltInDialect, InsertSql, OnConnection, QueueDatabase, no_insert};
pub use delivery::InboxDelivery;
pub use error::SqlxBrokerError;
pub use events::{
    Ack, Claim, DeadLetter, Discard, Extend, Fetch, Insert, Lock, Publish, Retry, RetryAfter,
    Unlock,
};
pub use form::{AdvisoryForm, FormDialect, FormOn, LeaseForm, RowLockForm};
pub use named::{ByName, NamedDelivery, NamedSubscriber, NamedTime};
pub use publish::{Repository, RepositoryPublisher, Routed, RoutedPublisher};
pub use queue::InboxQueue;
pub use subscriber::InboxSubscriber;
pub use time::{Clock, DatabaseClock, LeaseRow, QueueTime, SystemClock, TimeColumn, TimeSource};
pub use transactional::{InboxMode, InboxSettings, Plain, Transactional, TransactionalStep, Tx};

/// A struct that describes a queue table; `#[derive(Inbox)]` implements it.
///
/// The struct puts its table in one of two modes. In payload mode a `#[field(payload)]` field
/// holds the message, and a codec decodes it for the handler ([`PayloadRow`]). In row mode the
/// struct has no such field, and the handler takes the struct itself, `&Row` or `&[Row]` (see
/// [Row mode](crate#row-mode)).
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
/// // The derive implements `InboxRow` for `SendEmail`: the table `app.email_jobs`, its id and
/// // its payload column.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs", schema = "app")]
/// pub struct SendEmail {
///     #[field(id)]
///     job_id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
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
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe a queue table",
    label = "not an inbox row",
    note = "derive it: `#[derive(Inbox)]` with `#[inbox(table = \"..\")]` and a `#[field(id)]` field"
)]
pub trait InboxRow: QueueRow {
    /// The table the struct describes: its name, its columns and their roles, and the form its
    /// rows are claimed in.
    const SPEC: TableSpec<'static>;

    /// The form the table's rows are claimed in, as a type: a subscription requires its dialect
    /// to serve it ([`FormOn`]). Machinery; the derive sets it.
    #[doc(hidden)]
    type Form;

    /// What the table's transactions open at, as a type: a [`level`](crate::dialect::level)
    /// marker for the isolation level or SQLite mode the struct declares, `()` where it declares
    /// neither. A subscription requires its dialect to open it
    /// ([`Opens`](crate::dialect::Opens)). Machinery; the derive sets it.
    #[doc(hidden)]
    type Opening;
}

/// A row a subscription delivers, the type of its id, and how its message reaches a handler.
/// Machinery: `#[derive(Inbox)]` implements it beside [`InboxRow`], and the row of a by-name
/// subscription implements it without a table description of its own.
#[doc(hidden)]
pub trait QueueRow: Sized + Send + Sync + 'static {
    /// The type of the field that plays `id`; logs name a row by it, and a lease subscription
    /// keeps a copy of each id in work to extend its lease.
    type Id: Clone + Debug + Send + Sync + 'static;

    /// How a delivery hands the row's message to its handler: [`PayloadLane`] for a struct with a
    /// `#[field(payload)]` field, [`RowLane`] for one without.
    type Lane: Lane<Self>;
}

/// How a delivery hands a row's message to its handler. Machinery; the crate's two lanes are the
/// only ones.
#[doc(hidden)]
pub trait Lane<Row>: Send + Sync + 'static {
    /// Whether the handler takes the row itself.
    const ROWS: bool;

    /// What a delivery keeps of whether its handler borrowed the row: nothing in payload mode, a
    /// flag in row mode.
    type Lent: Default + Send + Sync;

    /// The bytes a delivery lends as its payload: the payload column in payload mode, none in row
    /// mode.
    fn payload(row: &Row) -> &[u8];

    /// Whether a payload read now comes from a delivery whose row its handler never borrowed, by
    /// what `lent` keeps: a handler that decodes a payload, mounted on a table in row mode. Never
    /// in payload mode.
    fn unlent(lent: &Self::Lent) -> bool;
}

/// Payload mode: the handler takes the payload decoded by a codec.
#[doc(hidden)]
#[derive(Debug)]
pub enum PayloadLane {}

/// Row mode: the handler takes the row itself, `&Row`, or a batch of them, `&[Row]`.
#[doc(hidden)]
#[derive(Debug)]
pub enum RowLane {}

impl<Row: PayloadRow> Lane<Row> for PayloadLane {
    const ROWS: bool = false;

    // Nothing to keep, so the field takes no room in a payload-mode delivery.
    type Lent = ();

    fn payload(row: &Row) -> &[u8] {
        row.payload()
    }

    fn unlent((): &()) -> bool {
        false
    }
}

// No `Clone` here: the carried lane asks it of the row where the derive writes `Input`, and one
// check gives one error.
impl<Row: QueueRow> Lane<Row> for RowLane {
    const ROWS: bool = true;

    type Lent = AtomicBool;

    fn payload(_row: &Row) -> &[u8] {
        &[]
    }

    fn unlent(lent: &AtomicBool) -> bool {
        // Relaxed: one task handles a delivery, and it stored the flag when its handler borrowed
        // the row.
        !lent.load(Ordering::Relaxed)
    }
}

/// A queue row that carries its message as bytes: payload mode.
///
/// The derive implements it for a struct with a `#[field(payload)]` field. A subscription hands
/// the handler the payload decoded by a codec or a `Deserialized` type, as on any broker, and the
/// bytes are lent from the row without a copy. Routes and by-name subscriptions read tables in
/// this mode.
///
/// A struct without a payload field is in row mode: its handler takes the row itself, and the
/// derive's [`insert`](crate::Insert::insert) or a [`Repository`] over the service's own
/// [`Publish`] writes its tasks (see [Row mode](crate#row-mode)).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::{PgConnection, PgPool, Postgres};
///
/// // `body` holds the message: `Job` is in payload mode, so a route can lead a name to it.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs")]
/// pub struct Job {
///     #[field(id, generated)]
///     id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     body: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for Job {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO jobs (name, body) VALUES ($1, $2)")
///             .bind(message.name())
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// // The codec decodes `Email` from the bytes lent from `body`.
/// #[subscriber("emails")]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
///         SqlxBroker::new(pool).route::<Job>("emails"),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no payload field, so a route has no column to write a message's bytes \
               into",
    label = "a table in row mode",
    note = "write a row of a row-mode table with the derive's `insert`, or with `Repository` over \
            a `Publish` of the service's own",
    note = "a route and a by-name subscription read a table whose `#[field(payload)]` field holds \
            the message"
)]
pub trait PayloadRow: QueueRow {
    /// The message bytes, lent from the row.
    fn payload(&self) -> &[u8];
}
