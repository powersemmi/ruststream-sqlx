//! The inbox: task queues in tables a service describes with its own structs.

mod broker;
mod columns;
mod database;
mod delivery;
pub(crate) mod engine;
mod error;
mod events;
pub mod keys;
mod publish;
mod queue;
mod subscriber;
#[cfg(feature = "testing")]
mod testing;
mod time;

use std::fmt::Debug;

use ruststream_sqlx_dialect::TableSpec;

pub use broker::{ClosedSqlxBroker, ConnectedSqlxBroker, SqlxBroker};
pub use columns::{AttemptColumn, HeaderColumn, KeyColumn};
#[cfg(feature = "postgres")]
pub use database::OnPostgres;
pub use database::{BuiltInDialect, QueueDatabase};
pub use delivery::InboxDelivery;
pub use error::SqlxBrokerError;
pub use events::{Ack, Claim, DeadLetter, Discard, Fetch, Insert, Publish, Retry, RetryAfter};
pub use publish::{Repository, RepositoryPublisher, Routed, RoutedPublisher};
pub use queue::InboxQueue;
pub use subscriber::InboxSubscriber;
pub use time::{Clock, DatabaseClock, QueueTime, SystemClock, TimeColumn, TimeSource};

/// A struct that describes a queue table; `#[derive(Inbox)]` implements it.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::{Inbox, InboxRow};
///
/// #[derive(Inbox)]
/// #[inbox(table = "email_jobs", schema = "app")]
/// struct SendEmail {
///     #[field(id)]
///     job_id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// /// Where a queue's rows live, for the line a service logs when it starts.
/// fn location<Row: InboxRow>() -> String {
///     let spec = Row::SPEC;
///     match spec.schema() {
///         Some(schema) => format!("{schema}.{}", spec.table()),
///         None => spec.table().to_owned(),
///     }
/// }
///
/// assert_eq!(location::<SendEmail>(), "app.email_jobs");
/// # let _ = |row: SendEmail| (row.job_id, row.payload);
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe a queue table",
    label = "not an inbox row",
    note = "derive it: `#[derive(Inbox)]` with `#[inbox(table = \"..\")]` and a `#[field(id)]` field"
)]
pub trait InboxRow: Sized + Send + Sync + 'static {
    /// The table the struct describes: its name, its columns and their roles, and the form its
    /// rows are claimed in.
    const SPEC: TableSpec<'static>;

    /// The type of the field that plays `id`; logs name a row by it.
    type Id: Debug + Send + Sync + 'static;
}

/// A queue row that carries its message as bytes: payload mode.
///
/// The derive implements it for a struct with a `#[field(payload)]` field. A subscription hands
/// the handler the payload decoded by a codec or a `Deserialized` type, as on any broker, and the
/// bytes are lent from the row without a copy.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::{Inbox, PayloadRow};
///
/// #[derive(Inbox)]
/// #[inbox(table = "jobs")]
/// struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     body: Vec<u8>,
/// }
///
/// let job = Job { id: 1, body: br#"{"to":"a@b"}"#.to_vec() };
/// assert_eq!(job.payload(), br#"{"to":"a@b"}"#);
/// # let _ = job.id;
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no payload field, so a subscription has no message to hand a handler",
    label = "no `#[field(payload)]` field",
    note = "mark the column that holds the message bytes with `#[field(payload)]`"
)]
pub trait PayloadRow: InboxRow {
    /// The message bytes, lent from the row.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx::PayloadRow;
    ///
    /// // What a delivery hands the codec.
    /// fn size<Row: PayloadRow>(row: &Row) -> usize {
    ///     row.payload().len()
    /// }
    /// # #[derive(ruststream_sqlx::Inbox)]
    /// # #[inbox(table = "t")]
    /// # struct Job { #[field(id)] id: i64, #[field(payload)] body: Vec<u8> }
    /// # assert_eq!(size(&Job { id: 1, body: vec![1, 2] }), 2);
    /// ```
    fn payload(&self) -> &[u8];
}
