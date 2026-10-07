//! `InboxSpec`, the typed builder of a queue table's description, and `InboxTable`, the trait a
//! table described by hand implements.

use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;

use ruststream_sqlx_dialect::{Column, Form, KeyPart, Opening, Role, TableSpec};

use super::{
    Advisory, Attempt, Clock, Declaration, Fifo, HeaderFields, Headers, Key, Lease, OpeningLevel,
    Opens, OwnEvent, Payload, ProcessedAt, Push, RetryAfter, Valid,
};
use crate::TimeSource;

/// A queue table described by hand.
///
/// A struct implements it with the table's description in `TABLE`, built by [`InboxSpec`], and
/// the builder's final type in `type Table`. The typed setters of the chain decide the type, and
/// the compiler holds the chain to it, so the time types of the chain come from `type Table`.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "sqlite", feature = "chrono"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use std::time::Duration;
///
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::spec::{Attempt, Lease, Payload, ProcessedAt, RetryAfter};
/// use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
/// use serde::Deserialize;
/// use sqlx::{Sqlite, SqlitePool};
///
/// #[derive(sqlx::FromRow)]
/// pub struct EmailJob {
///     job_id: i64,
///     attempt: i16,
///     payload: Vec<u8>,
/// }
///
/// impl InboxTable for EmailJob {
///     type Id = i64;
///     type Table = InboxSpec<(
///         Lease<DateTime<Utc>>,
///         Payload,
///         Attempt,
///         RetryAfter<DateTime<Utc>>,
///         ProcessedAt<DateTime<Utc>>,
///     )>;
///     const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
///         .group(Column::new("name"))
///         .lease(Column::new("locked_until"))
///         .payload(Column::new("payload"))
///         .attempt(Column::new("attempt").generated())
///         .retry_after(Column::new("retry_after"))
///         .processed_at(Column::new("processed_at"));
///
///     fn id(&self) -> &i64 {
///         &self.job_id
///     }
/// }
///
/// impl PayloadRow for EmailJob {
///     type Column = Vec<u8>;
///
///     fn payload(&self) -> &[u8] {
///         &self.payload
///     }
/// }
///
/// impl AttemptRow for EmailJob {
///     type Attempt = i16;
///
///     fn attempt(&self) -> &i16 {
///         &self.attempt
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// // An address the mail server refuses comes back a minute later.
/// #[subscriber(InboxQueue::<EmailJob>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     if email.to.ends_with("@example.com") {
///         HandlerOutcome::ack()
///     } else {
///         HandlerOutcome::retry_after(Duration::from_secs(60))
///     }
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "1.0.0")).with_broker(
///         SqlxBroker::<Sqlite>::new(pool),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait InboxTable: Sized + Send + Sync + 'static {
    /// The type of the column that identifies a row.
    type Id: Clone + Debug + Send + Sync + 'static;

    /// The table's description as a type: `InboxSpec<(..)>`, listing the markers the chain of
    /// `TABLE` sets, in the order it sets them.
    type Table: Valid;

    /// The table's description.
    const TABLE: Self::Table;

    /// The row's id.
    fn id(&self) -> &Self::Id;
}

/// The description of a queue table, with its settings as types.
///
/// `new` starts a table in the row lock form, in row mode. The column-only setters (`within`,
/// `group`, `priority`, `data`, `fetching`, `selecting_all`) keep the type. Each typed setter
/// adds its marker of [`spec`](crate::spec) to `Settings`, so a table's `type Table` lists the
/// markers its chain sets, in that order. A setting set twice does not compile. Every setter is a
/// `const fn`, and [`spec`](Self::spec) hands back the dialect's [`TableSpec`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "sqlite", feature = "chrono"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::dialect::{Column, KeyPart, level};
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::spec::{Advisory, Clock, Opens, Payload, RetryAfter};
/// use ruststream_sqlx::{DatabaseClock, InboxSpec, InboxTable, PayloadRow};
/// use serde::Deserialize;
/// use sqlx::{Sqlite, SqlitePool};
///
/// #[derive(sqlx::FromRow)]
/// pub struct SyncJob {
///     id: i64,
///     payload: Vec<u8>,
/// }
///
/// impl InboxTable for SyncJob {
///     type Id = i64;
///     type Table = InboxSpec<(
///         Advisory,
///         Clock<DatabaseClock>,
///         Opens<level::Immediate>,
///         RetryAfter<DateTime<Utc>>,
///         Payload,
///     )>;
///     const TABLE: Self::Table = InboxSpec::new("sync_jobs", Column::new("id").generated())
///         .advisory(&[KeyPart::Literal("sync-"), KeyPart::Column("tenant")])
///         .clock::<DatabaseClock>()
///         .opens::<level::Immediate>()
///         .retry_after(Column::new("retry_after"))
///         .payload(Column::new("payload"));
///
///     fn id(&self) -> &i64 {
///         &self.id
///     }
/// }
///
/// impl PayloadRow for SyncJob {
///     type Column = Vec<u8>;
///
///     fn payload(&self) -> &[u8] {
///         &self.payload
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Sync {
///     tenant: String,
/// }
///
/// // One tenant syncs at a time: the lock key holds the tenant.
/// #[subscriber(InboxQueue::<SyncJob>::new("sync"))]
/// async fn sync(job: &Sync) -> HandlerOutcome {
///     tracing::info!(tenant = %job.tenant, "syncing");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("sync", "1.0.0")).with_broker(
///         SqlxBroker::<Sqlite>::new(pool),
///         |b| {
///             b.include(sync);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub struct InboxSpec<Settings = ()> {
    spec: TableSpec<'static>,
    settings: PhantomData<fn() -> Settings>,
}

// By hand: a derive would require each marker of `Settings` to implement the trait, and the
// markers are never values.
impl<Settings> Debug for InboxSpec<Settings> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboxSpec")
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

impl<Settings> Clone for InboxSpec<Settings> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Settings> Copy for InboxSpec<Settings> {}

impl<Settings> PartialEq for InboxSpec<Settings> {
    fn eq(&self, other: &Self) -> bool {
        self.spec == other.spec
    }
}

impl<Settings> Eq for InboxSpec<Settings> {}

impl InboxSpec {
    /// A table in the connection's default schema, with the column that identifies a row: in the
    /// row lock form, in row mode, with no other setting.
    #[must_use]
    pub const fn new(table: &'static str, id: Column<'static>) -> Self {
        Self {
            spec: TableSpec::new(table, id, Form::RowLock),
            settings: PhantomData,
        }
    }
}

impl<Settings> InboxSpec<Settings> {
    /// The table's description, which every statement is built from.
    #[must_use]
    pub const fn spec(&self) -> TableSpec<'static> {
        self.spec
    }

    /// The same table, inside `schema`.
    #[must_use]
    pub const fn within(self, schema: &'static str) -> Self {
        Self {
            spec: self.spec.within(schema),
            ..self
        }
    }

    /// The same table, split into groups by `column`; a subscription reads one group.
    #[must_use]
    pub const fn group(self, column: Column<'static>) -> Self {
        Self {
            spec: self.spec.group(column),
            ..self
        }
    }

    /// The same table, with rows claimed in the order of `column`, a smaller value first.
    #[must_use]
    pub const fn priority(self, column: Column<'static>) -> Self {
        Self {
            spec: self.spec.priority(column),
            ..self
        }
    }

    /// The same table, with the columns of the message's own data: the columns without a role.
    #[must_use]
    pub const fn data(self, columns: &'static [Column<'static>]) -> Self {
        Self {
            spec: self.spec.data(columns),
            ..self
        }
    }

    /// The same table, with the columns a message assembled from header fields reads beside
    /// them.
    #[must_use]
    pub const fn fetching(self, columns: &'static [Column<'static>]) -> Self {
        Self {
            spec: self.spec.fetching(columns),
            ..self
        }
    }

    /// The same table, read with `*`: the row flattens another struct, so the columns are not
    /// all known.
    #[must_use]
    pub const fn selecting_all(self) -> Self {
        Self {
            spec: self.spec.selecting_all(),
            ..self
        }
    }

    /// The same table in the lease form: a claim sets `column`, the lease's expiry in `Time`, and
    /// commits. Adds [`Lease<Time>`](Lease).
    #[must_use]
    pub const fn lease<Time>(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<Lease<Time>>>::Out>
    where
        Settings: Push<Lease<Time>>,
        <Settings as Push<Lease<Time>>>::Out: Declaration,
    {
        Self::with(&self.reformed(Form::Lease(column)))
    }

    /// The same table in the advisory lock form: a session lock on the key these parts build from
    /// the row holds the row. Adds [`Advisory`].
    #[must_use]
    pub const fn advisory(
        self,
        key: &'static [KeyPart<'static>],
    ) -> InboxSpec<<Settings as Push<Advisory>>::Out>
    where
        Settings: Push<Advisory>,
        <Settings as Push<Advisory>>::Out: Declaration,
    {
        Self::with(&self.reformed(Form::Advisory(key)))
    }

    /// The same table in payload mode, the message's bytes read from `column`. Adds [`Payload`].
    #[must_use]
    pub const fn payload(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<Payload>>::Out>
    where
        Settings: Push<Payload>,
        <Settings as Push<Payload>>::Out: Declaration,
    {
        Self::with(&self.spec.payload(column))
    }

    /// The same table, with the delivery's partition key read from `column`. Adds [`Key`].
    #[must_use]
    pub const fn partition_key(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<Key>>::Out>
    where
        Settings: Push<Key>,
        <Settings as Push<Key>>::Out: Declaration,
    {
        Self::with(&self.spec.partition_key(column))
    }

    /// The same table, counting a row's attempts in `column`. Adds [`Attempt`].
    #[must_use]
    pub const fn attempt(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<Attempt>>::Out>
    where
        Settings: Push<Attempt>,
        <Settings as Push<Attempt>>::Out: Declaration,
    {
        Self::with(&self.spec.attempt(column))
    }

    /// The same table, with the delivery's headers read from `column`. Adds [`Headers`].
    #[must_use]
    pub const fn headers(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<Headers>>::Out>
    where
        Settings: Push<Headers>,
        <Settings as Push<Headers>>::Out: Declaration,
    {
        Self::with(&self.spec.headers(column))
    }

    /// The same table, with the delivery's headers built from the row's header fields, whose
    /// columns [`data`](Self::data) and [`fetching`](Self::fetching) name. Adds
    /// [`HeaderFields`].
    #[must_use]
    pub const fn header_fields(self) -> InboxSpec<<Settings as Push<HeaderFields>>::Out>
    where
        Settings: Push<HeaderFields>,
        <Settings as Push<HeaderFields>>::Out: Declaration,
    {
        Self::with(&self.spec)
    }

    /// The same table, with the time a retried row is due in `column`, in `Time`. Adds
    /// [`RetryAfter<Time>`](RetryAfter).
    #[must_use]
    pub const fn retry_after<Time>(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<RetryAfter<Time>>>::Out>
    where
        Settings: Push<RetryAfter<Time>>,
        <Settings as Push<RetryAfter<Time>>>::Out: Declaration,
    {
        Self::with(&self.spec.retry_after(column))
    }

    /// The same table, with the time a row was processed written into `column`, in `Time`, so a
    /// processed row stays in the table. Adds [`ProcessedAt<Time>`](ProcessedAt).
    #[must_use]
    pub const fn processed_at<Time>(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<ProcessedAt<Time>>>::Out>
    where
        Settings: Push<ProcessedAt<Time>>,
        <Settings as Push<ProcessedAt<Time>>>::Out: Declaration,
    {
        Self::with(&self.spec.processed_at(column))
    }

    /// The same table, split into groups by `column`, with each group in order: at most one row
    /// of a group is in work, taken in claim order. Adds [`Fifo`].
    #[must_use]
    pub const fn fifo_group(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<Fifo>>::Out>
    where
        Settings: Push<Fifo>,
        <Settings as Push<Fifo>>::Out: Declaration,
    {
        Self::with(&self.spec.fifo_group(column))
    }

    /// The same table, reading now from `Source`: the statements read the database's own clock
    /// where `Source` is [`DatabaseClock`](crate::DatabaseClock). Adds
    /// [`Clock<Source>`](Clock).
    #[must_use]
    pub const fn clock<Source: TimeSource>(
        self,
    ) -> InboxSpec<<Settings as Push<Clock<Source>>>::Out>
    where
        Settings: Push<Clock<Source>>,
        <Settings as Push<Clock<Source>>>::Out: Declaration,
    {
        if Source::DATABASE {
            Self::with(&self.spec.database_clock())
        } else {
            Self::with(&self.spec)
        }
    }

    /// The same table, with its transactions opened at `Level`, an isolation level or a SQLite
    /// mode. Adds [`Opens<Level>`](Opens).
    #[must_use]
    pub const fn opens<Level: OpeningLevel>(
        self,
    ) -> InboxSpec<<Settings as Push<Opens<Level>>>::Out>
    where
        Settings: Push<Opens<Level>>,
        <Settings as Push<Opens<Level>>>::Out: Declaration,
    {
        Self::with(&opened(&self.spec, Level::OPENING))
    }

    /// The same table, with `Event` written by the service itself: the row implements the
    /// event's trait. Adds `Event`, a marker of [`own`](super::own).
    #[must_use]
    pub const fn own<Event: OwnEvent>(self) -> InboxSpec<<Settings as Push<Event>>::Out>
    where
        Settings: Push<Event>,
        <Settings as Push<Event>>::Out: Declaration,
    {
        Self::with(&self.spec)
    }

    /// The same columns as `spec`, under the settings `Next`.
    const fn with<Next>(spec: &TableSpec<'static>) -> InboxSpec<Next> {
        InboxSpec {
            spec: *spec,
            settings: PhantomData,
        }
    }

    /// The same table in `form`: `TableSpec` takes its form when it is built, so the description
    /// is built again with every column set so far.
    const fn reformed(self, form: Form<'static>) -> TableSpec<'static> {
        let old = self.spec;
        let mut spec = TableSpec::new(old.table(), old.id(), form);
        if let Some(schema) = old.schema() {
            spec = spec.within(schema);
        }
        if let Some(column) = old.column(Role::Group) {
            spec = if old.is_fifo() {
                spec.fifo_group(column)
            } else {
                spec.group(column)
            };
        }
        if let Some(column) = old.column(Role::PartitionKey) {
            spec = spec.partition_key(column);
        }
        if let Some(column) = old.column(Role::Priority) {
            spec = spec.priority(column);
        }
        if let Some(column) = old.column(Role::RetryAfter) {
            spec = spec.retry_after(column);
        }
        if let Some(column) = old.column(Role::Attempt) {
            spec = spec.attempt(column);
        }
        if let Some(column) = old.column(Role::ProcessedAt) {
            spec = spec.processed_at(column);
        }
        if let Some(column) = old.column(Role::Headers) {
            spec = spec.headers(column);
        }
        if let Some(column) = old.column(Role::Payload) {
            spec = spec.payload(column);
        }
        if !old.data_columns().is_empty() {
            spec = spec.data(old.data_columns());
        }
        if !old.fetched_columns().is_empty() {
            spec = spec.fetching(old.fetched_columns());
        }
        if old.selects_all() {
            spec = spec.selecting_all();
        }
        if old.uses_database_clock() {
            spec = spec.database_clock();
        }
        opened(&spec, old.opening())
    }
}

/// `spec`, opening its transactions at `opening`.
const fn opened(spec: &TableSpec<'static>, opening: Opening) -> TableSpec<'static> {
    let spec = *spec;
    match opening {
        Opening::Isolation(isolation) => spec.isolation(isolation),
        Opening::Mode(mode) => spec.mode(mode),
        _ => spec,
    }
}
