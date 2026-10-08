//! `InboxSpec`, the typed builder of a queue table's description, and `InboxTable`, the trait a
//! table described by hand implements.

use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;

use ruststream_sqlx_dialect::{Column, Form, KeyPart, Opening, Role, TableSpec};

use super::{
    Advisory, Attempt, AttemptFrom, Clock, Declaration, Fifo, HeaderFields, Headers, Key, Lease,
    OpeningLevel, Opens, OwnEvent, Payload, ProcessedAt, Push, RetryAfter, Valid,
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::Payload;
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // webhooks: id BIGSERIAL PRIMARY KEY, body BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct Webhook {
    ///     id: i64,
    ///     body: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Webhook {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload,)>;
    ///     // The database numbers the rows, so an insert leaves `id` out.
    ///     const TABLE: Self::Table = InboxSpec::new("webhooks", Column::new("id").generated())
    ///         .payload(Column::new("body"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for Webhook {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.body
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Event { kind: String }
    /// # #[subscriber(InboxQueue::<Webhook>::new("webhooks"))]
    /// # async fn receive(event: &Event) -> HandlerOutcome {
    /// #     tracing::info!(kind = %event.kind, "received");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("hooks", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(receive);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::dialect::insert::{self, Sql};
    /// use ruststream_sqlx::spec::Payload;
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// use sqlx::PgConnection;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct ReceiptJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for ReceiptJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload,)>;
    ///     const TABLE: Self::Table = InboxSpec::new("receipt_jobs", Column::new("id").generated())
    ///         .group(Column::new("name"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for ReceiptJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// // INSERT INTO "receipt_jobs" ("name", "payload") VALUES ($1, $2)
    /// const INSERT: Sql<128> = insert::postgres(&ReceiptJob::TABLE.spec());
    ///
    /// /// Queues a receipt in the order's transaction, so the two commit together.
    /// pub async fn enqueue(tx: &mut PgConnection, receipt: &[u8]) -> Result<(), sqlx::Error> {
    ///     sqlx::query(INSERT.as_str())
    ///         .bind("receipts")
    ///         .bind(receipt)
    ///         .execute(tx)
    ///         .await?;
    ///     Ok(())
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Receipt { order: i64 }
    /// # #[subscriber(InboxQueue::<ReceiptJob>::new("receipts"))]
    /// # async fn send(receipt: &Receipt) -> HandlerOutcome {
    /// #     tracing::info!(order = receipt.order, "sending a receipt");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("shop", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(send);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn spec(&self) -> TableSpec<'static> {
        self.spec
    }

    /// The same table, inside `schema`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::Payload;
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct InvoiceJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for InvoiceJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload,)>;
    ///     // The queue lives beside the tables it serves: `billing.invoice_jobs`.
    ///     const TABLE: Self::Table = InboxSpec::new("invoice_jobs", Column::new("id").generated())
    ///         .within("billing")
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for InvoiceJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Invoice { number: String }
    /// # #[subscriber(InboxQueue::<InvoiceJob>::new("invoices"))]
    /// # async fn issue(invoice: &Invoice) -> HandlerOutcome {
    /// #     tracing::info!(number = %invoice.number, "issuing");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("billing", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(issue);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn within(self, schema: &'static str) -> Self {
        Self {
            spec: self.spec.within(schema),
            ..self
        }
    }

    /// The same table, split into groups by `column`; a subscription reads one group.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::spec::Payload;
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct MediaJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for MediaJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload,)>;
    ///     // One table holds two queues: the `kind` column names the queue of each row.
    ///     const TABLE: Self::Table = InboxSpec::new("media_jobs", Column::new("id").generated())
    ///         .group(Column::new("kind"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for MediaJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Upload {
    ///     file: String,
    /// }
    ///
    /// #[subscriber(InboxQueue::<MediaJob>::new("thumbnails"))]
    /// async fn thumbnail(upload: &Upload) -> HandlerOutcome {
    ///     tracing::info!(file = %upload.file, "drawing a thumbnail");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[subscriber(InboxQueue::<MediaJob>::new("transcodes"))]
    /// async fn transcode(upload: &Upload) -> HandlerOutcome {
    ///     tracing::info!(file = %upload.file, "transcoding");
    ///     HandlerOutcome::ack()
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("media", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(thumbnail);
    /// #         b.include(transcode);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn group(self, column: Column<'static>) -> Self {
        Self {
            spec: self.spec.group(column),
            ..self
        }
    }

    /// The same table, with rows claimed in the order of `column`, a smaller value first.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::Payload;
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // tickets: id BIGSERIAL PRIMARY KEY, severity SMALLINT NOT NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct Ticket {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Ticket {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload,)>;
    ///     // An outage (severity 0) is answered before a question (severity 3).
    ///     const TABLE: Self::Table = InboxSpec::new("tickets", Column::new("id").generated())
    ///         .priority(Column::new("severity"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for Ticket {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Request { subject: String }
    /// # #[subscriber(InboxQueue::<Ticket>::new("support"))]
    /// # async fn triage(request: &Request) -> HandlerOutcome {
    /// #     tracing::info!(subject = %request.subject, "triaging");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("support", "1.0.0"))
    /// #         .with_broker(SqlxBroker::new(pool), |b| {
    /// #             b.include(triage);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn priority(self, column: Column<'static>) -> Self {
        Self {
            spec: self.spec.priority(column),
            ..self
        }
    }

    /// The same table, with the columns of the message's own data: the columns without a role.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream::runtime::{Input, SoloCarried};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use sqlx::PgPool;
    ///
    /// // shipments: id BIGSERIAL PRIMARY KEY, carrier TEXT NOT NULL, tracking TEXT NOT NULL
    /// #[derive(Debug, Clone, sqlx::FromRow)]
    /// pub struct Shipment {
    ///     id: i64,
    ///     carrier: String,
    ///     tracking: String,
    /// }
    ///
    /// impl InboxTable for Shipment {
    ///     type Id = i64;
    ///     type Table = InboxSpec;
    ///     // Row mode: the claim reads these columns, and the handler takes the row.
    ///     const TABLE: Self::Table = InboxSpec::new("shipments", Column::new("id").generated())
    ///         .data(&[Column::new("carrier"), Column::new("tracking")]);
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl Input for Shipment {
    ///     type Axis = SoloCarried<Self>;
    /// }
    ///
    /// #[subscriber(InboxQueue::<Shipment>::new("shipments"))]
    /// async fn track(shipment: &Shipment) -> HandlerOutcome {
    ///     tracing::info!(carrier = %shipment.carrier, tracking = %shipment.tracking, "tracking");
    ///     HandlerOutcome::ack()
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("logistics", "1.0.0"))
    /// #         .with_broker(SqlxBroker::new(pool), |b| {
    /// #             b.include(track);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn data(self, columns: &'static [Column<'static>]) -> Self {
        Self {
            spec: self.spec.data(columns),
            ..self
        }
    }

    /// The same table, with the columns a message assembled from header fields reads beside
    /// them.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream::HeaderMap;
    /// use ruststream::runtime::{Input, SoloCarried};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec;
    /// use ruststream_sqlx::{HeaderFields, InboxSpec, InboxTable, put_header};
    /// # use ruststream_sqlx::prelude::*;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(Debug, Clone, sqlx::FromRow)]
    /// pub struct RefundHeaders {
    ///     id: i64,
    ///     tenant: String,
    ///     order_id: i64,
    /// }
    ///
    /// #[derive(Debug, Clone, sqlx::FromRow)]
    /// pub struct Refund {
    ///     #[sqlx(flatten)]
    ///     headers: RefundHeaders,
    ///     reason: Option<String>,
    /// }
    ///
    /// impl InboxTable for Refund {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(spec::HeaderFields,)>;
    ///     // `tenant` and `order_id` become headers; the claim reads `reason` for the message.
    ///     const TABLE: Self::Table = InboxSpec::new("refunds", Column::new("id").generated())
    ///         .data(&[Column::new("tenant"), Column::new("order_id")])
    ///         .fetching(&[Column::new("reason")])
    ///         .header_fields();
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.headers.id
    ///     }
    /// }
    ///
    /// impl Input for Refund {
    ///     type Axis = SoloCarried<Self>;
    /// }
    ///
    /// impl HeaderFields for Refund {
    ///     const NAMES: &'static [&'static str] = &["tenant", "order_id"];
    ///
    ///     fn header_map(&self) -> HeaderMap {
    ///         let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
    ///         put_header(&mut headers, "tenant", &self.headers.tenant);
    ///         put_header(&mut headers, "order_id", &self.headers.order_id);
    ///         headers
    ///     }
    /// }
    /// # #[subscriber(InboxQueue::<Refund>::new("refunds"))]
    /// # async fn pay_back(job: &Refund) -> HandlerOutcome {
    /// #     tracing::info!(order = job.headers.order_id, reason = ?job.reason, "refunding");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("payments", "1.0.0"))
    /// #         .with_broker(SqlxBroker::new(pool), |b| {
    /// #             b.include(pay_back);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn fetching(self, columns: &'static [Column<'static>]) -> Self {
        Self {
            spec: self.spec.fetching(columns),
            ..self
        }
    }

    /// The same table, read with `*`: the row flattens another struct, so the columns are not
    /// all known.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream::runtime::{Input, SoloCarried};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use ruststream_sqlx::prelude::*;
    /// # use sqlx::PgPool;
    ///
    /// /// The address type the service shares with its other tables.
    /// #[derive(Debug, Clone, sqlx::FromRow)]
    /// pub struct Address {
    ///     street: String,
    ///     city: String,
    /// }
    ///
    /// #[derive(Debug, Clone, sqlx::FromRow)]
    /// pub struct Parcel {
    ///     id: i64,
    ///     #[sqlx(flatten)]
    ///     address: Address,
    /// }
    ///
    /// impl InboxTable for Parcel {
    ///     type Id = i64;
    ///     type Table = InboxSpec;
    ///     // The claim reads every column, whatever `Address` holds.
    ///     const TABLE: Self::Table =
    ///         InboxSpec::new("parcels", Column::new("id").generated()).selecting_all();
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl Input for Parcel {
    ///     type Axis = SoloCarried<Self>;
    /// }
    /// # #[subscriber(InboxQueue::<Parcel>::new("parcels"))]
    /// # async fn route(parcel: &Parcel) -> HandlerOutcome {
    /// #     tracing::info!(street = %parcel.address.street, city = %parcel.address.city, "routing");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("logistics", "1.0.0"))
    /// #         .with_broker(SqlxBroker::new(pool), |b| {
    /// #             b.include(route);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn selecting_all(self) -> Self {
        Self {
            spec: self.spec.selecting_all(),
            ..self
        }
    }

    /// The same table in the lease form: a claim sets `column`, the lease's expiry in `Time`, and
    /// commits. Adds [`Lease<Time>`](Lease).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Lease, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // report_jobs: id BIGSERIAL PRIMARY KEY, locked_until TIMESTAMPTZ NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct ReportJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for ReportJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    ///     // A render that takes minutes holds its row by `locked_until`, not by an open transaction.
    ///     const TABLE: Self::Table = InboxSpec::new("report_jobs", Column::new("id").generated())
    ///         .lease(Column::new("locked_until"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for ReportJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Report { month: String }
    /// # #[subscriber(InboxQueue::<ReportJob>::new("reports"))]
    /// # async fn render(report: &Report) -> HandlerOutcome {
    /// #     tracing::info!(month = %report.month, "rendering");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("reports", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(render);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::{Column, KeyPart};
    /// use ruststream_sqlx::spec::{Advisory, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // postings: id BIGSERIAL PRIMARY KEY, account TEXT NOT NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct Posting {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Posting {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Advisory, Payload)>;
    ///     // One account's postings run one at a time across every process: the key is `ledger-<account>`.
    ///     const TABLE: Self::Table = InboxSpec::new("postings", Column::new("id").generated())
    ///         .advisory(&[KeyPart::Literal("ledger-"), KeyPart::Column("account")])
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for Posting {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Entry { amount: i64 }
    /// # #[subscriber(InboxQueue::<Posting>::new("ledger"))]
    /// # async fn post(entry: &Entry) -> HandlerOutcome {
    /// #     tracing::info!(amount = entry.amount, "posting");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("ledger", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(post);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::Payload;
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{MySql, MySqlPool};
    ///
    /// // audit_events: id BIGINT AUTO_INCREMENT PRIMARY KEY, body TEXT NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct AuditEvent {
    ///     id: i64,
    ///     body: String,
    /// }
    ///
    /// impl InboxTable for AuditEvent {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload,)>;
    ///     // The message is the JSON text in `body`, which the subscription's codec decodes.
    ///     const TABLE: Self::Table = InboxSpec::new("audit_events", Column::new("id").generated())
    ///         .payload(Column::new("body"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for AuditEvent {
    ///     type Column = String;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         self.body.as_bytes()
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Login { user: String }
    /// # #[subscriber(InboxQueue::<AuditEvent>::new("logins"))]
    /// # async fn record(login: &Login) -> HandlerOutcome {
    /// #     tracing::info!(user = %login.user, "logged in");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: MySqlPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("audit", "1.0.0")).with_broker(
    /// #         SqlxBroker::<MySql>::new(pool),
    /// #         |b| {
    /// #             b.include(record);
    /// #         },
    /// #     )
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Key, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, KeyRow, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // export_jobs: id BIGSERIAL PRIMARY KEY, customer TEXT NOT NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct ExportJob {
    ///     id: i64,
    ///     customer: String,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for ExportJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Key, Payload)>;
    ///     // Each delivery carries its customer as the partition key, which the dispatch keeps in order.
    ///     const TABLE: Self::Table = InboxSpec::new("export_jobs", Column::new("id").generated())
    ///         .partition_key(Column::new("customer"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl KeyRow for ExportJob {
    ///     type Key = String;
    ///
    ///     fn partition_key(&self) -> &String {
    ///         &self.customer
    ///     }
    /// }
    ///
    /// impl PayloadRow for ExportJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Export { file: String }
    /// # #[subscriber(InboxQueue::<ExportJob>::new("exports"))]
    /// # async fn export(job: &Export) -> HandlerOutcome {
    /// #     tracing::info!(file = %job.file, "exporting");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("exports", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(export);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::spec::{Attempt, Payload};
    /// use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
    /// use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // downloads: id BIGSERIAL PRIMARY KEY, tries INT NOT NULL DEFAULT 1, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct Download {
    ///     id: i64,
    ///     tries: i32,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Download {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Attempt, Payload)>;
    ///     // A retry counts in `tries`, and the handler reads the count.
    ///     const TABLE: Self::Table = InboxSpec::new("downloads", Column::new("id").generated())
    ///         .attempt(Column::new("tries").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl AttemptRow for Download {
    ///     type Attempt = i32;
    ///
    ///     fn attempt(&self) -> &i32 {
    ///         &self.tries
    ///     }
    /// }
    ///
    /// impl PayloadRow for Download {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Image {
    ///     url: String,
    /// }
    ///
    /// # async fn fetch(_: &str) -> bool { true }
    /// // An image the fifth attempt cannot fetch is given up.
    /// #[subscriber(InboxQueue::<Download>::new("images"))]
    /// async fn download(image: &Image, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    ///     if fetch(&image.url).await {
    ///         HandlerOutcome::ack()
    ///     } else if attempt.unwrap_or(1) >= 5 {
    ///         HandlerOutcome::drop()
    ///     } else {
    ///         HandlerOutcome::retry()
    ///     }
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("images", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(download);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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

    /// The same table, with `column` counting the row's attempts, decoded as `Decoded` and
    /// converted into the row's attempt field: what `#[sqlx(try_from = "..")]` on the derive's
    /// `attempt` field spells. Adds [`AttemptFrom`].
    #[doc(hidden)]
    #[must_use]
    pub const fn attempt_from<Decoded>(
        self,
        column: Column<'static>,
    ) -> InboxSpec<<Settings as Push<AttemptFrom<Decoded>>>::Out>
    where
        Settings: Push<AttemptFrom<Decoded>>,
        <Settings as Push<AttemptFrom<Decoded>>>::Out: Declaration,
    {
        Self::with(&self.spec.attempt(column))
    }

    /// The same table, with the delivery's headers read from `column`. Adds [`Headers`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "json"))]
    /// # mod demo {
    /// use std::collections::BTreeMap;
    ///
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::spec::{Headers, Payload};
    /// use ruststream_sqlx::{HeaderRow, InboxSpec, InboxTable, PayloadRow};
    /// use serde::Deserialize;
    /// use sqlx::types::Json;
    /// # use sqlx::PgPool;
    ///
    /// // pushes: id BIGSERIAL PRIMARY KEY, meta JSONB NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct PushJob {
    ///     id: i64,
    ///     meta: Option<Json<BTreeMap<String, String>>>,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for PushJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Headers, Payload)>;
    ///     // The JSON object in `meta` becomes the delivery's headers.
    ///     const TABLE: Self::Table = InboxSpec::new("pushes", Column::new("id").generated())
    ///         .headers(Column::new("meta"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl HeaderRow for PushJob {
    ///     type Column = Option<Json<BTreeMap<String, String>>>;
    ///
    ///     fn headers_mut(&mut self) -> &mut Self::Column {
    ///         &mut self.meta
    ///     }
    /// }
    ///
    /// impl PayloadRow for PushJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Notification {
    ///     title: String,
    /// }
    ///
    /// #[subscriber(InboxQueue::<PushJob>::new("pushes"))]
    /// async fn push(note: &Notification, ctx: &mut Context<'_>) -> HandlerOutcome {
    ///     let device = ctx.headers().get_str("device").unwrap_or("all");
    ///     tracing::info!(title = %note.title, device, "pushing");
    ///     HandlerOutcome::ack()
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("push", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(push);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream::HeaderMap;
    /// use ruststream::runtime::{Input, SoloCarried};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::spec;
    /// use ruststream_sqlx::{HeaderFields, InboxSpec, InboxTable, put_header};
    /// # use sqlx::PgPool;
    ///
    /// // sms_jobs: id BIGSERIAL PRIMARY KEY, phone TEXT NOT NULL, locale TEXT NOT NULL, text TEXT NOT NULL
    /// #[derive(Debug, Clone, sqlx::FromRow)]
    /// pub struct Sms {
    ///     id: i64,
    ///     phone: String,
    ///     locale: String,
    ///     text: String,
    /// }
    ///
    /// impl InboxTable for Sms {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(spec::HeaderFields,)>;
    ///     // The `locale` column reads as a header, as it would on any other broker.
    ///     const TABLE: Self::Table = InboxSpec::new("sms_jobs", Column::new("id").generated())
    ///         .data(&[Column::new("phone"), Column::new("locale"), Column::new("text")])
    ///         .header_fields();
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl Input for Sms {
    ///     type Axis = SoloCarried<Self>;
    /// }
    ///
    /// impl HeaderFields for Sms {
    ///     const NAMES: &'static [&'static str] = &["locale"];
    ///
    ///     fn header_map(&self) -> HeaderMap {
    ///         let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
    ///         put_header(&mut headers, "locale", &self.locale);
    ///         headers
    ///     }
    /// }
    ///
    /// #[subscriber(InboxQueue::<Sms>::new("sms"))]
    /// async fn send(sms: &Sms, ctx: &mut Context<'_>) -> HandlerOutcome {
    ///     let locale = ctx.headers().get_str("locale").unwrap_or("en");
    ///     tracing::info!(phone = %sms.phone, locale, len = sms.text.len(), "sending");
    ///     HandlerOutcome::ack()
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("sms", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(send);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::spec::{Payload, RetryAfter};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // crm_pushes: id BIGSERIAL PRIMARY KEY, due TIMESTAMPTZ NOT NULL DEFAULT now(),
    /// // payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct CrmPush {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for CrmPush {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(RetryAfter<DateTime<Utc>>, Payload)>;
    ///     // A row is claimed once `due` has passed, so a delayed retry waits in the table.
    ///     const TABLE: Self::Table = InboxSpec::new("crm_pushes", Column::new("id").generated())
    ///         .retry_after(Column::new("due").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for CrmPush {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Contact {
    ///     email: String,
    /// }
    ///
    /// # async fn upsert(_: &str) -> Result<(), Duration> { Ok(()) }
    /// #[subscriber(InboxQueue::<CrmPush>::new("crm"))]
    /// async fn sync(contact: &Contact) -> HandlerOutcome {
    ///     match upsert(&contact.email).await {
    ///         Ok(()) => HandlerOutcome::ack(),
    ///         // The CRM's rate limit says how long to wait.
    ///         Err(wait) => HandlerOutcome::retry_after(wait),
    ///     }
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("crm", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(sync);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "time"))]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Payload, ProcessedAt};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// use time::OffsetDateTime;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // payments: id BIGSERIAL PRIMARY KEY, processed_at TIMESTAMPTZ NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct PaymentJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for PaymentJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(ProcessedAt<OffsetDateTime>, Payload)>;
    ///     // A settled payment stays in the table for the auditors, stamped in `processed_at`.
    ///     const TABLE: Self::Table = InboxSpec::new("payments", Column::new("id").generated())
    ///         .processed_at(Column::new("processed_at").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for PaymentJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Payment { cents: i64 }
    /// # #[subscriber(InboxQueue::<PaymentJob>::new("payments"))]
    /// # async fn settle(payment: &Payment) -> HandlerOutcome {
    /// #     tracing::info!(cents = payment.cents, "settling");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("payments", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(settle);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::prelude::*;
    /// use ruststream_sqlx::spec::{Fifo, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // stock_updates: id BIGSERIAL PRIMARY KEY, warehouse TEXT NOT NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct StockUpdate {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for StockUpdate {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Fifo, Payload)>;
    ///     // Each warehouse is a group, and its updates arrive one at a time, in the order written.
    ///     const TABLE: Self::Table = InboxSpec::new("stock_updates", Column::new("id").generated())
    ///         .fifo_group(Column::new("warehouse"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for StockUpdate {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Level {
    ///     sku: String,
    ///     count: i64,
    /// }
    ///
    /// #[subscriber(InboxQueue::<StockUpdate>::new("berlin"))]
    /// async fn berlin(level: &Level) -> HandlerOutcome {
    ///     tracing::info!(sku = %level.sku, level.count, "berlin stock");
    ///     HandlerOutcome::ack()
    /// }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("stock", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(berlin);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Clock, Payload, RetryAfter};
    /// use ruststream_sqlx::{DatabaseClock, InboxSpec, InboxTable, PayloadRow};
    /// # use std::time::Duration;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // reminders: id BIGSERIAL PRIMARY KEY, retry_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    /// // payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct Reminder {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Reminder {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Clock<DatabaseClock>, RetryAfter<DateTime<Utc>>, Payload)>;
    ///     // Due times count from the database's `now()`, so hosts whose clocks drift still agree.
    ///     const TABLE: Self::Table = InboxSpec::new("reminders", Column::new("id").generated())
    ///         .clock::<DatabaseClock>()
    ///         .retry_after(Column::new("retry_after").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for Reminder {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Note { text: String }
    /// # #[subscriber(InboxQueue::<Reminder>::new("reminders"))]
    /// # async fn remind(note: &Note) -> HandlerOutcome {
    /// #     tracing::info!(text = %note.text, "reminding");
    /// #     HandlerOutcome::retry_after(Duration::from_secs(3600))
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("reminders", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(remind);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::{Column, level};
    /// use ruststream_sqlx::spec::{Opens, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct PayoutJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for PayoutJob {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Opens<level::ReadCommitted>, Payload)>;
    ///     // The claim's transaction runs at READ COMMITTED on a database whose default is SERIALIZABLE.
    ///     const TABLE: Self::Table = InboxSpec::new("payout_jobs", Column::new("id").generated())
    ///         .opens::<level::ReadCommitted>()
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for PayoutJob {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Payout { cents: i64 }
    /// # #[subscriber(InboxQueue::<PayoutJob>::new("payouts"))]
    /// # async fn pay(payout: &Payout) -> HandlerOutcome {
    /// #     tracing::info!(cents = payout.cents, "paying out");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("payouts", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(pay);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Payload, own};
    /// use ruststream_sqlx::{Discard, InboxSpec, InboxTable, PayloadRow};
    /// use sqlx::{PgConnection, Postgres};
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// // signups: id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, payload BYTEA NOT NULL
    /// #[derive(sqlx::FromRow)]
    /// pub struct Signup {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Signup {
    ///     type Id = i64;
    ///     type Table = InboxSpec<(Payload, own::Discard)>;
    ///     const TABLE: Self::Table = InboxSpec::new("signups", Column::new("id").generated())
    ///         .group(Column::new("name"))
    ///         .payload(Column::new("payload"))
    ///         .own::<own::Discard>();
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for Signup {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    ///
    /// // A dropped signup moves to the `rejected` group, where support reviews it.
    /// impl Discard<Postgres> for Signup {
    ///     async fn discard(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
    ///         sqlx::query("UPDATE signups SET name = 'rejected' WHERE id = $1")
    ///             .bind(id)
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Form { email: String }
    /// # #[subscriber(InboxQueue::<Signup>::new("signups"))]
    /// # async fn check(form: &Form) -> HandlerOutcome {
    /// #     if form.email.contains('@') { HandlerOutcome::ack() } else { HandlerOutcome::drop() }
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("signup", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(check);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
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
