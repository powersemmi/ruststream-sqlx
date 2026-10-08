//! The settings of a queue table described by hand, as types.
//!
//! Each typed setter of [`InboxSpec`](crate::InboxSpec) adds one marker of this module to the
//! table's settings, and the table's `type Table` lists them in the order the chain sets them. A
//! setting the chain leaves out keeps its default: the row lock form, row mode, the
//! [`SystemClock`](crate::SystemClock), the database's default opening and the crate's events. A
//! setting set twice does not compile ([`Merge`]), and neither does a combination of settings no
//! table can hold.

pub(super) mod builder;
mod declaration;
mod rules;

use std::marker::PhantomData;

use ruststream_sqlx_dialect::{Isolation, Mode, Opening, level};

use declaration::setting;
pub use declaration::{Declaration, Merge, Push, Set, Unset};
#[doc(hidden)]
pub use rules::{
    ClaimOutsideAdvisory, ClockSlot, ExtendInLease, FifoOutsideAdvisory, LeaseOnServiceClock,
    LockInAdvisory, LockWithUnlock, PayloadOutsideHeaderFields, Valid,
};

setting!(
    /// The lease form, its expiry in `Time`: set by [`InboxSpec::lease`](crate::InboxSpec::lease).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "sqlite", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Lease, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{Sqlite, SqlitePool};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct ScanJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for ScanJob {
    ///     type Id = i64;
    ///     // A virus scan holds its row by a lease, kept in `locked_until` as a UTC timestamp.
    ///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("scan_jobs", Column::new("id").generated())
    ///         .lease(Column::new("locked_until"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for ScanJob {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Upload { path: String }
    /// # #[subscriber(InboxQueue::<ScanJob>::new("scans"))]
    /// # async fn scan(upload: &Upload) -> HandlerOutcome {
    /// #     tracing::info!(path = %upload.path, "scanning");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: SqlitePool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("scanner", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<Sqlite>::new(pool), |b| {
    /// #             b.include(scan);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Lease<Time>, Form
);
setting!(
    /// The advisory lock form: set by [`InboxSpec::advisory`](crate::InboxSpec::advisory).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::{Column, KeyPart};
    /// use ruststream_sqlx::spec::{Advisory, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{MySql, MySqlPool};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct FirmwareUpdate {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for FirmwareUpdate {
    ///     type Id = i64;
    ///     // A device takes one update at a time: the lock key is `firmware-<device>`.
    ///     type Table = InboxSpec<(Advisory, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("firmware_updates", Column::new("id").generated())
    ///         .advisory(&[KeyPart::Literal("firmware-"), KeyPart::Column("device")])
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for FirmwareUpdate {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Image { version: String }
    /// # #[subscriber(InboxQueue::<FirmwareUpdate>::new("firmware"))]
    /// # async fn flash(image: &Image) -> HandlerOutcome {
    /// #     tracing::info!(version = %image.version, "flashing");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: MySqlPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("devices", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<MySql>::new(pool), |b| {
    /// #             b.include(flash);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Advisory, Form
);
setting!(
    /// Payload mode: set by [`InboxSpec::payload`](crate::InboxSpec::payload).
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
    /// pub struct OrderEvent {
    ///     id: i64,
    ///     body: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for OrderEvent {
    ///     type Id = i64;
    ///     // The handler takes the message decoded from `body`, not the row.
    ///     type Table = InboxSpec<(Payload,)>;
    ///     const TABLE: Self::Table = InboxSpec::new("order_events", Column::new("id").generated())
    ///         .payload(Column::new("body"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl PayloadRow for OrderEvent {
    ///     type Column = Vec<u8>;
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.body
    ///     }
    /// }
    /// # #[derive(Deserialize)]
    /// # struct Placed { order: i64 }
    /// # #[subscriber(InboxQueue::<OrderEvent>::new("orders"))]
    /// # async fn placed(event: &Placed) -> HandlerOutcome {
    /// #     tracing::info!(order = event.order, "placed");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(placed);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Payload, Message
);
setting!(
    /// The partition key: set by [`InboxSpec::partition_key`](crate::InboxSpec::partition_key).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Key, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable, KeyRow};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct MeterReading {
    ///     id: i64,
    ///     meter: Option<String>,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for MeterReading {
    ///     type Id = i64;
    ///     // One meter's readings keep their order; a reading without a meter has no key.
    ///     type Table = InboxSpec<(Key, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("meter_readings", Column::new("id").generated())
    ///         .partition_key(Column::new("meter"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl KeyRow for MeterReading {
    ///     type Key = Option<String>;
    ///
    ///     fn partition_key(&self) -> &Option<String> {
    ///         &self.meter
    ///     }
    /// }
    /// # impl PayloadRow for MeterReading {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Reading { kwh: f64 }
    /// # #[subscriber(InboxQueue::<MeterReading>::new("readings"))]
    /// # async fn bill(reading: &Reading) -> HandlerOutcome {
    /// #     tracing::info!(kwh = reading.kwh, "billing");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("metering", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(bill);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Key, Key
);
setting!(
    /// The attempt count: set by [`InboxSpec::attempt`](crate::InboxSpec::attempt).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Attempt, Payload};
    /// use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{MySql, MySqlPool};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct CallbackJob {
    ///     id: i64,
    ///     attempt: i16,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for CallbackJob {
    ///     type Id = i64;
    ///     // `attempt` counts the tries, and the delivery reports it.
    ///     type Table = InboxSpec<(Attempt, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("callbacks", Column::new("id").generated())
    ///         .attempt(Column::new("attempt").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl AttemptRow for CallbackJob {
    ///     type Attempt = i16;
    ///
    ///     fn attempt(&self) -> &i16 {
    ///         &self.attempt
    ///     }
    /// }
    /// # impl PayloadRow for CallbackJob {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Callback { url: String }
    /// # #[subscriber(InboxQueue::<CallbackJob>::new("callbacks"))]
    /// # async fn call(callback: &Callback, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
    /// #     tracing::info!(url = %callback.url, ?attempt, "calling back");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: MySqlPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("callbacks", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<MySql>::new(pool), |b| {
    /// #             b.include(call);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Attempt, Attempt
);
setting!(
    /// The attempt count of a field sqlx converts from the column's `Decoded`
    /// (`#[sqlx(try_from = "..")]`): a row that does not decode reads its attempt as `Decoded`.
    /// Set by `#[derive(Inbox)]`.
    #[doc(hidden)]
    AttemptFrom<Decoded>, Attempt
);
setting!(
    /// The delivery's headers, read from one column: set by
    /// [`InboxSpec::headers`](crate::InboxSpec::headers).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "sqlite", feature = "chrono", feature = "json"))]
    /// # mod demo {
    /// use std::collections::BTreeMap;
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Headers, Lease, Payload};
    /// use ruststream_sqlx::{HeaderRow, InboxSpec, InboxTable};
    /// use sqlx::types::Json;
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{Sqlite, SqlitePool};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct Relay {
    ///     id: i64,
    ///     headers: Json<BTreeMap<String, String>>,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Relay {
    ///     type Id = i64;
    ///     // The headers a relayed message carried come back from the `headers` JSON column.
    ///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Headers, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("relay", Column::new("id").generated())
    ///         .lease(Column::new("locked_until"))
    ///         .headers(Column::new("headers"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl HeaderRow for Relay {
    ///     type Column = Json<BTreeMap<String, String>>;
    ///
    ///     fn headers_mut(&mut self) -> &mut Self::Column {
    ///         &mut self.headers
    ///     }
    /// }
    /// # impl PayloadRow for Relay {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Forwarded { id: String }
    /// # #[subscriber(InboxQueue::<Relay>::new("relay"))]
    /// # async fn forward(message: &Forwarded, ctx: &mut Context<'_>) -> HandlerOutcome {
    /// #     let source = ctx.headers().get_str("source").unwrap_or("unknown");
    /// #     tracing::info!(id = %message.id, source, "forwarding");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: SqlitePool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("relay", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<Sqlite>::new(pool), |b| {
    /// #             b.include(forward);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Headers, Headers
);
setting!(
    /// The delivery's headers, built from the row's header fields: set by
    /// [`InboxSpec::header_fields`](crate::InboxSpec::header_fields).
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
    /// pub struct Signal {
    ///     id: i64,
    ///     trace: Option<String>,
    ///     sensor: String,
    ///     value: f64,
    /// }
    ///
    /// impl InboxTable for Signal {
    ///     type Id = i64;
    ///     // `trace` reads as a header; a row without one has no `trace` header.
    ///     type Table = InboxSpec<(spec::HeaderFields,)>;
    ///     const TABLE: Self::Table = InboxSpec::new("signals", Column::new("id").generated())
    ///         .data(&[Column::new("trace"), Column::new("sensor"), Column::new("value")])
    ///         .header_fields();
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    ///
    /// impl HeaderFields for Signal {
    ///     const NAMES: &'static [&'static str] = &["trace"];
    ///
    ///     fn header_map(&self) -> HeaderMap {
    ///         let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
    ///         put_header(&mut headers, "trace", &self.trace);
    ///         headers
    ///     }
    /// }
    /// # impl Input for Signal {
    /// #     type Axis = SoloCarried<Self>;
    /// # }
    /// # #[subscriber(InboxQueue::<Signal>::new("signals"))]
    /// # async fn record(signal: &Signal) -> HandlerOutcome {
    /// #     tracing::info!(sensor = %signal.sensor, value = signal.value, "recorded");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("sensors", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(record);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    HeaderFields, Headers
);
setting!(
    /// The time a retried row is due, in `Time`: set by
    /// [`InboxSpec::retry_after`](crate::InboxSpec::retry_after).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "mysql", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Payload, RetryAfter};
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use std::time::Duration;
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{MySql, MySqlPool};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct Delivery {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Delivery {
    ///     type Id = i64;
    ///     // A retried webhook waits until `retry_after`, a `DATETIME(6)` read as UTC.
    ///     type Table = InboxSpec<(RetryAfter<DateTime<Utc>>, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("webhook_deliveries", Column::new("id").generated())
    ///         .retry_after(Column::new("retry_after").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for Delivery {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Hook { url: String }
    /// # #[subscriber(InboxQueue::<Delivery>::new("webhooks"))]
    /// # async fn deliver(hook: &Hook) -> HandlerOutcome {
    /// #     tracing::info!(url = %hook.url, "delivering");
    /// #     HandlerOutcome::retry_after(Duration::from_secs(300))
    /// # }
    /// # pub fn app(pool: MySqlPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("webhooks", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<MySql>::new(pool), |b| {
    /// #             b.include(deliver);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    RetryAfter<Time>, RetryAfter
);
setting!(
    /// The time a row was processed, in `Time`: set by
    /// [`InboxSpec::processed_at`](crate::InboxSpec::processed_at).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Payload, ProcessedAt};
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct Import {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Import {
    ///     type Id = i64;
    ///     // A finished import keeps its row, with the time it finished in `done_at`.
    ///     type Table = InboxSpec<(ProcessedAt<DateTime<Utc>>, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("imports", Column::new("id").generated())
    ///         .processed_at(Column::new("done_at").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for Import {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Csv { file: String }
    /// # #[subscriber(InboxQueue::<Import>::new("imports"))]
    /// # async fn import(csv: &Csv) -> HandlerOutcome {
    /// #     tracing::info!(file = %csv.file, "importing");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("imports", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(import);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    ProcessedAt<Time>, ProcessedAt
);
setting!(
    /// Groups kept in order: set by [`InboxSpec::fifo_group`](crate::InboxSpec::fifo_group).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{Fifo, Lease, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::PgPool;
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct Migration {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Migration {
    ///     type Id = i64;
    ///     // A tenant's schema migrations run one at a time, in the order they were queued.
    ///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Fifo, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("migrations", Column::new("id").generated())
    ///         .lease(Column::new("locked_until"))
    ///         .fifo_group(Column::new("tenant"))
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for Migration {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Step { version: u32 }
    /// # #[subscriber(InboxQueue::<Migration>::new("acme"))]
    /// # async fn migrate(step: &Step) -> HandlerOutcome {
    /// #     tracing::info!(version = step.version, "migrating acme");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: PgPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("tenants", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
    /// #         b.include(migrate);
    /// #     })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Fifo, Fifo
);
setting!(
    /// The clock the table reads now from, a [`TimeSource`](crate::TimeSource): set by
    /// [`InboxSpec::clock`](crate::InboxSpec::clock).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "mysql", feature = "chrono"))]
    /// # mod demo {
    /// use std::time::{Duration, SystemTime, UNIX_EPOCH};
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::spec::{self, Payload, RetryAfter};
    /// use ruststream_sqlx::{Clock, InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{MySql, MySqlPool};
    ///
    /// /// The host's time in whole seconds, as the table's `DATETIME(0)` columns keep it.
    /// pub struct WholeSeconds;
    ///
    /// impl Clock for WholeSeconds {
    ///     fn now() -> SystemTime {
    ///         let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    ///         UNIX_EPOCH + Duration::from_secs(since.as_secs())
    ///     }
    /// }
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct Digest {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for Digest {
    ///     type Id = i64;
    ///     // Due times are bound from `WholeSeconds`, so they compare equal to what the column stores.
    ///     type Table = InboxSpec<(spec::Clock<WholeSeconds>, RetryAfter<DateTime<Utc>>, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("digests", Column::new("id").generated())
    ///         .clock::<WholeSeconds>()
    ///         .retry_after(Column::new("retry_after").generated())
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for Digest {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Daily { user: String }
    /// # #[subscriber(InboxQueue::<Digest>::new("digests"))]
    /// # async fn digest(daily: &Daily) -> HandlerOutcome {
    /// #     tracing::info!(user = %daily.user, "sending a digest");
    /// #     HandlerOutcome::retry_after(Duration::from_secs(86_400))
    /// # }
    /// # pub fn app(pool: MySqlPool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("digests", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<MySql>::new(pool), |b| {
    /// #             b.include(digest);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Clock<Source>, Clock
);
setting!(
    /// What the table's transactions open at, a [`level`] marker: set by
    /// [`InboxSpec::opens`](crate::InboxSpec::opens).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "sqlite", feature = "chrono"))]
    /// # mod demo {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::dialect::{Column, level};
    /// use ruststream_sqlx::spec::{Lease, Opens, Payload};
    /// use ruststream_sqlx::{InboxSpec, InboxTable};
    /// # use ruststream_sqlx::PayloadRow;
    /// # use ruststream_sqlx::prelude::*;
    /// # use serde::Deserialize;
    /// # use sqlx::{Sqlite, SqlitePool};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct PrintJob {
    ///     id: i64,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl InboxTable for PrintJob {
    ///     type Id = i64;
    ///     // A transaction takes the database's write lock as it begins, not at its first write.
    ///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Opens<level::Immediate>, Payload)>;
    ///     const TABLE: Self::Table = InboxSpec::new("print_jobs", Column::new("id").generated())
    ///         .lease(Column::new("locked_until"))
    ///         .opens::<level::Immediate>()
    ///         .payload(Column::new("payload"));
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    /// }
    /// # impl PayloadRow for PrintJob {
    /// #     type Column = Vec<u8>;
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Document { pages: u32 }
    /// # #[subscriber(InboxQueue::<PrintJob>::new("printer"))]
    /// # async fn print(document: &Document) -> HandlerOutcome {
    /// #     tracing::info!(pages = document.pages, "printing");
    /// #     HandlerOutcome::ack()
    /// # }
    /// # pub fn app(pool: SqlitePool) -> RustStream {
    /// #     RustStream::new(AppInfo::new("printing", "1.0.0"))
    /// #         .with_broker(SqlxBroker::<Sqlite>::new(pool), |b| {
    /// #             b.include(print);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Opens<Level>, Opening
);

/// The events a service writes itself, each set by [`InboxSpec::own`](crate::InboxSpec::own); the
/// row implements the event's trait.
pub mod own {
    use super::declaration::{Declaration, Set, Unset, setting};

    setting!(
        /// The service's own [`Claim`](crate::Claim): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Payload, own};
        /// use ruststream_sqlx::{Claim, InboxSpec, InboxTable};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct Callout {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for Callout {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Payload, own::Claim)>;
        ///     const TABLE: Self::Table = InboxSpec::new("callouts", Column::new("id").generated())
        ///         .group(Column::new("name"))
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Claim>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // The callouts of paying customers are claimed before the others.
        /// impl Claim<Postgres> for Callout {
        ///     async fn claim(
        ///         conn: &mut PgConnection,
        ///         queue: &str,
        ///         limit: i64,
        ///     ) -> Result<Vec<i64>, sqlx::Error> {
        ///         sqlx::query_scalar(
        ///             "SELECT c.id FROM callouts c JOIN customers u ON u.id = c.customer \
        ///              WHERE c.name = $1 ORDER BY u.paying DESC, c.id LIMIT $2 FOR UPDATE OF c SKIP LOCKED",
        ///         )
        ///         .bind(queue)
        ///         .bind(limit)
        ///         .fetch_all(conn)
        ///         .await
        ///     }
        /// }
        /// # impl PayloadRow for Callout {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Call { phone: String }
        /// # #[subscriber(InboxQueue::<Callout>::new("callouts"))]
        /// # async fn dial(call: &Call) -> HandlerOutcome {
        /// #     tracing::info!(phone = %call.phone, "dialing");
        /// #     HandlerOutcome::ack()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("callouts", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(dial);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Claim, OwnClaim
    );
    setting!(
        /// The service's own [`Fetch`](crate::Fetch): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Payload, own};
        /// use ruststream_sqlx::{Fetch, InboxSpec, InboxTable};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct RenderJob {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for RenderJob {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Payload, own::Fetch)>;
        ///     const TABLE: Self::Table = InboxSpec::new("render_jobs", Column::new("id").generated())
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Fetch>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // The job holds a document's id, and its message is that document's latest body.
        /// impl Fetch<Postgres> for RenderJob {
        ///     async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
        ///         sqlx::query_as(
        ///             "SELECT j.id, d.body AS payload FROM render_jobs j \
        ///              JOIN documents d ON d.id = j.document WHERE j.id = ANY($1)",
        ///         )
        ///         .bind(ids)
        ///         .fetch_all(conn)
        ///         .await
        ///     }
        /// }
        /// # impl PayloadRow for RenderJob {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Document { title: String }
        /// # #[subscriber(InboxQueue::<RenderJob>::new("renders"))]
        /// # async fn render(document: &Document) -> HandlerOutcome {
        /// #     tracing::info!(title = %document.title, "rendering");
        /// #     HandlerOutcome::ack()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("documents", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(render);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Fetch, OwnFetch
    );
    setting!(
        /// The service's own [`Ack`](crate::Ack): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Payload, own};
        /// use ruststream_sqlx::{Ack, InboxSpec, InboxTable};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct UploadJob {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for UploadJob {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Payload, own::Ack)>;
        ///     const TABLE: Self::Table = InboxSpec::new("uploads", Column::new("id").generated())
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Ack>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // A finished upload leaves a line in `upload_log` as its row goes.
        /// impl Ack<Postgres> for UploadJob {
        ///     async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        ///         sqlx::query("INSERT INTO upload_log (upload, finished_at) VALUES ($1, now())")
        ///             .bind(id)
        ///             .execute(&mut *conn)
        ///             .await?;
        ///         sqlx::query("DELETE FROM uploads WHERE id = $1")
        ///             .bind(id)
        ///             .execute(conn)
        ///             .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # impl PayloadRow for UploadJob {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Upload { key: String }
        /// # #[subscriber(InboxQueue::<UploadJob>::new("uploads"))]
        /// # async fn store(upload: &Upload) -> HandlerOutcome {
        /// #     tracing::info!(key = %upload.key, "storing");
        /// #     HandlerOutcome::ack()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("uploads", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(store);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Ack, OwnAck
    );
    setting!(
        /// The service's own [`Retry`](crate::Retry): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Payload, own};
        /// use ruststream_sqlx::{InboxSpec, InboxTable, Retry};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct CrawlJob {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for CrawlJob {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Payload, own::Retry)>;
        ///     const TABLE: Self::Table = InboxSpec::new("crawl_jobs", Column::new("id").generated())
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Retry>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // A retry records when the page last failed, for the crawler's dashboard.
        /// impl Retry<Postgres> for CrawlJob {
        ///     async fn retry(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        ///         sqlx::query("UPDATE crawl_jobs SET last_failure = now() WHERE id = $1")
        ///             .bind(id)
        ///             .execute(conn)
        ///             .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # impl PayloadRow for CrawlJob {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Page { url: String }
        /// # #[subscriber(InboxQueue::<CrawlJob>::new("crawl"))]
        /// # async fn crawl(page: &Page) -> HandlerOutcome {
        /// #     tracing::info!(url = %page.url, "crawling");
        /// #     HandlerOutcome::retry()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("crawler", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(crawl);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Retry, OwnRetry
    );
    setting!(
        /// The service's own [`RetryAfter`](crate::RetryAfter): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
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
        /// use ruststream_sqlx::spec::{self, Payload, own};
        /// use ruststream_sqlx::{InboxSpec, InboxTable, RetryAfter};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct QuoteJob {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for QuoteJob {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(spec::RetryAfter<DateTime<Utc>>, Payload, own::RetryAfter)>;
        ///     const TABLE: Self::Table = InboxSpec::new("quote_jobs", Column::new("id").generated())
        ///         .retry_after(Column::new("retry_after").generated())
        ///         .payload(Column::new("payload"))
        ///         .own::<own::RetryAfter>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // The wait doubles with every delayed retry of the row, starting from the handler's delay.
        /// impl RetryAfter<Postgres> for QuoteJob {
        ///     async fn retry_after(
        ///         conn: &mut PgConnection,
        ///         id: &i64,
        ///         delay: Duration,
        ///     ) -> Result<(), sqlx::Error> {
        ///         sqlx::query(
        ///             "UPDATE quote_jobs SET delays = delays + 1, \
        ///              retry_after = now() + make_interval(secs => $2 * power(2, delays)) WHERE id = $1",
        ///         )
        ///         .bind(id)
        ///         .bind(delay.as_secs_f64())
        ///         .execute(conn)
        ///         .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # impl PayloadRow for QuoteJob {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Quote { symbol: String }
        /// # #[subscriber(InboxQueue::<QuoteJob>::new("quotes"))]
        /// # async fn quote(job: &Quote) -> HandlerOutcome {
        /// #     tracing::info!(symbol = %job.symbol, "quoting");
        /// #     HandlerOutcome::retry_after(Duration::from_secs(10))
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("quotes", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(quote);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        RetryAfter, OwnRetryAfter
    );
    setting!(
        /// The service's own [`Discard`](crate::Discard): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Payload, own};
        /// use ruststream_sqlx::{Discard, InboxSpec, InboxTable};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct Comment {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for Comment {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Payload, own::Discard)>;
        ///     const TABLE: Self::Table = InboxSpec::new("comments", Column::new("id").generated())
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Discard>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // A rejected comment moves to `rejected_comments`, stamped with the time of the rejection.
        /// impl Discard<Postgres> for Comment {
        ///     async fn discard(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        ///         sqlx::query(
        ///             "WITH gone AS (DELETE FROM comments WHERE id = $1 RETURNING id, payload) \
        ///              INSERT INTO rejected_comments (id, payload, rejected_at) \
        ///              SELECT id, payload, now() FROM gone",
        ///         )
        ///         .bind(id)
        ///         .execute(conn)
        ///         .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # impl PayloadRow for Comment {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Text { body: String }
        /// # #[subscriber(InboxQueue::<Comment>::new("comments"))]
        /// # async fn moderate(text: &Text) -> HandlerOutcome {
        /// #     if text.body.is_empty() { HandlerOutcome::drop() } else { HandlerOutcome::ack() }
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("comments", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(moderate);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Discard, OwnDiscard
    );
    setting!(
        /// The service's own [`DeadLetter`](crate::DeadLetter): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Payload, own};
        /// use ruststream_sqlx::{DeadLetter, InboxSpec, InboxTable};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct Charge {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for Charge {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Payload, own::DeadLetter)>;
        ///     const TABLE: Self::Table = InboxSpec::new("charges", Column::new("id").generated())
        ///         .payload(Column::new("payload"))
        ///         .own::<own::DeadLetter>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // Every queue of the service parks its spent rows in one `dead_letters` table.
        /// impl DeadLetter<Postgres> for Charge {
        ///     async fn dead_letter(
        ///         conn: &mut PgConnection,
        ///         id: &i64,
        ///         destination: &str,
        ///     ) -> Result<(), sqlx::Error> {
        ///         sqlx::query(
        ///             "WITH dead AS (DELETE FROM charges WHERE id = $1 RETURNING payload) \
        ///              INSERT INTO dead_letters (source, destination, payload) \
        ///              SELECT 'charges', $2, payload FROM dead",
        ///         )
        ///         .bind(id)
        ///         .bind(destination)
        ///         .execute(conn)
        ///         .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # impl PayloadRow for Charge {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Card { cents: i64 }
        /// # #[subscriber(InboxQueue::<Charge>::new("charges"))]
        /// # async fn charge(card: &Card) -> HandlerOutcome {
        /// #     tracing::info!(cents = card.cents, "charging");
        /// #     HandlerOutcome::retry()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("charges", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(charge);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        DeadLetter, OwnDeadLetter
    );
    setting!(
        /// The service's own [`Extend`](crate::Extend): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(all(feature = "postgres", feature = "chrono"))]
        /// # mod demo {
        /// use chrono::{DateTime, Utc};
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::spec::{Lease, Payload, own};
        /// use ruststream_sqlx::{Extend, InboxSpec, InboxTable};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::PayloadRow;
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct Transcode {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for Transcode {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload, own::Extend)>;
        ///     const TABLE: Self::Table = InboxSpec::new("transcodes", Column::new("id").generated())
        ///         .lease(Column::new("locked_until"))
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Extend>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // Each extension also counts in `extensions`, which shows a transcode that runs too long.
        /// impl Extend<Postgres> for Transcode {
        ///     async fn extend(
        ///         conn: &mut PgConnection,
        ///         id: &i64,
        ///         held: &DateTime<Utc>,
        ///         until: &DateTime<Utc>,
        ///     ) -> Result<bool, sqlx::Error> {
        ///         let extended = sqlx::query(
        ///             "UPDATE transcodes SET locked_until = $1, extensions = extensions + 1 \
        ///              WHERE id = $2 AND locked_until = $3",
        ///         )
        ///         .bind(until)
        ///         .bind(id)
        ///         .bind(held)
        ///         .execute(conn)
        ///         .await?;
        ///         Ok(extended.rows_affected() == 1)
        ///     }
        /// }
        /// # impl PayloadRow for Transcode {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Video { file: String }
        /// # #[subscriber(InboxQueue::<Transcode>::new("transcodes"))]
        /// # async fn transcode(video: &Video) -> HandlerOutcome {
        /// #     tracing::info!(file = %video.file, "transcoding");
        /// #     HandlerOutcome::ack()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("video", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(transcode);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Extend, OwnExtend
    );
    setting!(
        /// The service's own [`Lock`](crate::Lock): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// The service's own lock comes with its own unlock, [`own::Unlock`](Unlock):
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::{Column, KeyPart};
        /// use ruststream_sqlx::spec::{Advisory, Payload, own};
        /// use ruststream_sqlx::{InboxSpec, InboxTable, Lock};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::{PayloadRow, Unlock};
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct Booking {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for Booking {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Advisory, Payload, own::Lock, own::Unlock)>;
        ///     const TABLE: Self::Table = InboxSpec::new("bookings", Column::new("id").generated())
        ///         .advisory(&[KeyPart::Literal("room-"), KeyPart::Column("room")])
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Lock>()
        ///         .own::<own::Unlock>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // The front desk application locks a room as `(42, hashtext(key))`, so the queue takes that lock.
        /// impl Lock<Postgres> for Booking {
        ///     async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
        ///         sqlx::query_scalar("SELECT pg_try_advisory_lock(42, hashtext($1))")
        ///             .bind(key)
        ///             .fetch_one(conn)
        ///             .await
        ///     }
        /// }
        /// # impl Unlock<Postgres> for Booking {
        /// #     async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
        /// #         sqlx::query_scalar("SELECT pg_advisory_unlock(42, hashtext($1))")
        /// #             .bind(key)
        /// #             .fetch_one(conn)
        /// #             .await
        /// #     }
        /// # }
        /// # impl PayloadRow for Booking {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Request { guest: String }
        /// # #[subscriber(InboxQueue::<Booking>::new("bookings"))]
        /// # async fn book(request: &Request) -> HandlerOutcome {
        /// #     tracing::info!(guest = %request.guest, "booking");
        /// #     HandlerOutcome::ack()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("hotel", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(book);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Lock, OwnLock
    );
    setting!(
        /// The service's own [`Unlock`](crate::Unlock): set by
        /// [`InboxSpec::own`](crate::InboxSpec::own).
        ///
        /// # Examples
        ///
        /// The service's own unlock releases what its own lock, [`own::Lock`](Lock), took:
        ///
        /// ```
        /// # #[cfg(feature = "postgres")]
        /// # mod demo {
        /// use ruststream_sqlx::dialect::{Column, KeyPart};
        /// use ruststream_sqlx::spec::{Advisory, Payload, own};
        /// use ruststream_sqlx::{InboxSpec, InboxTable, Unlock};
        /// use sqlx::{PgConnection, Postgres};
        /// # use ruststream_sqlx::{Lock, PayloadRow};
        /// # use ruststream_sqlx::prelude::*;
        /// # use serde::Deserialize;
        /// # use sqlx::PgPool;
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct Rebalance {
        ///     id: i64,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl InboxTable for Rebalance {
        ///     type Id = i64;
        ///     type Table = InboxSpec<(Advisory, Payload, own::Lock, own::Unlock)>;
        ///     const TABLE: Self::Table = InboxSpec::new("rebalances", Column::new("id").generated())
        ///         .advisory(&[KeyPart::Literal("portfolio-"), KeyPart::Column("portfolio")])
        ///         .payload(Column::new("payload"))
        ///         .own::<own::Lock>()
        ///         .own::<own::Unlock>();
        ///
        ///     fn id(&self) -> &i64 {
        ///         &self.id
        ///     }
        /// }
        ///
        /// // The trading desk locks a portfolio as `(7, hashtext(key))`, and the unlock releases that lock.
        /// impl Unlock<Postgres> for Rebalance {
        ///     async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
        ///         sqlx::query_scalar("SELECT pg_advisory_unlock(7, hashtext($1))")
        ///             .bind(key)
        ///             .fetch_one(conn)
        ///             .await
        ///     }
        /// }
        /// # impl Lock<Postgres> for Rebalance {
        /// #     async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
        /// #         sqlx::query_scalar("SELECT pg_try_advisory_lock(7, hashtext($1))")
        /// #             .bind(key)
        /// #             .fetch_one(conn)
        /// #             .await
        /// #     }
        /// # }
        /// # impl PayloadRow for Rebalance {
        /// #     type Column = Vec<u8>;
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// # }
        /// # #[derive(Deserialize)]
        /// # struct Target { equity: f64 }
        /// # #[subscriber(InboxQueue::<Rebalance>::new("rebalances"))]
        /// # async fn rebalance(target: &Target) -> HandlerOutcome {
        /// #     tracing::info!(equity = target.equity, "rebalancing");
        /// #     HandlerOutcome::ack()
        /// # }
        /// # pub fn app(pool: PgPool) -> RustStream {
        /// #     RustStream::new(AppInfo::new("portfolios", "1.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        /// #         b.include(rebalance);
        /// #     })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Unlock, OwnUnlock
    );
}

/// An event a service writes itself: a marker of [`own`], which
/// [`InboxSpec::own`](crate::InboxSpec::own) takes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an event of the service's own",
    label = "not an event",
    note = "name one of `spec::own::{{Claim, Fetch, Ack, Retry, RetryAfter, Discard, DeadLetter, \
            Extend, Lock, Unlock}}`"
)]
pub trait OwnEvent: Declaration {}

macro_rules! own_events {
    ($($event:ident),*) => {
        $(impl OwnEvent for own::$event {})*
    };
}

own_events!(
    Claim, Fetch, Ack, Retry, RetryAfter, Discard, DeadLetter, Extend, Lock, Unlock
);

/// An isolation level or a SQLite mode as a [`level`] marker, which
/// [`InboxSpec::opens`](crate::InboxSpec::opens) takes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an isolation level or a SQLite mode",
    label = "not a level",
    note = "name one of the `dialect::level` markers"
)]
pub trait OpeningLevel {
    /// The opening the marker stands for.
    const OPENING: Opening;
}

macro_rules! levels {
    ($($marker:ident => $opening:expr),*) => {
        $(impl OpeningLevel for level::$marker {
            const OPENING: Opening = $opening;
        })*
    };
}

levels!(
    ReadUncommitted => Opening::Isolation(Isolation::ReadUncommitted),
    ReadCommitted => Opening::Isolation(Isolation::ReadCommitted),
    RepeatableRead => Opening::Isolation(Isolation::RepeatableRead),
    Serializable => Opening::Isolation(Isolation::Serializable),
    Deferred => Opening::Mode(Mode::Deferred),
    Immediate => Opening::Mode(Mode::Immediate),
    Exclusive => Opening::Mode(Mode::Exclusive)
);
