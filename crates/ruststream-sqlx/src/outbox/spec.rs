//! The settings of an outbox table described by hand, as types.
//!
//! Each typed setter of [`OutboxSpec`] adds one marker of this module to the
//! table's settings, and the table's `type Table` lists them in the order the chain sets them. A
//! setting the chain leaves out keeps its default: no headers column, a processed record deleted,
//! and the crate's events. A setting set twice does not compile ([`Merge`]).

mod builder;
mod declaration;

pub use builder::{Described, OutboxSpec, OutboxTable};
pub use declaration::Declaration;
use declaration::setting;

pub use crate::settings::{Merge, Push, Set, Unset};

setting!(
    /// The published headers, kept in one column: set by
    /// [`OutboxSpec::headers`](super::OutboxSpec::headers). The record implements
    /// [`HeaderRow`](crate::HeaderRow).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "json", feature = "postgres"))]
    /// # mod demo {
    /// # use ruststream::OutgoingMessage;
    /// # use ruststream::memory::prelude::*;
    /// # use serde::{Deserialize, Serialize};
    /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
    /// use std::collections::BTreeMap;
    ///
    /// use ruststream_sqlx::HeaderRow;
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::spec::Headers;
    /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
    /// use sqlx::types::Json;
    /// use sqlx::{PgConnection, PgPool, Postgres};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct OrderEvent {
    ///     id: i64,
    ///     name: String,
    ///     payload: Vec<u8>,
    ///     headers: Option<Json<BTreeMap<String, String>>>,
    /// }
    ///
    /// impl OutboxTable for OrderEvent {
    ///     type Id = i64;
    ///     type Table = OutboxSpec<(Headers,)>;
    ///     const TABLE: Self::Table = OutboxSpec::new(
    ///         "outbox",
    ///         Column::new("id"),
    ///         Column::new("name"),
    ///         Column::new("payload"),
    ///     )
    ///     .headers(Column::new("headers"));
    /// #     fn id(&self) -> &i64 { &self.id }
    /// #     fn name(&self) -> &str { &self.name }
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// }
    ///
    /// // A republished record carries the headers it was published with.
    /// impl HeaderRow for OrderEvent {
    ///     type Column = Option<Json<BTreeMap<String, String>>>;
    ///
    ///     fn headers_mut(&mut self) -> &mut Self::Column {
    ///         &mut self.headers
    ///     }
    /// }
    /// # impl outbox::Publish<Postgres> for OrderEvent {
    /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
    /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
    /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
    /// # pub struct Orders;
    /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
    /// # pub fn app(pool: PgPool) -> impl App {
    /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
    /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
    /// #         .layer(tracking.layer())
    /// #         .publish_layer(tracking.publish_layer())
    /// #         .with_broker(MemoryBroker::new(), |b| {
    /// #             b.include(place).out_reply(Publish);
    /// #             b.include(fulfil);
    /// #             b.after_startup(Publish, tracking.republish());
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    Headers, Headers
);
setting!(
    /// The time a record was processed, which the database's clock writes: set by
    /// [`OutboxSpec::processed_at`](super::OutboxSpec::processed_at). Without it a processed record
    /// is deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
    /// # mod demo {
    /// # use ruststream::OutgoingMessage;
    /// # use ruststream::memory::prelude::*;
    /// # use serde::{Deserialize, Serialize};
    /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::spec::ProcessedAt;
    /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
    /// use sqlx::{PgConnection, PgPool, Postgres};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct OrderEvent {
    ///     id: i64,
    ///     name: String,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// // A processed record stays in the table, stamped with the database's time.
    /// impl OutboxTable for OrderEvent {
    ///     type Id = i64;
    ///     type Table = OutboxSpec<(ProcessedAt,)>;
    ///     const TABLE: Self::Table = OutboxSpec::new(
    ///         "outbox",
    ///         Column::new("id"),
    ///         Column::new("name"),
    ///         Column::new("payload"),
    ///     )
    ///     .processed_at(Column::new("processed_at"));
    /// #     fn id(&self) -> &i64 { &self.id }
    /// #     fn name(&self) -> &str { &self.name }
    /// #     fn payload(&self) -> &[u8] { &self.payload }
    /// }
    /// # impl outbox::Publish<Postgres> for OrderEvent {
    /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
    /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
    /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
    /// # pub struct Orders;
    /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
    /// # pub fn app(pool: PgPool) -> impl App {
    /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
    /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
    /// #         .layer(tracking.layer())
    /// #         .publish_layer(tracking.publish_layer())
    /// #         .with_broker(MemoryBroker::new(), |b| {
    /// #             b.include(place).out_reply(Publish);
    /// #             b.include(fulfil);
    /// #             b.after_startup(Publish, tracking.republish());
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    ProcessedAt, ProcessedAt
);

/// The events a record runs itself instead of the crate's default, each set by
/// [`OutboxSpec::own`](super::OutboxSpec::own); the record implements the event's trait of
/// [`outbox`](super).
pub mod own {
    use super::declaration::{Declaration, setting};
    use super::{Set, Unset};

    setting!(
        /// The take of a record when its message reaches a subscription:
        /// [`outbox::Fetch`](crate::outbox::Fetch).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
        /// # mod demo {
        /// # use ruststream::OutgoingMessage;
        /// # use ruststream::memory::prelude::*;
        /// # use serde::{Deserialize, Serialize};
        /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::outbox::spec::own;
        /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
        /// use sqlx::{PgConnection, PgPool, Postgres};
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct OrderEvent {
        ///     id: i64,
        ///     name: String,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl OutboxTable for OrderEvent {
        ///     type Id = i64;
        ///     type Table = OutboxSpec<(own::Fetch,)>;
        ///     const TABLE: Self::Table = OutboxSpec::new(
        ///         "outbox",
        ///         Column::new("id"),
        ///         Column::new("name"),
        ///         Column::new("payload"),
        ///     )
        ///     .own::<own::Fetch>();
        /// #     fn id(&self) -> &i64 { &self.id }
        /// #     fn name(&self) -> &str { &self.name }
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// }
        /// # impl outbox::Publish<Postgres> for OrderEvent {
        /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
        /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
        /// #     }
        /// # }
        ///
        /// // A record the service cancelled is not delivered: its delivery is acknowledged without the handler.
        /// impl outbox::Fetch<Postgres> for OrderEvent {
        ///     async fn fetch(conn: &mut PgConnection, id: &i64) -> sqlx::Result<Option<Self>> {
        ///         sqlx::query_as("SELECT id, name, payload FROM outbox WHERE id = $1 AND NOT cancelled")
        ///             .bind(id)
        ///             .fetch_optional(conn)
        ///             .await
        ///     }
        /// }
        /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
        /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
        /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
        /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
        /// # pub struct Orders;
        /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
        /// # pub fn app(pool: PgPool) -> impl App {
        /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
        /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
        /// #         .layer(tracking.layer())
        /// #         .publish_layer(tracking.publish_layer())
        /// #         .with_broker(MemoryBroker::new(), |b| {
        /// #             b.include(place).out_reply(Publish);
        /// #             b.include(fulfil);
        /// #             b.after_startup(Publish, tracking.republish());
        /// #         })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Fetch, OwnFetch
    );
    setting!(
        /// The mark of an acknowledged record: [`outbox::Ack`](crate::outbox::Ack).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
        /// # mod demo {
        /// # use ruststream::OutgoingMessage;
        /// # use ruststream::memory::prelude::*;
        /// # use serde::{Deserialize, Serialize};
        /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::outbox::spec::own;
        /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
        /// use sqlx::{PgConnection, PgPool, Postgres};
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct OrderEvent {
        ///     id: i64,
        ///     name: String,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl OutboxTable for OrderEvent {
        ///     type Id = i64;
        ///     type Table = OutboxSpec<(own::Ack,)>;
        ///     const TABLE: Self::Table = OutboxSpec::new(
        ///         "outbox",
        ///         Column::new("id"),
        ///         Column::new("name"),
        ///         Column::new("payload"),
        ///     )
        ///     .own::<own::Ack>();
        /// #     fn id(&self) -> &i64 { &self.id }
        /// #     fn name(&self) -> &str { &self.name }
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// }
        /// # impl outbox::Publish<Postgres> for OrderEvent {
        /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
        /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
        /// #     }
        /// # }
        ///
        /// // An acknowledged record moves to the archive.
        /// impl outbox::Ack<Postgres> for OrderEvent {
        ///     async fn ack(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
        ///         sqlx::query(
        ///             "WITH done AS (DELETE FROM outbox WHERE id = $1 RETURNING *) \
        ///              INSERT INTO outbox_archive SELECT * FROM done",
        ///         )
        ///         .bind(id)
        ///         .execute(conn)
        ///         .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
        /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
        /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
        /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
        /// # pub struct Orders;
        /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
        /// # pub fn app(pool: PgPool) -> impl App {
        /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
        /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
        /// #         .layer(tracking.layer())
        /// #         .publish_layer(tracking.publish_layer())
        /// #         .with_broker(MemoryBroker::new(), |b| {
        /// #             b.include(place).out_reply(Publish);
        /// #             b.include(fulfil);
        /// #             b.after_startup(Publish, tracking.republish());
        /// #         })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Ack, OwnAck
    );
    setting!(
        /// What a retried record runs: [`outbox::Retry`](crate::outbox::Retry).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
        /// # mod demo {
        /// # use ruststream::OutgoingMessage;
        /// # use ruststream::memory::prelude::*;
        /// # use serde::{Deserialize, Serialize};
        /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::outbox::spec::own;
        /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
        /// use sqlx::{PgConnection, PgPool, Postgres};
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct OrderEvent {
        ///     id: i64,
        ///     name: String,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl OutboxTable for OrderEvent {
        ///     type Id = i64;
        ///     type Table = OutboxSpec<(own::Retry,)>;
        ///     const TABLE: Self::Table = OutboxSpec::new(
        ///         "outbox",
        ///         Column::new("id"),
        ///         Column::new("name"),
        ///         Column::new("payload"),
        ///     )
        ///     .own::<own::Retry>();
        /// #     fn id(&self) -> &i64 { &self.id }
        /// #     fn name(&self) -> &str { &self.name }
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// }
        /// # impl outbox::Publish<Postgres> for OrderEvent {
        /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
        /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
        /// #     }
        /// # }
        ///
        /// // The service counts how often a consumer asked for a record again.
        /// impl outbox::Retry<Postgres> for OrderEvent {
        ///     async fn retry(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
        ///         sqlx::query("UPDATE outbox SET retries = retries + 1 WHERE id = $1")
        ///             .bind(id)
        ///             .execute(conn)
        ///             .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
        /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
        /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
        /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
        /// # pub struct Orders;
        /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
        /// # pub fn app(pool: PgPool) -> impl App {
        /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
        /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
        /// #         .layer(tracking.layer())
        /// #         .publish_layer(tracking.publish_layer())
        /// #         .with_broker(MemoryBroker::new(), |b| {
        /// #             b.include(place).out_reply(Publish);
        /// #             b.include(fulfil);
        /// #             b.after_startup(Publish, tracking.republish());
        /// #         })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Retry, OwnRetry
    );
    setting!(
        /// The mark of a dropped record: [`outbox::Discard`](crate::outbox::Discard).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
        /// # mod demo {
        /// # use ruststream::OutgoingMessage;
        /// # use ruststream::memory::prelude::*;
        /// # use serde::{Deserialize, Serialize};
        /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::outbox::spec::own;
        /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
        /// use sqlx::{PgConnection, PgPool, Postgres};
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct OrderEvent {
        ///     id: i64,
        ///     name: String,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl OutboxTable for OrderEvent {
        ///     type Id = i64;
        ///     type Table = OutboxSpec<(own::Discard,)>;
        ///     const TABLE: Self::Table = OutboxSpec::new(
        ///         "outbox",
        ///         Column::new("id"),
        ///         Column::new("name"),
        ///         Column::new("payload"),
        ///     )
        ///     .own::<own::Discard>();
        /// #     fn id(&self) -> &i64 { &self.id }
        /// #     fn name(&self) -> &str { &self.name }
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// }
        /// # impl outbox::Publish<Postgres> for OrderEvent {
        /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
        /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
        /// #     }
        /// # }
        ///
        /// // A dropped record moves to a table the operators review.
        /// impl outbox::Discard<Postgres> for OrderEvent {
        ///     async fn discard(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
        ///         sqlx::query(
        ///             "WITH dropped AS (DELETE FROM outbox WHERE id = $1 RETURNING *) \
        ///              INSERT INTO outbox_dropped SELECT * FROM dropped",
        ///         )
        ///         .bind(id)
        ///         .execute(conn)
        ///         .await?;
        ///         Ok(())
        ///     }
        /// }
        /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
        /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
        /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
        /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
        /// # pub struct Orders;
        /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
        /// # pub fn app(pool: PgPool) -> impl App {
        /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
        /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
        /// #         .layer(tracking.layer())
        /// #         .publish_layer(tracking.publish_layer())
        /// #         .with_broker(MemoryBroker::new(), |b| {
        /// #             b.include(place).out_reply(Publish);
        /// #             b.include(fulfil);
        /// #             b.after_startup(Publish, tracking.republish());
        /// #         })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Discard, OwnDiscard
    );
    setting!(
        /// The selection of the unprocessed records at startup:
        /// [`outbox::Recover`](crate::outbox::Recover).
        ///
        /// # Examples
        ///
        /// ```
        /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
        /// # mod demo {
        /// # use ruststream::OutgoingMessage;
        /// # use ruststream::memory::prelude::*;
        /// # use serde::{Deserialize, Serialize};
        /// # use ruststream_sqlx::outbox::{Outbox, TrackedName};
        /// use ruststream_sqlx::dialect::Column;
        /// use ruststream_sqlx::outbox::spec::own;
        /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
        /// use sqlx::{PgConnection, PgPool, Postgres};
        ///
        /// #[derive(sqlx::FromRow)]
        /// pub struct OrderEvent {
        ///     id: i64,
        ///     name: String,
        ///     payload: Vec<u8>,
        /// }
        ///
        /// impl OutboxTable for OrderEvent {
        ///     type Id = i64;
        ///     type Table = OutboxSpec<(own::Recover,)>;
        ///     const TABLE: Self::Table = OutboxSpec::new(
        ///         "outbox",
        ///         Column::new("id"),
        ///         Column::new("name"),
        ///         Column::new("payload"),
        ///     )
        ///     .own::<own::Recover>();
        /// #     fn id(&self) -> &i64 { &self.id }
        /// #     fn name(&self) -> &str { &self.name }
        /// #     fn payload(&self) -> &[u8] { &self.payload }
        /// }
        /// # impl outbox::Publish<Postgres> for OrderEvent {
        /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
        /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
        /// #     }
        /// # }
        ///
        /// // The startup republish sends the records oldest first.
        /// impl outbox::Recover<Postgres> for OrderEvent {
        ///     async fn recover(conn: &mut PgConnection, name: &str) -> sqlx::Result<Vec<Self>> {
        ///         sqlx::query_as("SELECT id, name, payload FROM outbox WHERE name = $1 ORDER BY id")
        ///             .bind(name)
        ///             .fetch_all(conn)
        ///             .await
        ///     }
        /// }
        /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
        /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
        /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
        /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
        /// # pub struct Orders;
        /// # impl TrackedName for Orders { const NAME: &'static str = "orders"; }
        /// # pub fn app(pool: PgPool) -> impl App {
        /// #     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
        /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
        /// #         .layer(tracking.layer())
        /// #         .publish_layer(tracking.publish_layer())
        /// #         .with_broker(MemoryBroker::new(), |b| {
        /// #             b.include(place).out_reply(Publish);
        /// #             b.include(fulfil);
        /// #             b.after_startup(Publish, tracking.republish());
        /// #         })
        /// # }
        /// # }
        /// # fn main() {}
        /// ```
        Recover, OwnRecover
    );
}

/// An event a record runs itself: a marker of [`own`], which
/// [`OutboxSpec::own`](super::OutboxSpec::own) takes.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an event of an outbox record",
    label = "not an outbox event",
    note = "name one of `outbox::spec::own::{{Fetch, Ack, Retry, Discard, Recover}}`"
)]
pub trait OwnEvent: Declaration {}

impl OwnEvent for own::Fetch {}
impl OwnEvent for own::Ack {}
impl OwnEvent for own::Retry {}
impl OwnEvent for own::Discard {}
impl OwnEvent for own::Recover {}
