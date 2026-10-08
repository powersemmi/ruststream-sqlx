//! One trait per event of an outbox record: what a service implements when it names the event as
//! its own (`#[outbox(custom(..))]`, or `own` on its `OutboxSpec`), and `Publish`, which has no
//! default.

use std::future::Future;

use ruststream::OutgoingMessage;
use sqlx::{Database, Error};

use super::OutboxRow;
use super::database::Defaults;
use super::dispatch::{AckBy, Declared, DiscardBy, FetchBy, HeadersOf, RecoverBy, RetryBy};
use super::spec::{Declaration, OutboxTable};

/// Creates the record of a message a handler publishes under a registered name, and returns its
/// id, which the message then carries in [`OUTBOX_ID_HEADER`](super::OUTBOX_ID_HEADER).
///
/// It has no default: the service's statement lays the name, the payload and the headers out in
/// its columns. A record type without it does not register.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use chrono::{DateTime, Utc};
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream_sqlx::{Outbox, outbox};
///
/// #[derive(Outbox, sqlx::FromRow)]
/// #[outbox(table = "outbox")]
/// pub struct OrderOutbox {
///     #[field(id)]
///     id: i64,
///     #[field(name)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
///     #[field(processed_at)]
///     processed_at: Option<DateTime<Utc>>,
/// }
///
/// impl outbox::Publish<Postgres> for OrderOutbox {
///     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
///         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
///             .bind(msg.name())
///             .bind(msg.payload())
///             .fetch_one(conn)
///             .await
///     }
/// }
///
/// pub fn app(pool: PgPool) -> impl App {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(tracking.layer())
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///             b.after_startup(Publish, tracking.republish());
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `outbox::Publish<{DB}>`, so nothing records what is published under its names",
    label = "no `outbox::Publish` for this record",
    note = "implement `outbox::Publish<{DB}>` for `{Self}`: its statement inserts the record and returns the id"
)]
pub trait Publish<DB: Database>: OutboxRow {
    /// Inserts the record of `msg` and returns its id.
    ///
    /// # Errors
    ///
    /// The database's error; the message is not sent, and the publish fails with it.
    fn publish(
        conn: &mut DB::Connection,
        msg: &OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<Self::Id, Error>> + Send;
}

/// Takes the record `id` into work when its message reaches a subscription.
///
/// The default selects the record while it is unprocessed. `None` means the record is taken or
/// processed already: the handler does not run, and the delivery is acknowledged.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use chrono::{DateTime, Utc};
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream_sqlx::{Outbox, outbox};
///
/// #[derive(Outbox, sqlx::FromRow)]
/// #[outbox(table = "outbox", custom(fetch))]
/// pub struct OrderOutbox {
///     #[field(id)]
///     id: i64,
///     #[field(name)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
///     #[field(processed_at)]
///     processed_at: Option<DateTime<Utc>>,
/// }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
///
/// // The service's fetch also stamps when the record was taken.
/// impl outbox::Fetch<Postgres> for OrderOutbox {
///     async fn fetch(conn: &mut PgConnection, id: &i64) -> sqlx::Result<Option<Self>> {
///         sqlx::query_as(
///             "UPDATE outbox SET taken_at = now() WHERE id = $1 AND processed_at IS NULL \
///              RETURNING id, name, payload, processed_at",
///         )
///         .bind(id)
///         .fetch_optional(conn)
///         .await
///     }
/// }
///
/// pub fn app(pool: PgPool) -> impl App {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(tracking.layer())
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///             b.after_startup(Publish, tracking.republish());
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` names its own fetch and does not implement `outbox::Fetch<{DB}>`",
    label = "the service's own fetch is missing",
    note = "implement `outbox::Fetch<{DB}>` for `{Self}`, or drop `fetch` from `#[outbox(custom(..))]` (or `.own::<own::Fetch>()` from its `OutboxSpec`)"
)]
pub trait Fetch<DB: Database>: OutboxRow {
    /// Takes the record `id` into work, or `None` when it is taken or processed already.
    ///
    /// # Errors
    ///
    /// The database's error; the handler does not run, and the delivery is retried.
    fn fetch(
        conn: &mut DB::Connection,
        id: &Self::Id,
    ) -> impl Future<Output = Result<Option<Self>, Error>> + Send;
}

macro_rules! outcome_event {
    (
        $(#[$doc:meta])* $trait:ident, $method:ident, $event:literal,
        message = $message:literal, note = $note:literal
    ) => {
        $(#[$doc])*
        #[diagnostic::on_unimplemented(
            message = $message,
            label = "the service's own event is missing",
            note = $note
        )]
        pub trait $trait<DB: Database>: OutboxRow {
            #[doc = concat!("Runs the `", $event, "` event for the record `id` after its handler finished.")]
            #[doc = ""]
            #[doc = "# Errors"]
            #[doc = ""]
            #[doc = "The database's error; the record stays unprocessed, and the next startup publishes it again."]
            fn $method(
                conn: &mut DB::Connection,
                id: &Self::Id,
            ) -> impl Future<Output = Result<(), Error>> + Send;
        }
    };
}

outcome_event!(
    /// Marks the record processed once its handler acknowledged the message.
    ///
    /// The default sets `processed_at` from the database's clock, or deletes the record when the
    /// struct has no `processed_at` field.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
    /// # mod demo {
    /// # use chrono::{DateTime, Utc};
    /// # use ruststream::OutgoingMessage;
    /// # use ruststream::memory::prelude::*;
    /// # use serde::{Deserialize, Serialize};
    /// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
    /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
    /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
    /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
    /// use ruststream_sqlx::{Outbox, outbox};
    ///
    /// #[derive(Outbox, sqlx::FromRow)]
    /// #[outbox(table = "outbox", custom(ack))]
    /// pub struct OrderOutbox {
    ///     #[field(id)]
    ///     id: i64,
    ///     #[field(name)]
    ///     name: String,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    ///     #[field(processed_at)]
    ///     processed_at: Option<DateTime<Utc>>,
    /// }
    /// # impl outbox::Publish<Postgres> for OrderOutbox {
    /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    ///
    /// // A processed record moves to the archive.
    /// impl outbox::Ack<Postgres> for OrderOutbox {
    ///     async fn ack(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
    ///         sqlx::query(
    ///             "WITH done AS (DELETE FROM outbox WHERE id = $1 RETURNING id, name, payload) \
    ///              INSERT INTO outbox_archive (id, name, payload) SELECT id, name, payload FROM done",
    ///         )
    ///         .bind(id)
    ///         .execute(conn)
    ///         .await?;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .layer(tracking.layer())
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.include(fulfil);
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    Ack, ack, "ack",
    message = "`{Self}` names its own ack and does not implement `outbox::Ack<{DB}>`",
    note = "implement `outbox::Ack<{DB}>` for `{Self}`, or drop `ack` from `#[outbox(custom(..))]` (or `.own::<own::Ack>()` from its `OutboxSpec`)"
);

outcome_event!(
    /// Runs when the handler asked for the message again.
    ///
    /// The default leaves the record unprocessed and runs no statement, so the next startup
    /// publishes it again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
    /// # mod demo {
    /// # use chrono::{DateTime, Utc};
    /// # use ruststream::OutgoingMessage;
    /// # use ruststream::memory::prelude::*;
    /// # use serde::{Deserialize, Serialize};
    /// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
    /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
    /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
    /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
    /// use ruststream_sqlx::{Outbox, outbox};
    ///
    /// #[derive(Outbox, sqlx::FromRow)]
    /// #[outbox(table = "outbox", custom(retry))]
    /// pub struct OrderOutbox {
    ///     #[field(id)]
    ///     id: i64,
    ///     #[field(name)]
    ///     name: String,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    ///     #[field(processed_at)]
    ///     processed_at: Option<DateTime<Utc>>,
    /// }
    /// # impl outbox::Publish<Postgres> for OrderOutbox {
    /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    ///
    /// // The service counts how often a consumer asked for a record again.
    /// impl outbox::Retry<Postgres> for OrderOutbox {
    ///     async fn retry(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
    ///         sqlx::query("UPDATE outbox SET retries = retries + 1 WHERE id = $1")
    ///             .bind(id)
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .layer(tracking.layer())
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.include(fulfil);
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    Retry, retry, "retry",
    message = "`{Self}` names its own retry and does not implement `outbox::Retry<{DB}>`",
    note = "implement `outbox::Retry<{DB}>` for `{Self}`, or drop `retry` from `#[outbox(custom(..))]` (or `.own::<own::Retry>()` from its `OutboxSpec`)"
);

outcome_event!(
    /// Runs when the handler dropped the message.
    ///
    /// The default marks the record processed as [`Ack`] does: a dropped message is not sent
    /// again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
    /// # mod demo {
    /// # use chrono::{DateTime, Utc};
    /// # use ruststream::OutgoingMessage;
    /// # use ruststream::memory::prelude::*;
    /// # use serde::{Deserialize, Serialize};
    /// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
    /// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
    /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
    /// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
    /// use ruststream_sqlx::{Outbox, outbox};
    ///
    /// #[derive(Outbox, sqlx::FromRow)]
    /// #[outbox(table = "outbox", custom(discard))]
    /// pub struct OrderOutbox {
    ///     #[field(id)]
    ///     id: i64,
    ///     #[field(name)]
    ///     name: String,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    ///     #[field(processed_at)]
    ///     processed_at: Option<DateTime<Utc>>,
    /// }
    /// # impl outbox::Publish<Postgres> for OrderOutbox {
    /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    ///
    /// // A dropped record is marked apart from a processed one.
    /// impl outbox::Discard<Postgres> for OrderOutbox {
    ///     async fn discard(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
    ///         sqlx::query("UPDATE outbox SET processed_at = now(), dropped = true WHERE id = $1")
    ///             .bind(id)
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .layer(tracking.layer())
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.include(fulfil);
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    Discard, discard, "discard",
    message = "`{Self}` names its own discard and does not implement `outbox::Discard<{DB}>`",
    note = "implement `outbox::Discard<{DB}>` for `{Self}`, or drop `discard` from `#[outbox(custom(..))]` (or `.own::<own::Discard>()` from its `OutboxSpec`)"
);

/// Selects the unprocessed records of one name, for the republish at startup.
///
/// The default selects the records of `name` whose `processed_at` is `NULL`, or every record of
/// `name` when the struct has no `processed_at` field.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use chrono::{DateTime, Utc};
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream_sqlx::{Outbox, outbox};
///
/// #[derive(Outbox, sqlx::FromRow)]
/// #[outbox(table = "outbox", custom(recover))]
/// pub struct OrderOutbox {
///     #[field(id)]
///     id: i64,
///     #[field(name)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
///     #[field(processed_at)]
///     processed_at: Option<DateTime<Utc>>,
/// }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
///
/// // A record published in the last minute may still be in flight; the next startup takes it.
/// impl outbox::Recover<Postgres> for OrderOutbox {
///     async fn recover(conn: &mut PgConnection, name: &str) -> sqlx::Result<Vec<Self>> {
///         sqlx::query_as(
///             "SELECT id, name, payload, processed_at FROM outbox \
///              WHERE name = $1 AND processed_at IS NULL AND created_at < now() - interval '1 minute'",
///         )
///         .bind(name)
///         .fetch_all(conn)
///         .await
///     }
/// }
///
/// pub fn app(pool: PgPool) -> impl App {
///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(tracking.layer())
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///             b.after_startup(Publish, tracking.republish());
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` names its own recovery and does not implement `outbox::Recover<{DB}>`",
    label = "the service's own recovery is missing",
    note = "implement `outbox::Recover<{DB}>` for `{Self}`, or drop `recover` from `#[outbox(custom(..))]` (or `.own::<own::Recover>()` from its `OutboxSpec`)"
)]
pub trait Recover<DB: Database>: OutboxRow {
    /// The unprocessed records published under `name`.
    ///
    /// # Errors
    ///
    /// The database's error; startup fails with it.
    fn recover(
        conn: &mut DB::Connection,
        name: &str,
    ) -> impl Future<Output = Result<Vec<Self>, Error>> + Send;
}

/// Every event of a record type on `DB`, each dispatched by its slot in the record's settings:
/// what registering a record requires. Implemented for every [`OutboxTable`] that implements
/// [`Publish`] and every event its settings name as its own. Machinery.
// No message of its own: rustc then reports the unmet bound underneath (the missing `Publish`, the
// missing event of the record's own), with that trait's message.
#[doc(hidden)]
pub trait Tracked<DB: Database>: Publish<DB> {
    /// The record's headers, moved out of it; empty without a headers column.
    fn take_headers(&mut self) -> ruststream::HeaderMap;

    /// Takes the record `id` into work.
    fn fetch_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c Self::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Option<Self>, Error>> + Send + 'c;

    /// Marks the record `id` acknowledged.
    fn ack_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c Self::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Runs the retry of the record `id`.
    fn retry_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c Self::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// Marks the record `id` dropped.
    fn discard_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c Self::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;

    /// The unprocessed records published under `name`.
    fn recover_records<'c>(
        conn: &'c mut DB::Connection,
        name: &'c str,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Vec<Self>, Error>> + Send + 'c;
}

impl<DB, Record> Tracked<DB> for Record
where
    DB: Database,
    Record: OutboxTable + Publish<DB>,
    <Declared<Record> as Declaration>::Headers: HeadersOf<Record>,
    <Declared<Record> as Declaration>::OwnFetch: FetchBy<DB, Record>,
    <Declared<Record> as Declaration>::OwnAck: AckBy<DB, Record>,
    <Declared<Record> as Declaration>::OwnRetry: RetryBy<DB, Record>,
    <Declared<Record> as Declaration>::OwnDiscard: DiscardBy<DB, Record>,
    <Declared<Record> as Declaration>::OwnRecover: RecoverBy<DB, Record>,
{
    #[inline]
    fn take_headers(&mut self) -> ruststream::HeaderMap {
        <<Declared<Record> as Declaration>::Headers as HeadersOf<Record>>::take(self)
    }

    fn fetch_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Option<Self>, Error>> + Send + 'c {
        <<Declared<Record> as Declaration>::OwnFetch as FetchBy<DB, Record>>::fetch(
            conn, id, defaults,
        )
    }

    fn ack_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c {
        <<Declared<Record> as Declaration>::OwnAck as AckBy<DB, Record>>::ack(conn, id, defaults)
    }

    fn retry_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c {
        <<Declared<Record> as Declaration>::OwnRetry as RetryBy<DB, Record>>::retry(
            conn, id, defaults,
        )
    }

    fn discard_record<'c>(
        conn: &'c mut DB::Connection,
        id: &'c <Record as OutboxTable>::Id,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c {
        <<Declared<Record> as Declaration>::OwnDiscard as DiscardBy<DB, Record>>::discard(
            conn, id, defaults,
        )
    }

    fn recover_records<'c>(
        conn: &'c mut DB::Connection,
        name: &'c str,
        defaults: &'c Defaults,
    ) -> impl Future<Output = Result<Vec<Self>, Error>> + Send + 'c {
        <<Declared<Record> as Declaration>::OwnRecover as RecoverBy<DB, Record>>::recover(
            conn, name, defaults,
        )
    }
}
