//! The registry: which record type tracks which name, and the pool every handle it gives out
//! shares.
//!
//! The registrations are a type-level list, `Registered<Record, Rest>` ending in [`Nil`], each
//! node holding its name and its record's default statements, built when the record type was
//! registered. A lookup walks the list comparing names, and every node is its own monomorphized
//! code: no map, no `dyn`. A name tracked by type adds a [`Checked`] node, which holds nothing and
//! forwards every call.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use ruststream::Publisher;
use sqlx::{Database, Pool};

use super::database::OutboxDatabase;
use super::error::PoolAlreadySet;
use super::layer::TrackingLayer;
use super::publish::TrackingPublishLayer;
use super::republish::Republishing;
use super::spec::OutboxTable;
use super::wrap::TrackedPublisher;
use crate::outbox::store::Store;

mod list;
mod tracked;

use list::defaults;
pub use list::{Nil, RecordList, RecordNames, Registered};
pub use tracked::{Checked, Lacks, TrackedName};

/// The outbox of a service: the record type of each name it tracks, and the pool their records
/// live in.
///
/// It hands out the subscription middleware ([`layer`](Self::layer)), the publish middleware
/// ([`publish_layer`](Self::publish_layer)), the startup republish
/// ([`republish`](Self::republish)) and [`wrap`](Self::wrap) for a publisher outside the
/// handlers. Every handle shares one pool: a pool set later, with [`set_pool`](Self::set_pool),
/// reaches the handles given out before.
///
/// A message whose name is not registered passes every handle after a comparison with each
/// registered name, and nothing else. A tracked message reads the pool with one atomic load, takes
/// a connection for its statements, and allocates the id header's value.
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
/// use std::convert::Infallible;
/// use std::io;
///
/// /// What `#[ruststream::app]` runs: the pool is built in `on_startup`, inside the runtime.
/// pub fn app() -> impl App {
///     let tracking = Outbox::<Postgres>::deferred().register::<OrderOutbox>("orders");
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
///         .after_shutdown(async move |pool| {
///             pool.close().await;
///             Ok::<_, Infallible>(())
///         })
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///             b.after_startup(Publish, tracking.republish());
///         })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct Outbox<DB: Database, Records = Nil> {
    store: Arc<Store<DB>>,
    records: Records,
}

impl<DB: Database, Records: Copy> Clone for Outbox<DB, Records> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            records: self.records,
        }
    }
}

impl<DB: Database, Records: fmt::Debug> fmt::Debug for Outbox<DB, Records> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox")
            .field("records", &self.records)
            .field("pool_set", &self.store.get().is_some())
            .finish()
    }
}

impl<DB: Database> Outbox<DB, Nil> {
    /// An outbox with no names yet, whose records live in `pool`.
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
    /// /// An app built inside a running Tokio runtime, where the pool exists already.
    /// pub async fn run(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let tracking = Outbox::new(pool).register::<OrderOutbox>("orders");
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .layer(tracking.layer())
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.include(fulfil);
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    ///         .run()
    ///         .await?;
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn new(pool: Pool<DB>) -> Self {
        Self {
            store: Arc::new(Store::from(pool)),
            records: Nil,
        }
    }

    /// An outbox with no names and no pool yet: the service gives it the pool with
    /// [`set_pool`](Self::set_pool), usually in `on_startup`, where the pool is built.
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
    /// pub fn app() -> impl App {
    ///     let tracking = Outbox::<Postgres>::deferred().register::<OrderOutbox>("orders");
    ///     let registry = tracking.clone();
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .on_startup(async move |()| {
    ///             let pool = PgPool::connect("postgres://localhost/orders")
    ///                 .await
    ///                 .map_err(io::Error::other)?;
    ///             registry.set_pool(pool.clone()).map_err(io::Error::other)?;
    ///             Ok::<_, io::Error>(pool)
    ///         })
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn deferred() -> Self {
        Self {
            store: Arc::new(Store::default()),
            records: Nil,
        }
    }
}

impl<DB: Database, Records: RecordNames> Outbox<DB, Records> {
    /// Tracks the messages published under `name` with the record type `Record`, for a name known
    /// only at run time; [`track`](Self::track) refuses a repeated name while the service
    /// compiles. The record's default statements are built here, once.
    ///
    /// # Panics
    ///
    /// Panics when `name` is registered already (each name has one record type), and when
    /// `Record`'s table does not fit a dialect of `DB` (an identifier longer than the database
    /// takes).
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
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "refunds")] pub struct RefundRequested { id: u64 }
    /// # #[subscriber("cancellations", reply)] async fn cancel(cmd: &PlaceOrder) -> RefundRequested { RefundRequested { id: cmd.id } }
    /// /// One record type tracks both names; each name republishes its own records.
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = Outbox::new(pool)
    ///         .register::<OrderOutbox>("orders")
    ///         .register::<OrderOutbox>("refunds");
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.include(cancel).out_reply(Publish);
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    #[track_caller]
    pub fn register<Record: OutboxTable>(
        self,
        name: &'static str,
    ) -> Outbox<DB, Registered<Record, Records>>
    where
        DB: OutboxDatabase,
        // On the node rather than on `Record`: rustc then reports the unmet bound underneath (a
        // missing `Publish`, an event of the record's own without its impl) with its own message.
        Registered<Record, Records>: RecordList<DB>,
    {
        // Why at run time: the names are run-time strings here; `track` and `outbox!` reject a
        // repeated name at compile time.
        assert!(
            !self.records.contains(name),
            "`{name}` is registered with the outbox twice; register each name once"
        );
        Outbox {
            store: self.store,
            records: Registered {
                name,
                defaults: defaults::<DB, Record>(),
                rest: self.records,
                record: PhantomData,
            },
        }
    }

    /// Tracks the messages published under `Name::NAME` with the record type `Record`. A name
    /// tracked twice does not compile. The record's default statements are built here, once.
    ///
    /// # Panics
    ///
    /// Panics when the name is registered already through [`register`](Self::register), whose
    /// names the compiler does not see, and when `Record`'s table does not fit a dialect of `DB`
    /// (an identifier longer than the database takes).
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
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "refunds")] pub struct OrderCancelled { id: u64 }
    /// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
    /// # #[subscriber("cancel", reply)] async fn cancel(cmd: &PlaceOrder) -> OrderCancelled { OrderCancelled { id: cmd.id } }
    /// use ruststream_sqlx::outbox::TrackedName;
    ///
    /// pub struct Orders;
    ///
    /// impl TrackedName for Orders {
    ///     const NAME: &'static str = "orders";
    /// }
    ///
    /// pub struct Refunds;
    ///
    /// impl TrackedName for Refunds {
    ///     const NAME: &'static str = "refunds";
    /// }
    ///
    /// /// One record type tracks both names; a third `track::<OrderOutbox, Orders>()` would not
    /// /// compile.
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = Outbox::new(pool)
    ///         .track::<OrderOutbox, Orders>()
    ///         .track::<OrderOutbox, Refunds>();
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.include(cancel).out_reply(Publish);
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    ///
    /// A name tracked twice stops the build:
    ///
    /// ```compile_fail,E0080
    /// # use ruststream::OutgoingMessage;
    /// # use ruststream_sqlx::{Outbox, outbox};
    /// # use sqlx::{Sqlite, SqliteConnection};
    /// # #[derive(Outbox, sqlx::FromRow)]
    /// # #[outbox(table = "outbox")]
    /// # pub struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
    /// # impl outbox::Publish<Sqlite> for OrderOutbox {
    /// #     async fn publish(conn: &mut SqliteConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    /// use ruststream_sqlx::outbox::TrackedName;
    ///
    /// pub struct Orders;
    ///
    /// impl TrackedName for Orders {
    ///     const NAME: &'static str = "orders";
    /// }
    ///
    /// pub struct OrdersAgain;
    ///
    /// impl TrackedName for OrdersAgain {
    ///     const NAME: &'static str = "orders";
    /// }
    ///
    /// fn main() {
    ///     let _ = Outbox::<Sqlite>::deferred()
    ///         .track::<OrderOutbox, Orders>()
    ///         .track::<OrderOutbox, OrdersAgain>();
    /// }
    /// ```
    #[must_use]
    #[track_caller]
    pub fn track<Record: OutboxTable, Name: TrackedName>(
        self,
    ) -> Outbox<DB, Registered<Record, Checked<Name, Records>>>
    where
        DB: OutboxDatabase,
        Records: Lacks<Name>,
        Registered<Record, Checked<Name, Records>>: RecordList<DB>,
    {
        // The message cannot carry the name: a `const` panic formats only literals. The build
        // names the call that repeats it, `track::<Record, Name>`.
        const {
            assert!(
                <Records as Lacks<Name>>::LACKS,
                "a name is tracked with the outbox twice: track each name once"
            );
        }
        // Why at run time: a name registered through `register` is a run-time string.
        assert!(
            !self.records.contains(Name::NAME),
            "`{}` is registered with the outbox twice; register each name once",
            Name::NAME
        );
        Outbox {
            store: self.store,
            records: Registered {
                name: Name::NAME,
                defaults: defaults::<DB, Record>(),
                rest: Checked(self.records, PhantomData),
                record: PhantomData,
            },
        }
    }

    /// Gives the outbox its pool, when it was made with [`deferred`](Outbox::deferred).
    ///
    /// # Errors
    ///
    /// [`PoolAlreadySet`] when the outbox has a pool already; the one it has stays.
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
    /// pub fn app() -> impl App {
    ///     let tracking = outbox! { "orders" => OrderOutbox };
    ///     let registry = tracking.clone();
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .on_startup(async move |()| {
    ///             let pool = PgPool::connect("postgres://localhost/orders")
    ///                 .await
    ///                 .map_err(io::Error::other)?;
    ///             // Every handle the registry gave out reads this pool from now on.
    ///             registry.set_pool(pool.clone()).map_err(io::Error::other)?;
    ///             Ok::<_, io::Error>(pool)
    ///         })
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn set_pool(&self, pool: Pool<DB>) -> Result<(), PoolAlreadySet> {
        // Why at run time: a pool is built asynchronously, often in `on_startup`, after the
        // registry and its handles were made.
        self.store.set(pool).map_err(|_| PoolAlreadySet)
    }
}

impl<DB: Database, Records: RecordList<DB>> Outbox<DB, Records> {
    /// The subscription middleware: a delivery under a registered name that carries an id takes
    /// its record before the handler runs, and settles it by the handler's outcome.
    ///
    /// Mounted with `RustStream::layer`.
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
    /// /// The consumer side: `fulfil` runs once per record, and its acknowledgement marks the record.
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .layer(tracking.layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(fulfil);
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn layer(&self) -> TrackingLayer<DB, Records> {
        TrackingLayer::new(Arc::clone(&self.store), self.records)
    }

    /// The publish middleware: a message a handler publishes under a registered name is recorded
    /// before it is sent, and carries its record's id.
    ///
    /// Mounted with `RustStream::publish_layer`.
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
    /// /// The producer side: the reply of `place` is recorded, then sent with its record's id.
    /// pub fn app(pool: PgPool) -> impl App {
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
    #[must_use]
    pub fn publish_layer(&self) -> TrackingPublishLayer<DB, Records> {
        TrackingPublishLayer::new(Arc::clone(&self.store), self.records)
    }

    /// The startup republish of every registered name: an `after_startup` hook that publishes
    /// each unprocessed record again through the scope's publisher, with its id.
    ///
    /// The hook allocates its body once, at startup.
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
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             // Runs once the subscriptions are open; a record it cannot send fails startup.
    ///             b.after_startup(Publish, tracking.republish());
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn republish<Live: Publisher + 'static>(
        &self,
    ) -> impl FnOnce(Live) -> Republishing + Send + 'static {
        self.republishing(None)
    }

    /// The startup republish of the registered names `names` alone; see
    /// [`republish`](Self::republish).
    ///
    /// # Panics
    ///
    /// Panics when a name of `names` is not registered.
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
    /// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "audits")] pub struct Audited { id: u64 }
    /// # #[subscriber("checkout-audit", reply)] async fn audit(cmd: &PlaceOrder) -> Audited { Audited { id: cmd.id } }
    /// /// Each broker republishes the names it carries.
    /// pub fn app(pool: PgPool) -> impl App {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox, "audits" => OrderOutbox };
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .publish_layer(tracking.publish_layer())
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(place).out_reply(Publish);
    ///             b.after_startup(Publish, tracking.republish_names(["orders"]));
    ///         })
    ///         .with_broker(MemoryBroker::new(), |b| {
    ///             b.include(audit).out_reply(Publish);
    ///             b.after_startup(Publish, tracking.republish_names(["audits"]));
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[track_caller]
    pub fn republish_names<Live: Publisher + 'static>(
        &self,
        names: impl IntoIterator<Item = &'static str>,
    ) -> impl FnOnce(Live) -> Republishing + Send + 'static {
        let names: Vec<&'static str> = names.into_iter().collect();
        for name in &names {
            assert!(
                self.records.contains(name),
                "`{name}` is not registered with the outbox, so it has nothing to republish"
            );
        }
        self.republishing(Some(names))
    }

    fn republishing<Live: Publisher + 'static>(
        &self,
        only: Option<Vec<&'static str>>,
    ) -> impl FnOnce(Live) -> Republishing + Send + 'static {
        let pool = Arc::clone(&self.store);
        let records = self.records;
        move |publisher: Live| {
            Republishing::new(
                async move { records.republish(&pool, &publisher, only.as_deref()).await },
            )
        }
    }

    /// `publisher` with the publish middleware's tracking, for publishes outside the handlers
    /// (an HTTP endpoint, an `after_startup` hook), which the publish pipeline does not reach.
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
    /// pub async fn serve(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let tracking = outbox! { pool: pool, "orders" => OrderOutbox };
    ///     let broker = MemoryBroker::new().bindable();
    ///     let egress = broker.bind(Publish);
    ///     let running = RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(broker, |b| {
    ///             b.include(fulfil);
    ///         })
    ///         .start()
    ///         .await?;
    ///
    ///     // What an HTTP endpoint publishes: recorded first, then sent with its record's id.
    ///     let publisher = tracking.wrap(running.publisher(egress).await?);
    ///     publisher.message(&OrderPlaced { id: 7 }).publish().await?;
    ///
    ///     running.shutdown().await?;
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn wrap<Live: Publisher>(&self, publisher: Live) -> TrackedPublisher<Live, DB, Records> {
        TrackedPublisher::new(publisher, Arc::clone(&self.store), self.records)
    }
}
