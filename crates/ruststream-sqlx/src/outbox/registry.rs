//! The registry: which record type tracks which name, and the pool every handle it gives out
//! shares.
//!
//! The registrations are a type-level list, `Registered<Record, Rest>` ending in [`Nil`], each
//! node holding its name. A lookup walks the list comparing names, and every node is its own
//! monomorphized code: no map, no `dyn`.

use std::any::type_name;
use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use ruststream::runtime::{Context, Handler, HandlerOutcome};
use ruststream::{OutgoingMessage, Publisher};
use sqlx::{Database, Pool};

use super::error::{OutboxError, PoolAlreadySet};
use super::events::Tracked;
use super::layer::{TrackingLayer, deliver};
use super::publish::{TrackingPublishLayer, record};
use super::republish::{Republishing, recover_and_publish};
use super::wrap::TrackedPublisher;

/// The end of the registrations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Nil;

/// A name the outbox tracks with `Record`, in front of the registrations `Rest`.
pub struct Registered<Record, Rest> {
    name: &'static str,
    rest: Rest,
    record: PhantomData<fn() -> Record>,
}

impl<Record, Rest: Clone> Clone for Registered<Record, Rest> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            rest: self.rest.clone(),
            record: PhantomData,
        }
    }
}

impl<Record, Rest: Copy> Copy for Registered<Record, Rest> {}

impl<Record, Rest: fmt::Debug> fmt::Debug for Registered<Record, Rest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registered")
            .field("name", &self.name)
            .field("record", &type_name::<Record>())
            .field("rest", &self.rest)
            .finish()
    }
}

/// The names of a registry, whatever their record types.
#[doc(hidden)]
pub trait RecordNames: Copy + Send + Sync + 'static {
    /// Whether `name` is registered.
    fn contains(&self, name: &str) -> bool;
}

impl RecordNames for Nil {
    #[inline]
    fn contains(&self, _name: &str) -> bool {
        false
    }
}

impl<Record: 'static, Rest: RecordNames> RecordNames for Registered<Record, Rest> {
    #[inline]
    fn contains(&self, name: &str) -> bool {
        self.name == name || self.rest.contains(name)
    }
}

/// What the registry does for the record type of a name; each method finds the name's node and
/// runs the record's events there.
#[doc(hidden)]
pub trait RecordList<DB: Database>: RecordNames {
    /// Records `msg` under its name; `None` when the name is not registered, else the id header's
    /// value.
    fn record<'a>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        msg: &'a OutgoingMessage<'_>,
    ) -> impl Future<Output = Option<Result<String, OutboxError>>> + Send + 'a;

    /// Runs `handler` on a delivery under a registered name, taking and settling its record.
    fn deliver<'a, M, C, S, H>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        handler: &'a H,
        msg: &'a M,
        ctx: &'a mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send + 'a
    where
        M: Sync,
        C: Send,
        S: Send + Sync,
        H: Handler<M, C, S>;

    /// Publishes the unprocessed records of every name `only` lists (every name when `None`)
    /// through `publisher`.
    fn republish<'a, Live: Publisher>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        publisher: &'a Live,
        only: Option<&'a [&'static str]>,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send + 'a;
}

impl<DB: Database> RecordList<DB> for Nil {
    fn record<'a>(
        &'a self,
        _pool: &'a OnceLock<Pool<DB>>,
        _msg: &'a OutgoingMessage<'_>,
    ) -> impl Future<Output = Option<Result<String, OutboxError>>> + Send + 'a {
        ready(None)
    }

    fn deliver<'a, M, C, S, H>(
        &'a self,
        _pool: &'a OnceLock<Pool<DB>>,
        handler: &'a H,
        msg: &'a M,
        ctx: &'a mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send + 'a
    where
        M: Sync,
        C: Send,
        S: Send + Sync,
        H: Handler<M, C, S>,
    {
        handler.handle(msg, ctx)
    }

    fn republish<'a, Live: Publisher>(
        &'a self,
        _pool: &'a OnceLock<Pool<DB>>,
        _publisher: &'a Live,
        _only: Option<&'a [&'static str]>,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send + 'a {
        ready(Ok(()))
    }
}

impl<DB, Record, Rest> RecordList<DB> for Registered<Record, Rest>
where
    DB: Database,
    Record: Tracked<DB>,
    Rest: RecordList<DB>,
{
    async fn record<'a>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        msg: &'a OutgoingMessage<'_>,
    ) -> Option<Result<String, OutboxError>> {
        if msg.name() == self.name {
            Some(record::<DB, Record>(self.name, pool, msg).await)
        } else {
            self.rest.record(pool, msg).await
        }
    }

    async fn deliver<'a, M, C, S, H>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        handler: &'a H,
        msg: &'a M,
        ctx: &'a mut Context<'_, C, S>,
    ) -> HandlerOutcome
    where
        M: Sync,
        C: Send,
        S: Send + Sync,
        H: Handler<M, C, S>,
    {
        if ctx.name() == self.name {
            deliver::<DB, Record, M, C, S, H>(self.name, pool, handler, msg, ctx).await
        } else {
            self.rest.deliver(pool, handler, msg, ctx).await
        }
    }

    async fn republish<'a, Live: Publisher>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        publisher: &'a Live,
        only: Option<&'a [&'static str]>,
    ) -> Result<(), OutboxError> {
        if only.is_none_or(|names| names.contains(&self.name)) {
            recover_and_publish::<DB, Record, Live>(self.name, pool, publisher).await?;
        }
        self.rest.republish(pool, publisher, only).await
    }
}

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
pub struct Outbox<DB: Database, Records = Nil> {
    pool: Arc<OnceLock<Pool<DB>>>,
    records: Records,
}

impl<DB: Database, Records: Copy> Clone for Outbox<DB, Records> {
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            records: self.records,
        }
    }
}

impl<DB: Database, Records: fmt::Debug> fmt::Debug for Outbox<DB, Records> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox")
            .field("records", &self.records)
            .field("pool_set", &self.pool.get().is_some())
            .finish()
    }
}

impl<DB: Database> Outbox<DB, Nil> {
    /// An outbox with no names yet, whose records live in `pool`.
    #[must_use]
    pub fn new(pool: Pool<DB>) -> Self {
        Self {
            pool: Arc::new(OnceLock::from(pool)),
            records: Nil,
        }
    }

    /// An outbox with no names and no pool yet: the service gives it the pool with
    /// [`set_pool`](Self::set_pool), usually in `on_startup`, where the pool is built.
    #[must_use]
    pub fn deferred() -> Self {
        Self {
            pool: Arc::new(OnceLock::new()),
            records: Nil,
        }
    }
}

impl<DB: Database, Records: RecordNames> Outbox<DB, Records> {
    /// Tracks the messages published under `name` with the record type `Record`.
    ///
    /// # Panics
    ///
    /// Panics when `name` is registered already: each name has one record type.
    #[must_use]
    #[track_caller]
    pub fn register<Record: Tracked<DB>>(
        self,
        name: &'static str,
    ) -> Outbox<DB, Registered<Record, Records>> {
        // Why at run time: the names are run-time strings here; `outbox!` rejects a repeated
        // literal at compile time.
        assert!(
            !self.records.contains(name),
            "`{name}` is registered with the outbox twice; register each name once"
        );
        Outbox {
            pool: self.pool,
            records: Registered {
                name,
                rest: self.records,
                record: PhantomData,
            },
        }
    }

    /// Gives the outbox its pool, when it was made with [`deferred`](Outbox::deferred).
    ///
    /// # Errors
    ///
    /// [`PoolAlreadySet`] when the outbox has a pool already; the one it has stays.
    pub fn set_pool(&self, pool: Pool<DB>) -> Result<(), PoolAlreadySet> {
        // Why at run time: a pool is built asynchronously, often in `on_startup`, after the
        // registry and its handles were made.
        self.pool.set(pool).map_err(|_| PoolAlreadySet)
    }
}

impl<DB: Database, Records: RecordList<DB>> Outbox<DB, Records> {
    /// The subscription middleware: a delivery under a registered name that carries an id takes
    /// its record before the handler runs, and settles it by the handler's outcome.
    ///
    /// Mounted with `RustStream::layer`.
    #[must_use]
    pub fn layer(&self) -> TrackingLayer<DB, Records> {
        TrackingLayer::new(Arc::clone(&self.pool), self.records)
    }

    /// The publish middleware: a message a handler publishes under a registered name is recorded
    /// before it is sent, and carries its record's id.
    ///
    /// Mounted with `RustStream::publish_layer`.
    #[must_use]
    pub fn publish_layer(&self) -> TrackingPublishLayer<DB, Records> {
        TrackingPublishLayer::new(Arc::clone(&self.pool), self.records)
    }

    /// The startup republish of every registered name: an `after_startup` hook that publishes
    /// each unprocessed record again through the scope's publisher, with its id.
    ///
    /// The hook allocates its body once, at startup.
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
        let pool = Arc::clone(&self.pool);
        let records = self.records;
        move |publisher: Live| {
            Republishing::new(
                async move { records.republish(&pool, &publisher, only.as_deref()).await },
            )
        }
    }

    /// `publisher` with the publish middleware's tracking, for publishes outside the handlers
    /// (an HTTP endpoint, an `after_startup` hook), which the publish pipeline does not reach.
    #[must_use]
    pub fn wrap<Live: Publisher>(&self, publisher: Live) -> TrackedPublisher<Live, DB, Records> {
        TrackedPublisher::new(publisher, Arc::clone(&self.pool), self.records)
    }
}

#[cfg(test)]
mod tests {
    use super::{Nil, PhantomData, RecordNames, Registered};

    fn node<Rest>(name: &'static str, rest: Rest) -> Registered<(), Rest> {
        Registered {
            name,
            rest,
            record: PhantomData,
        }
    }

    #[test]
    fn a_lookup_finds_every_registered_name_and_nothing_else() {
        let names = node("orders", node("refunds", node("audits", Nil)));
        assert!(names.contains("orders"));
        assert!(names.contains("refunds"));
        assert!(names.contains("audits"));
        assert!(!names.contains("notes"));
        assert!(!names.contains("order"));
        assert!(!names.contains(""));
        assert!(!Nil.contains("orders"));
    }
}
