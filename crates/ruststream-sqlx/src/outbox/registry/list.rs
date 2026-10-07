//! The type-level list of registrations: each node holds its name and its record's default
//! statements, and a lookup walks the list comparing names.

use std::any::type_name;
use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::OnceLock;

use ruststream::runtime::{Context, Handler, HandlerOutcome};
use ruststream::{Bytes, OutgoingMessage, Publisher};
use sqlx::{Database, Pool};

use crate::outbox::database::{Defaults, OutboxDatabase};
use crate::outbox::error::OutboxError;
use crate::outbox::events::Tracked;
use crate::outbox::layer::deliver;
use crate::outbox::publish::record;
use crate::outbox::republish::recover_and_publish;
use crate::outbox::spec::{Described, OutboxTable};

/// The end of the registrations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Nil;

/// A name the outbox tracks with `Record`, in front of the registrations `Rest`.
pub struct Registered<Record, Rest> {
    pub(super) name: &'static str,
    pub(super) defaults: Defaults,
    pub(super) rest: Rest,
    pub(super) record: PhantomData<fn() -> Record>,
}

impl<Record, Rest: Clone> Clone for Registered<Record, Rest> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            defaults: self.defaults,
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
            .field("defaults", &self.defaults)
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
    ) -> impl Future<Output = Option<Result<Bytes, OutboxError>>> + Send + 'a;

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
    ) -> impl Future<Output = Option<Result<Bytes, OutboxError>>> + Send + 'a {
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
    ) -> Option<Result<Bytes, OutboxError>> {
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
            deliver::<DB, Record, M, C, S, H>(self.name, &self.defaults, pool, handler, msg, ctx)
                .await
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
            recover_and_publish::<DB, Record, Live>(self.name, &self.defaults, pool, publisher)
                .await?;
        }
        self.rest.republish(pool, publisher, only).await
    }
}

/// The default statements of `Record` on `DB`, built from its table's description.
///
/// # Panics
///
/// Panics when the table does not fit a dialect of `DB`.
#[track_caller]
pub(super) fn defaults<DB: OutboxDatabase, Record: OutboxTable>() -> Defaults {
    // Why at run time: the dialects write their statements at run time; the derive checks the
    // same description while the service compiles.
    DB::defaults(&Record::TABLE.spec()).unwrap_or_else(|error| {
        panic!(
            "`{}` describes an outbox table its database cannot run: {error}",
            type_name::<Record>()
        )
    })
}

#[cfg(test)]
pub(super) mod tests {
    use super::{Defaults, Nil, PhantomData, RecordNames, Registered};

    pub(in crate::outbox::registry) fn node<Rest>(
        name: &'static str,
        rest: Rest,
    ) -> Registered<(), Rest> {
        Registered {
            name,
            defaults: Defaults::default(),
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
