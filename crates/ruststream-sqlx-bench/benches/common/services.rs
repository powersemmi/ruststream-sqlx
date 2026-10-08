//! The service half of every scenario: the handlers and apps a user writes, on the production
//! broker and the framework's own builder.
//!
//! A handler reads the run's [`Latch`] as the application state and counts its delivery down
//! after the decode, where the raw loop counts it too. The outbox scenarios run over
//! `MemoryBroker`, the way the crate's own outbox example runs: the outbox works over any broker,
//! and an in-process one keeps a network transport out of a number that is about the outbox.

use std::convert::Infallible;
use std::hint::black_box;
use std::num::NonZeroUsize;

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::Bound;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Outbox, OutboxTable};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, PgPool, Pool, Postgres, Sqlite};

use super::raw::{self, Sql};
use super::tables::{
    AdvisoryJob, LeaseJob, NamedJob, OUTBOX_INSERT, OrderRow, OutboxRecord, RowLockJob,
};
use super::{Latch, Order};

/// The name every inbox subscription reads by. The tables have no group column, so the name
/// addresses the whole table.
pub const JOBS: &str = "jobs";

/// One delivery of a payload table: decode, read both fields, count.
#[subscriber(InboxQueue::<RowLockJob>::new(JOBS))]
async fn row_lock(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// A batch of a payload table: every delivery read and counted, the batch settled as one.
#[subscriber(InboxQueue::<RowLockJob>::new(JOBS))]
async fn row_lock_batch(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<LeaseJob>::new(JOBS))]
async fn lease(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<LeaseJob>::new(JOBS))]
async fn lease_batch(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<AdvisoryJob>::new(JOBS))]
async fn advisory(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// A subscription by the name a route leads into [`NamedJob`]'s table.
#[subscriber("jobs")]
async fn by_name(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// Row mode: the handler takes the row the driver decoded.
#[subscriber(InboxQueue::<OrderRow>::new(JOBS))]
async fn row_mode(order: &OrderRow, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.customer.len(), order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// The service with the run's latch as its state, and `mount` on `broker`.
macro_rules! service {
    ($broker:expr, $latch:expr, |$b:ident| $mount:block) => {{
        let latch = $latch;
        RustStream::new(AppInfo::new("bench", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(latch))
            .with_broker($broker, |$b| $mount)
    }};
}

/// How a throughput run mounts its handler: one worker is the plain mount, which handles a
/// delivery before it pulls the next; more is `workers(n)`. A batch is `batch(n)`.
#[derive(Clone, Copy, Debug)]
pub struct Mount {
    pub workers: NonZeroUsize,
    pub batch: Option<NonZeroUsize>,
}

impl Mount {
    /// One delivery at a time, as a service mounts a handler by default.
    pub const SEQUENTIAL: Self = Self {
        workers: NonZeroUsize::MIN,
        batch: None,
    };
}

/// Mounts `$single` or `$batch` the way `$mount` says.
macro_rules! mount {
    ($b:ident, $mount:expr, $single:ident, $batch:ident) => {{
        let Mount { workers, batch } = $mount;
        match (batch, workers.get()) {
            (None, 1) => {
                $b.include($single);
            }
            (None, _) => {
                $b.include($single.workers(workers));
            }
            (Some(size), 1) => {
                $b.include($batch.batch(size));
            }
            (Some(size), _) => {
                $b.include($batch.batch(size).workers(workers));
            }
        }
    }};
}

/// The row lock form on Postgres.
pub fn postgres_row_lock(pool: PgPool, latch: Latch, how: Mount) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| {
        mount!(b, how, row_lock, row_lock_batch);
    })
}

/// The row lock form on MySQL.
pub fn mysql_row_lock(pool: Pool<MySql>, latch: Latch, how: Mount) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| {
        mount!(b, how, row_lock, row_lock_batch);
    })
}

/// The lease form on Postgres.
pub fn postgres_lease(pool: PgPool, latch: Latch, how: Mount) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| {
        mount!(b, how, lease, lease_batch);
    })
}

/// The lease form on SQLite, the form a SQLite table runs a claim in one statement with.
pub fn sqlite_lease(pool: Pool<Sqlite>, latch: Latch, how: Mount) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| {
        mount!(b, how, lease, lease_batch);
    })
}

/// The advisory lock form on Postgres.
pub fn postgres_advisory(pool: PgPool, latch: Latch) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| {
        b.include(advisory);
    })
}

/// A by-name subscription on Postgres: the route leads `jobs` into [`NamedJob`]'s table.
pub fn postgres_by_name(pool: PgPool, latch: Latch) -> impl App {
    service!(SqlxBroker::new(pool).route::<NamedJob>(JOBS), latch, |b| {
        b.include(by_name);
    })
}

/// Row mode in the row lock form on Postgres.
pub fn postgres_row_mode(pool: PgPool, latch: Latch) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| {
        b.include(row_mode);
    })
}

/// A service that publishes from outside a handler through a `Repository` of [`NamedJob`], and
/// the token its running form pairs the publisher from.
pub fn postgres_repository(
    pool: PgPool,
) -> (impl App, Bound<SqlxBroker<Postgres>, Repository<NamedJob>>) {
    let broker = SqlxBroker::new(pool).bindable();
    let egress = broker.bind(Repository::<NamedJob>::default());
    let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker, |_b| {});
    (app, egress)
}

/// The same through `Routed`: the message's name finds [`NamedJob`]'s table through a route.
pub fn postgres_routed(pool: PgPool) -> (impl App, Bound<SqlxBroker<Postgres>, Routed>) {
    let broker = SqlxBroker::new(pool)
        .route::<NamedJob>(super::ORDERS)
        .bindable();
    let egress = broker.bind(Routed);
    let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker, |_b| {});
    (app, egress)
}

/// What the outbox scenarios publish into the service: a command its relay answers.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "in")]
pub struct Command {
    pub id: i64,
}

/// The relay's answer, which the outbox tracks under its name.
#[derive(Debug, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "out")]
pub struct Reply {
    pub id: i64,
}

/// The name the outbox scenarios track.
pub const TRACKED: &str = "out";

#[subscriber("in", reply)]
async fn relay(cmd: &Command) -> Reply {
    Reply { id: cmd.id }
}

#[subscriber("out")]
async fn sink(reply: &Reply, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box(reply.id);
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// The outbox service over `MemoryBroker` with both middlewares: `relay` answers each command on
/// `in` with a reply on `out`, which `sink` consumes. The registry tracks `name`: [`TRACKED`]
/// records every reply, another name leaves every message untracked. The token publishes the
/// commands.
pub fn outbox(
    pool: PgPool,
    latch: Latch,
    name: &'static str,
) -> (impl App, Bound<MemoryBroker, MemoryPublish>) {
    let broker = MemoryBroker::new().bindable();
    let egress = broker.bind(MemoryPublish);
    let registry = Outbox::new(pool).register::<OutboxRecord>(name);
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(latch))
        .layer(registry.layer())
        .publish_layer(registry.publish_layer())
        .with_broker(broker, |b| {
            b.include(relay).out_reply(MemoryPublish);
            b.include(sink);
        });
    (app, egress)
}

/// The same service with no middleware.
pub fn outbox_bare(latch: Latch) -> (impl App, Bound<MemoryBroker, MemoryPublish>) {
    let broker = MemoryBroker::new().bindable();
    let egress = broker.bind(MemoryPublish);
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(latch))
        .with_broker(broker, |b| {
            b.include(relay).out_reply(MemoryPublish);
            b.include(sink);
        });
    (app, egress)
}

/// The outbox by hand: the relay writes the record with the record's own insert and answers with
/// its id, and the sink fetches the record and marks it, each on a connection of its own, as the
/// middleware takes them.
#[derive(Debug)]
pub struct ByHand {
    pub latch: Latch,
    pub pool: PgPool,
    pub fetch: Sql,
    pub mark: Sql,
}

/// What the relay inserts by hand: the reply the service half encodes for the same command.
const REPLY_BODY: &[u8] = br#"{"id":1}"#;

#[subscriber("in", reply)]
async fn relay_by_hand(_cmd: &Command, ctx: &mut Context<'_, (), ByHand>) -> Reply {
    let state = ctx.state();
    let mut conn = state
        .pool
        .acquire()
        .await
        .expect("the pool lends a connection");
    let id = sqlx::query_scalar(OUTBOX_INSERT)
        .bind(TRACKED)
        .bind(REPLY_BODY)
        .fetch_one(&mut *conn)
        .await
        .expect("the record is written");
    Reply { id }
}

#[subscriber("out")]
async fn sink_by_hand(reply: &Reply, ctx: &mut Context<'_, (), ByHand>) -> HandlerOutcome {
    let state = ctx.state();
    let record: OutboxRecord = {
        let mut conn = state
            .pool
            .acquire()
            .await
            .expect("the pool lends a connection");
        sqlx::query_as(state.fetch.text)
            .bind(reply.id)
            .fetch_one(&mut *conn)
            .await
            .expect("the record is unprocessed")
    };
    black_box(reply.id);
    state.latch.arrived();
    let mut conn = state
        .pool
        .acquire()
        .await
        .expect("the pool lends a connection");
    sqlx::query(state.mark.text)
        .bind(record.id)
        .execute(&mut *conn)
        .await
        .expect("the record is marked");
    HandlerOutcome::ack()
}

/// The outbox scenario by hand, on the same broker and the same two handlers' positions.
pub fn outbox_by_hand(
    pool: PgPool,
    latch: Latch,
) -> (impl App, Bound<MemoryBroker, MemoryPublish>) {
    let (fetch, mark) = raw::outbox(
        &ruststream_sqlx::dialect::Postgres,
        &OutboxRecord::TABLE.spec(),
    );
    let state = ByHand {
        latch,
        pool,
        fetch,
        mark,
    };
    let broker = MemoryBroker::new().bindable();
    let egress = broker.bind(MemoryPublish);
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker, |b| {
            b.include(relay_by_hand).out_reply(MemoryPublish);
            b.include(sink_by_hand);
        });
    (app, egress)
}
