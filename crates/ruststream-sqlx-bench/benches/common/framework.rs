//! The framework loop of every inbox scenario: the handlers and apps a user writes, on the
//! production broker and the framework's own builder.
//!
//! A handler reads the run's [`Latch`] as the application state and counts its delivery down
//! after the decode, where the raw and the adapter loops count it too. The outbox's apps are in
//! [`outbox`](super::outbox).

use std::convert::Infallible;
use std::hint::black_box;
use std::num::NonZeroUsize;

use ruststream::runtime::{Bound, BrokerScope, Identity, MountChain, MountRoutes, PublishIdentity};
use ruststream_sqlx::prelude::*;
use sqlx::{MySql, PgPool, Pool, Postgres, Sqlite};

use super::tables::{AdvisoryJob, LeaseJob, NamedJob, OrderRow, ReplyJob, RowLockJob};
use super::{Confirmation, Latch, Order, REPLIES};

/// The name every inbox subscription reads by. The tables have no group column, so the name
/// addresses the whole table.
pub const JOBS: &str = "jobs";

/// One delivery of a payload table: decode, read both fields, count.
#[subscriber(InboxQueue::<RowLockJob>::new(JOBS))]
pub async fn row_lock(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

/// A delivery answered: the runtime encodes the reply and hands it to the connected broker's
/// default publisher, which inserts it where the reply type's name leads.
#[subscriber(InboxQueue::<RowLockJob>::new(JOBS), reply)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

/// A batch of a payload table: every delivery read and counted, the batch settled as one.
#[subscriber(InboxQueue::<RowLockJob>::new(JOBS))]
pub async fn row_lock_batch(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<LeaseJob>::new(JOBS))]
pub async fn lease(order: &Order, ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<LeaseJob>::new(JOBS))]
pub async fn lease_batch(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
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
    ($broker:expr, $latch:expr, $mount:expr) => {{
        let latch = $latch;
        RustStream::new(AppInfo::new("bench", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(latch))
            .with_broker($broker, $mount)
    }};
}

/// The scope a service of this module mounts its handlers on: the run's latch as its state.
pub type Scope<DB> = BrokerScope<SqlxBroker<DB>, Identity, (), Latch, PublishIdentity>;

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

/// Runs `$run` with `$mount` bound to the chain that mounts `$single` or `$batch` the way `$how`
/// says. Each way is a chain of its own type, so each runs `$run` on its own.
// Only the throughput run mounts by a value read at run time.
#[allow(unused_macros)]
macro_rules! mounted {
    ($how:expr, $single:path, $batch:path, |$mount:ident| $run:expr) => {{
        let $crate::common::framework::Mount { workers, batch } = $how;
        match (batch, workers.get()) {
            (None, 1) => {
                let $mount = |b: $crate::common::framework::Scope<_>| b.include($single);
                $run
            }
            (None, _) => {
                let $mount = move |b: $crate::common::framework::Scope<_>| {
                    b.include($single.workers(workers))
                };
                $run
            }
            (Some(size), 1) => {
                let $mount =
                    move |b: $crate::common::framework::Scope<_>| b.include($batch.batch(size));
                $run
            }
            (Some(size), _) => {
                let $mount = move |b: $crate::common::framework::Scope<_>| {
                    b.include($batch.batch(size).workers(workers))
                };
                $run
            }
        }
    }};
}
#[allow(unused_imports)]
pub(crate) use mounted;

/// The row lock form on Postgres, with what `mount` mounts.
pub fn postgres_row_lock<Mounting, Chain>(pool: PgPool, latch: Latch, mount: Mounting) -> impl App
where
    Mounting: FnOnce(Scope<Postgres>) -> Chain,
    Chain: MountChain<Routes: MountRoutes<SqlxBroker<Postgres>, Identity, Latch, PublishIdentity>>,
{
    service!(SqlxBroker::new(pool), latch, mount)
}

/// The row lock form on MySQL, with what `mount` mounts.
pub fn mysql_row_lock<Mounting, Chain>(pool: Pool<MySql>, latch: Latch, mount: Mounting) -> impl App
where
    Mounting: FnOnce(Scope<MySql>) -> Chain,
    Chain: MountChain<Routes: MountRoutes<SqlxBroker<MySql>, Identity, Latch, PublishIdentity>>,
{
    service!(SqlxBroker::new(pool), latch, mount)
}

/// The row lock form on Postgres, each delivery answered into [`ReplyJob`]'s table.
pub fn postgres_reply(pool: PgPool, latch: Latch) -> impl App {
    service!(
        SqlxBroker::new(pool).route::<ReplyJob>(REPLIES),
        latch,
        |b| b.include(confirm)
    )
}

/// The lease form on Postgres, with what `mount` mounts.
pub fn postgres_lease<Mounting, Chain>(pool: PgPool, latch: Latch, mount: Mounting) -> impl App
where
    Mounting: FnOnce(Scope<Postgres>) -> Chain,
    Chain: MountChain<Routes: MountRoutes<SqlxBroker<Postgres>, Identity, Latch, PublishIdentity>>,
{
    service!(SqlxBroker::new(pool), latch, mount)
}

/// The lease form on SQLite, the form a SQLite table runs a claim in one statement with, with
/// what `mount` mounts.
pub fn sqlite_lease<Mounting, Chain>(pool: Pool<Sqlite>, latch: Latch, mount: Mounting) -> impl App
where
    Mounting: FnOnce(Scope<Sqlite>) -> Chain,
    Chain: MountChain<Routes: MountRoutes<SqlxBroker<Sqlite>, Identity, Latch, PublishIdentity>>,
{
    service!(SqlxBroker::new(pool), latch, mount)
}

/// The advisory lock form on Postgres.
pub fn postgres_advisory(pool: PgPool, latch: Latch) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| b.include(advisory))
}

/// A by-name subscription on Postgres: the route leads `jobs` into [`NamedJob`]'s table.
pub fn postgres_by_name(pool: PgPool, latch: Latch) -> impl App {
    service!(SqlxBroker::new(pool).route::<NamedJob>(JOBS), latch, |b| b
        .include(by_name))
}

/// Row mode in the row lock form on Postgres.
pub fn postgres_row_mode(pool: PgPool, latch: Latch) -> impl App {
    service!(SqlxBroker::new(pool), latch, |b| b.include(row_mode))
}

/// A service that publishes from outside a handler through a `Repository` of [`NamedJob`], and
/// the token its running form pairs the publisher from.
pub fn postgres_repository(
    pool: PgPool,
) -> (impl App, Bound<SqlxBroker<Postgres>, Repository<NamedJob>>) {
    let broker = SqlxBroker::new(pool).bindable();
    let egress = broker.bind(Repository::<NamedJob>::default());
    let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker, |b| b);
    (app, egress)
}

/// The same through `Routed`: the message's name finds [`NamedJob`]'s table through a route.
pub fn postgres_routed(pool: PgPool) -> (impl App, Bound<SqlxBroker<Postgres>, Routed>) {
    let broker = SqlxBroker::new(pool)
        .route::<NamedJob>(super::ORDERS)
        .bindable();
    let egress = broker.bind(Routed);
    let app = RustStream::new(AppInfo::new("bench", "0.0.0")).with_broker(broker, |b| b);
    (app, egress)
}
