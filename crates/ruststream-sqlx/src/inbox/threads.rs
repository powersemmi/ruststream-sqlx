//! A subscription on dedicated threads with pools of their own: how many threads, whether a
//! partition key keeps to one, and how many connections each thread's pool opens.

use std::num::{NonZeroU32, NonZeroUsize};

use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings, Workers};

use super::queue::InboxQueue;

/// The dedicated threads of an `InboxQueue` subscription, and the connections each one opens for
/// its handlers' queries: what [`InboxSettings::on_threads`](crate::InboxSettings::on_threads)
/// mounts.
///
/// A handler on one of the threads queries through `Ctx<keys::Pool<..>>` on a pool of the
/// thread's own, with the service pool's options and [`connections`](Self::connections)
/// connections at most, one unless set. Its connections open on the thread and close with it.
/// Knowing both numbers, the subscription counts `threads × connections` against the broker's
/// [`connection_limit`](crate::SqlxBroker::connection_limit) when it opens.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::nonzero;
/// use ruststream_sqlx::prelude::*;
/// use sqlx::{PgPool, Postgres};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "ledger_jobs")]
/// # pub struct LedgerJob { #[field(id)] id: i64, #[field(partition_key)] account: String, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Entry { amount: i64 }
///
/// #[subscriber(InboxQueue::<LedgerJob>::new("entries"))]
/// async fn post(entry: &Entry, Ctx(pool): Ctx<keys::Pool<Postgres>>) -> HandlerOutcome {
///     let posted = sqlx::query("INSERT INTO postings (amount) VALUES ($1)")
///         .bind(entry.amount)
///         .execute(&pool)
///         .await;
///     if posted.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     // Eight threads, an account's entries on one of them, one connection each.
///     let threads = InboxThreads::new(nonzero!(8)).by_key();
///     RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(post.on_threads(threads));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct InboxThreads {
    count: NonZeroUsize,
    connections: NonZeroU32,
    by_key: bool,
}

impl InboxThreads {
    /// `count` dedicated threads, as `threads(count)`, each opening one connection.
    pub const fn new(count: NonZeroUsize) -> Self {
        Self {
            count,
            connections: NonZeroU32::MIN,
            by_key: false,
        }
    }

    /// Sends the deliveries of one partition key to one thread, as `threads(count, by_key)`, so
    /// a key keeps its order.
    pub const fn by_key(mut self) -> Self {
        self.by_key = true;
        self
    }

    /// Opens `connections` connections at most on each thread, for its handlers' queries.
    ///
    /// A thread runs its deliveries one at a time, so one connection serves a handler that holds
    /// one connection at a time. A handler that holds two at once, or queries from tasks it
    /// spawns on its thread, needs one more for each.
    pub const fn connections(mut self, connections: NonZeroU32) -> Self {
        self.connections = connections;
        self
    }

    /// The connections the threads open at most together: threads times connections each.
    pub(crate) fn total_connections(&self) -> u64 {
        u64::try_from(self.count.get())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::from(self.connections.get()))
    }

    /// The connections each thread's pool opens at most.
    pub(crate) const fn connections_each(&self) -> NonZeroU32 {
        self.connections
    }

    /// The dispatch the core runs the threads with.
    pub(crate) const fn workers(&self) -> Workers {
        if self.by_key {
            Workers::threads_keyed(self.count)
        } else {
            Workers::threads(self.count)
        }
    }
}

/// Recording a subscription's dedicated threads on its descriptor. Machinery behind
/// [`InboxSettings::on_threads`](crate::InboxSettings::on_threads); never named in a service.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.on_threads(..)` runs an `InboxQueue` subscription on threads with pools of their own",
    label = "this registration does not subscribe through `InboxQueue`",
    note = "a subscription by name takes the framework's `threads(n)`: subscribe with \
            `InboxQueue::<Row>::new(..)` for threads with pools of their own"
)]
pub trait ThreadsStep {
    /// The registration with its threads recorded.
    type Out;

    /// Records `threads` on the registration's descriptor.
    fn apply_threads(self, threads: InboxThreads) -> Self::Out;
}

impl<Def, Row, Mode, State, DefCodec> ThreadsStep
    for SubscriberBuilder<Def, InboxQueue<Row, Mode>, State, DefCodec>
where
    Def: Declared,
{
    type Out = Self;

    fn apply_threads(self, threads: InboxThreads) -> Self {
        self.map_source(move |queue| queue.on_threads(threads))
    }
}
