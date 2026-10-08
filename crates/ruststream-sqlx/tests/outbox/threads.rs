//! The outbox under subscriptions on dedicated threads, run as a service: the record a handler's
//! publish writes and the mark of its delivery happen on a thread's runtime, and the connections
//! they open there serve the app's runtime and a restarted service once the threads are gone.

use std::convert::Infallible;

use ruststream::memory::prelude::*;
use ruststream::runtime::{FromRef, RunningApp};
use ruststream_sqlx::outbox::{Nil, Outbox, Registered};
use serde::{Deserialize, Serialize};

use crate::live::stands;
use crate::pool::{every_connection_answers, fresh_pool};
use crate::probe::{GUARD, Probe, until};
use crate::records::OrderRecord;
use crate::tracking_on;

#[derive(Debug, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "requests")]
struct Request {
    id: u32,
}

#[derive(Debug, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u32,
}

/// Where a suite's service runs its handlers on dedicated threads.
#[derive(Debug, Clone, Copy)]
enum Threads {
    /// The handler whose reply the outbox records.
    Recording,
    /// The handler of the tracked delivery the outbox marks.
    Marking,
}

stands! {
    use sqlx::Pool;

    type Tracking = Outbox<Db, Registered<OrderRecord, Nil>>;

    /// The service's state: what its handlers report, and the pool they empty.
    #[derive(Clone)]
    struct Kit {
        probe: Probe,
        pool: Pool<Db>,
    }

    impl FromRef<Self> for Kit {
        fn from_ref(kit: &Self) -> Self {
            kit.clone()
        }
    }

    impl Kit {
        /// Takes every connection of the pool once all are back, and keeps them: a connection
        /// the outbox needs next on this thread opens there.
        async fn take_every_connection(&self) {
            loop {
                while let Some(conn) = self.pool.try_acquire() {
                    self.probe.keep(conn);
                }
                if self.probe.kept() >= usize::try_from(self.pool.size()).expect("fits") {
                    return;
                }
                tokio::task::yield_now().await;
            }
        }
    }

    #[subscriber("requests", reply)]
    async fn place(request: &Request) -> Order {
        Order { id: request.id }
    }

    #[subscriber("requests", reply, threads(2))]
    async fn place_on_a_thread(request: &Request, State(kit): State<Kit>) -> Order {
        kit.take_every_connection().await;
        kit.probe.handled("requests", i64::from(request.id));
        Order { id: request.id }
    }

    #[subscriber("orders", threads(2))]
    async fn fulfil_on_a_thread(order: &Order, State(kit): State<Kit>) -> HandlerOutcome {
        kit.take_every_connection().await;
        kit.probe.handled("orders", i64::from(order.id));
        HandlerOutcome::ack()
    }

    async fn started(kit: &Kit, threads: Threads, id: u32) -> RunningApp {
        let tracking: Tracking = Outbox::new(kit.pool.clone()).register::<OrderRecord>("orders");
        let state = kit.clone();
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .layer(tracking.layer())
            .publish_layer(tracking.publish_layer())
            .with_broker(MemoryBroker::new(), move |b| {
                match threads {
                    Threads::Recording => {
                        b.include(place_on_a_thread).out_reply(Publish);
                    }
                    Threads::Marking => {
                        b.include(place).out_reply(Publish);
                        b.include(fulfil_on_a_thread);
                    }
                }
                b.after_startup(Publish, move |live| async move {
                    live.message(&Request { id }).publish().await
                });
            });
        tokio::time::timeout(GUARD, app.start())
            .await
            .expect("the service starts in time")
            .expect("the service starts")
    }

    /// The records of `outbox`, and how many of them are processed.
    async fn records(pool: &Pool<Db>) -> (i64, i64) {
        sqlx::query_as(
            "SELECT COUNT(*), COUNT(processed_at) FROM outbox",
        )
        .fetch_one(pool)
        .await
        .expect("the outbox reads")
    }

    /// The `run`th run of the service: one request, its order recorded and, where a handler
    /// takes it, processed.
    async fn run(db: &crate::live::Database<Db>, pool: &Pool<Db>, threads: Threads, run: u32) {
        let kit = Kit { probe: Probe::default(), pool: pool.clone() };
        let running = started(&kit, threads, run).await;
        kit.probe.reached(1).await;
        let runs = i64::from(run);
        let processed = match threads {
            Threads::Recording => 0,
            Threads::Marking => runs,
        };
        // The record is written or marked, and the connection that did it returns to the pool
        // while the threads still run.
        until("the outbox written and its connection returned", async || {
            pool.num_idle() >= 1 && records(&db.pool).await == (runs, processed)
        })
        .await;
        tokio::time::timeout(GUARD, running.shutdown())
            .await
            .expect("the service stops in time")
            .expect("the service stops");
        kit.probe.release();
    }

    /// Two runs of the service on one pool, each followed by a query on every connection.
    async fn outlived(threads: Threads) {
        if !tracking_on() {
            return;
        }
        let Some(db) = database().await else { return };
        let pool = fresh_pool(&db);
        run(&db, &pool, threads, 1).await;
        every_connection_answers(&pool, "after the first run").await;
        run(&db, &pool, threads, 2).await;
        every_connection_answers(&pool, "after the restart").await;
        pool.close().await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_record_written_on_a_thread_opens_its_connection_on_the_app_runtime() {
        outlived(Threads::Recording).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mark_on_a_thread_opens_its_connection_on_the_app_runtime() {
        outlived(Threads::Marking).await;
    }
}
