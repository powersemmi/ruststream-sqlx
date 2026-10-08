//! How a code-cost scenario runs under valgrind: two measured regions, the start and the drain.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`](super::MESSAGES) deliveries
//! and over twice as many. The slope between the last two is the steady-state cost of a message:
//! everything that happens once is in both totals and cancels in the subtraction. The
//! one-delivery run is the cold start, reported on its own: the pool's first connection, the
//! broker's startup checks and the first delivery.
//!
//! The first region is the start: the service's `start()`, or the raw loop's first connection.
//! Between the two regions a producer on a thread and a runtime of its own fills the table and
//! waits for its insert to commit. The service's runtime is current-thread, so it does not run
//! while the producer works: nothing is claimed before the drain region opens, and the fill is in
//! neither region. The second region is the drain: the service's runtime driven until the latch
//! counts the last delivery, or the raw loop run until its claim comes back empty.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which both regions run
//! inside. Everything the service's thread runs there is counted: the dispatcher, the codec, this
//! crate's code, and sqlx's driver encoding, sending, receiving and decoding on that thread. The
//! database server is another process and is not counted, and neither is the kernel's side of a
//! system call. DHAT is pointed at the same frame; the number read is `Total blocks`, allocations
//! per run.

use std::thread;

use futures::future::join_all;

use ruststream::runtime::{App, RunningApp};
use sqlx::PgPool;
use tokio::runtime::{Builder, Runtime};

use super::stand::{Table, postgres_fill, postgres_pool, postgres_table};
use super::{Latch, measure};

/// What filling the table takes: a raw pool on a runtime of its own, whose `block_on` runs on a
/// thread of the producer's own.
struct Producer {
    runtime: Runtime,
    pool: PgPool,
}

impl Producer {
    /// Recreates `table` empty.
    fn prepare(table: Table) -> Self {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("the producer's runtime");
        let pool = runtime.block_on(async {
            let pool = postgres_pool(1);
            postgres_table(&pool, table).await;
            pool
        });
        Self { runtime, pool }
    }

    /// Fills `table` with `rows` rows from a thread of its own, and returns once they committed.
    fn fill(&self, table: Table, rows: usize) {
        thread::scope(|scope| {
            scope
                .spawn(|| {
                    self.runtime
                        .block_on(postgres_fill(&self.pool, table, rows));
                })
                .join()
                .expect("the producer thread finishes");
        });
    }
}

/// What the start region leaves for the drain region.
pub enum Started {
    /// A service whose subscription drains the table: the drain waits for the latch.
    Waiting(RunningApp),
    /// Work the drain runs itself, a raw loop or a service's publishes, and the running service
    /// it runs against, if any, which outlives the region.
    Driving(Box<dyn FnOnce(&Runtime)>, Option<RunningApp>),
}

/// A scenario built but not started, and the table it drains.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    producer: Producer,
    fill: Option<Table>,
    start: Box<dyn FnOnce(&Runtime) -> Started>,
}

impl Pending {
    /// A scenario that starts with `start` against a fresh `table`, filled with as many rows as
    /// `latch` expects between the regions when `fill` is set. `build` receives the latch.
    ///
    /// The start is part of the measurement rather than of the setup, because the cold number is
    /// what starting costs. It is held as a boxed call so that every scenario hands over the same
    /// type; the one indirect call it adds lands in the cold number and nowhere else.
    pub fn new(
        table: Table,
        fill: bool,
        latch: Latch,
        build: impl FnOnce(Latch) -> Box<dyn FnOnce(&Runtime) -> Started>,
    ) -> Self {
        let producer = Producer::prepare(table);
        let runtime = runtime();
        // A pool belongs to the runtime it is built in, which for the scenario is the measured
        // one.
        let start = {
            let _context = runtime.enter();
            build(latch.clone())
        };
        Self {
            runtime,
            latch,
            producer,
            fill: fill.then_some(table),
            start,
        }
    }

    /// A service the subscription drains, on a pool of up to `connections`: `app` is built with
    /// the pool and the run's latch as its state.
    pub fn service<Service: App + 'static>(
        table: Table,
        messages: usize,
        connections: u32,
        app: impl FnOnce(PgPool, Latch) -> Service,
    ) -> Self {
        Self::new(table, true, Latch::new(messages), |latch| {
            let pool = postgres_pool(connections);
            let app = app(pool.clone(), latch);
            Box::new(move |runtime: &Runtime| {
                Started::Waiting(runtime.block_on(async {
                    warm(&pool, connections).await;
                    app.start().await.expect("the service starts")
                }))
            })
        })
    }

    /// A raw loop over `table` on a pool of up to `connections`: the start opens the pool, the
    /// drain runs `drain` with the run's latch until the table is empty.
    pub fn raw(
        table: Table,
        messages: usize,
        connections: u32,
        drain: impl FnOnce(&Runtime, &PgPool, &Latch) + 'static,
    ) -> Self {
        Self::new(table, true, Latch::new(messages), |latch| {
            let pool = postgres_pool(connections);
            Box::new(move |runtime: &Runtime| {
                runtime.block_on(warm(&pool, connections));
                Started::Driving(Box::new(move |runtime| drain(runtime, &pool, &latch)), None)
            })
        })
    }
}

/// Opens `connections` connections of the pool at once and returns them, part of every start.
///
/// A drain then never opens a connection. Otherwise how many it opens depends on the scheduling:
/// a returned connection goes back to the pool through a task, and an acquire that runs before
/// that task opens a new one. Every opening allocates, and the allocation limits would move with
/// the scheduler.
pub async fn warm(pool: &PgPool, connections: u32) {
    // All at once: a connection returned before the next acquire would be lent again.
    let held = join_all((0..connections).map(|_| pool.acquire())).await;
    for connection in held {
        drop(connection.expect("the pool opens a connection"));
    }
}

/// A single-threaded runtime: one thread means one order of execution, and the driver's I/O runs
/// on it rather than on a thread of its own.
fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Starts the scenario, fills its table, and drains it: the shape of every code-cost scenario.
///
/// Two measured regions, and the fill between them is in neither.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        producer,
        fill,
        start,
    } = pending;
    // What is dropped after the drain, a running service and its pool among it, returns its
    // connections through tasks of the runtime it belongs to.
    let _context = runtime.enter();
    let started = measure(|| start(&runtime));
    if let Some(table) = fill {
        producer.fill(table, latch.total());
    }
    assert_eq!(
        latch.remaining(),
        latch.total(),
        "the table was drained while it was being filled, so the measured region would be short"
    );
    match started {
        Started::Waiting(running) => {
            measure(|| runtime.block_on(latch.drained()));
            drop(running);
        }
        Started::Driving(drain, running) => {
            measure(|| drain(&runtime));
            drop(running);
        }
    }
    assert_eq!(latch.remaining(), 0, "the drain handled every delivery");
}
