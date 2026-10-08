//! A subscription on dedicated threads claims on the app's runtime and settles and publishes on
//! the threads': the connections the broker opens there serve the app's runtime and a restarted
//! service once the threads are gone.

use std::convert::Infallible;
use std::time::Duration;

use ruststream::Outgoing;
use ruststream::runtime::RunningApp;
use ruststream_sqlx::prelude::*;
use serde::Serialize;
use sqlx::Pool;

use crate::live;
use crate::locks::HeldLocks;
use crate::pool::{every_connection_answers, fresh_pool};
use crate::probe::{GUARD, Probe, until};

const POLL: Duration = Duration::from_millis(20);

/// What a suite's service publishes from its handler.
#[derive(Debug, Clone, Copy)]
enum Mount {
    /// Nothing: the handler's row settles alone.
    Settles,
    /// A reply through the broker's route table.
    Routed,
    /// A reply through the typed `Repository` policy.
    Typed,
    /// Nothing, after a query of the handler's own through `Ctx<keys::Pool>`.
    Queries,
}

/// What each handled row answers with, written to `email_jobs` under `replies`.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "replies")]
struct Reply {
    id: i64,
}

live::matrix! {
    /// Takes every connection of the pool but the one the delivery holds, where it holds one,
    /// once the claim's is back, and keeps them in `probe`: a connection the broker needs next
    /// on this thread opens there.
    async fn take_every_connection(pool: &Pool<Db>, probe: &Probe) {
        let held = usize::from(!LEASED);
        loop {
            while let Some(conn) = pool.try_acquire() {
                probe.keep(conn);
            }
            let size = usize::try_from(pool.size()).expect("fits");
            if probe.kept() + held >= size {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Keeps every idle connection of the pool until the test lets them go, so the settlement
    /// that follows on this thread finds none idle and opens its own.
    #[subscriber(InboxQueue::<Plain>::new("plain"), threads(2))]
    async fn settled(id: &i64, Ctx(pool): Ctx<keys::Pool<Db>>, State(probe): State<Probe>) {
        take_every_connection(&pool, &probe).await;
        probe.handled("plain", *id);
    }

    /// Keeps every idle connection like [`settled`], so the reply's insert opens its own.
    #[subscriber(InboxQueue::<Plain>::new("plain"), threads(2), reply)]
    async fn answered(
        id: &i64,
        Ctx(pool): Ctx<keys::Pool<Db>>,
        State(probe): State<Probe>,
    ) -> Reply {
        take_every_connection(&pool, &probe).await;
        probe.handled("plain", *id);
        Reply { id: *id }
    }

    /// Queries through the pool its context lends, after it took every idle connection of that
    /// pool, so the query opens one on this thread's runtime.
    #[subscriber(InboxQueue::<Plain>::new("plain"), threads(2))]
    async fn queried(id: &i64, Ctx(pool): Ctx<keys::Pool<Db>>, State(probe): State<Probe>) {
        take_every_connection(&pool, &probe).await;
        let mut conn = pool.acquire().await.expect("a connection");
        sqlx::raw_sql("SELECT 1").execute(&mut *conn).await.expect("the handler's query");
        drop(conn);
        probe.handled("plain", *id);
    }

    async fn started(pool: &Pool<Db>, probe: &Probe, mount: Mount) -> RunningApp {
        let probe = probe.clone();
        let broker = SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .route::<SendEmail>("replies");
        let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(probe))
            .with_broker(broker, move |b| match mount {
                Mount::Settles => {
                    b.include(settled);
                }
                Mount::Routed => {
                    b.include(answered);
                }
                Mount::Typed => {
                    b.include(answered).out_reply(Repository::<SendEmail>::default());
                }
                Mount::Queries => {
                    b.include(queried);
                }
            });
        tokio::time::timeout(GUARD, app.start())
            .await
            .expect("the service starts in time")
            .expect("the service starts")
    }

    /// The `run`th run of the service on its database: one row, handled and settled, and
    /// answered unless `mount` publishes nothing.
    async fn run(db: &live::Database<Db>, pool: &Pool<Db>, mount: Mount, run: usize) {
        let probe = Probe::default();
        let running = started(pool, &probe, mount).await;
        db.plain(&[run.to_string()]).await;
        probe.reached(1).await;
        // The settlement ends, and the connection it or the reply opened returns to the pool
        // while the threads still run.
        until("the row settled and its connection returned", async || {
            pool.num_idle() >= 1 && db.count("plain_jobs").await == 0
        })
        .await;
        tokio::time::timeout(GUARD, running.shutdown())
            .await
            .expect("the service stops in time")
            .expect("the service stops");
        probe.release();
        let runs = i64::try_from(run).expect("a count");
        assert_eq!(db.count("plain_jobs").await, 0, "run {run}: the row settled");
        let replies = if matches!(mount, Mount::Routed | Mount::Typed) { runs } else { 0 };
        assert_eq!(db.count("email_jobs").await, replies, "run {run}: the replies written");
        // A fresh database numbers its rows from one.
        let ids: Vec<i64> = (1..=runs).collect();
        assert_eq!(
            Db::held_locks(&db.pool, "plain_jobs", &ids).await,
            0,
            "run {run}: the stopped service holds no lock"
        );
    }

    /// Two runs of the service on one pool, each followed by a query on every connection.
    async fn outlived(mount: Mount) {
        let Some(db) = database().await else { return };
        let pool = fresh_pool(&db);
        run(&db, &pool, mount, 1).await;
        every_connection_answers(&pool, "after the first run").await;
        run(&db, &pool, mount, 2).await;
        every_connection_answers(&pool, "after the restart").await;
        pool.close().await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_settlement_opens_its_connection_on_the_app_runtime() {
        outlived(Mount::Settles).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_routed_reply_opens_its_connection_on_the_app_runtime() {
        outlived(Mount::Routed).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_typed_reply_opens_its_connection_on_the_app_runtime() {
        outlived(Mount::Typed).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_query_leaves_no_connection_that_dies_with_its_thread() {
        outlived(Mount::Queries).await;
    }
}
