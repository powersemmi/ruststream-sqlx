//! Transactional mode, run as an application through `TestApp::start_live` on every stand and in
//! every form: the handler writes through the delivery's transaction, which acknowledgement commits
//! and every other outcome discards; writes through the pool commit on their own; the keys after
//! the transaction read the delivery's attempt and the pool. In the lease form a lease that runs
//! out under the handler rolls its writes back, and a Postgres table whose transactions could not
//! see the lease extended refuses the mode. In the advisory lock form the session that holds the
//! key holds the transaction too. On SQLite a table in `immediate` mode takes the write lock when
//! its delivery's transaction opens.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::Tx;
use ruststream_sqlx::dialect::Dialect;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, Pool};

const POLL: Duration = Duration::from_millis(20);

/// How long a handler's delayed retry waits before its row comes back.
const LATER: Duration = Duration::from_millis(300);

/// The lease of the suite's lease tables: short, so a row whose lease the broker stopped extending
/// returns while the test waits.
const LEASE: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    n: i64,
}

const JOB: Job = Job { n: 7 };

/// A task a handler schedules in its delivery's transaction.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Followup {
    attempt: u64,
}

/// The audit row of `job` noting `note`, written as text so one statement serves every stand.
fn audit(job: &Job, note: &str) -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        "INSERT INTO audit (job_id, note) VALUES ({}, '{note}')",
        job.n
    ))
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .lease(LEASE)
            .route::<SendEmail>("emails")
            .route::<Plain>("plain")
            .route::<Fragile>("fragile")
    }

    /// The audit rows' notes, in order.
    async fn notes(db: &live::Database<Db>) -> Vec<String> {
        sqlx::query_scalar("SELECT note FROM audit ORDER BY note")
            .fetch_all(&db.pool)
            .await
            .expect("the audit reads")
    }

    async fn published<State: Send + Sync + 'static>(tb: &TestApp<State>, to: &str) {
        tb.broker::<SqlxBroker<Db>>()
            .message(&JOB)
            .to(to)
            .publish()
            .await
            .expect("the publish settles");
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn audited(job: &Job, Ctx(mut tx): Ctx<keys::Tx<Db>>) -> HandlerOutcome {
        sqlx::raw_sql(audit(job, "acked"))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ack_commits_the_handler_writes() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(audited.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "plain").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(notes(&db).await, ["acked"], "the acknowledgement kept the handler's write");
        assert_eq!(db.count("plain_jobs").await, 0, "and finished the job with it");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // In process the connection's database calls run off the test's clock, the delivery's own
    // transaction and its settlement among them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_process_ack_commits_the_handler_writes() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(audited.transactional());
            });
        let tb = TestApp::start(app).await.expect("the app starts");
        published(&tb, "plain").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(notes(&db).await, ["acked"], "the acknowledgement kept the handler's write");
        assert_eq!(db.count("plain_jobs").await, 0, "and finished the job with it");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn noted(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
    ) -> HandlerOutcome {
        let attempt = attempt.expect("the table counts attempts");
        sqlx::raw_sql(audit(job, &attempt.to_string()))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        if attempt == 1 {
            HandlerOutcome::retry_after(LATER)
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_discards_the_writes_and_keeps_attempt_and_retry_after() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(noted.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "emails").await;
        tb.settle().await.expect("the first delivery settles");
        let rows = db.email_rows("email_jobs").await;
        assert_eq!(rows.len(), 1, "the retry kept the job: {rows:?}");
        assert_eq!(rows[0].2, 2, "the delivery counted the attempt");
        assert!(!rows[0].3, "the job is not finished");
        assert!(db.email_waits().await, "the retry wrote the time the job waits for");
        assert!(notes(&db).await.is_empty(), "the retry discarded the handler's write");
        tb.advance(LATER + Duration::from_millis(100))
            .await
            .expect("the redelivery settles");
        assert_eq!(notes(&db).await, ["2"], "the acknowledgement kept the second write alone");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn scheduling(
        _job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
    ) -> HandlerOutcome {
        let attempt = attempt.expect("the table counts attempts");
        let payload = serde_json::to_vec(&Followup { attempt }).expect("json");
        SendEmail::queued("followups", payload)
            .insert(&mut *tx)
            .await
            .expect("the follow-up writes");
        if attempt == 1 {
            HandlerOutcome::retry_after(LATER)
        } else {
            HandlerOutcome::ack()
        }
    }

    #[subscriber(InboxQueue::<SendEmail>::new("followups"))]
    async fn followed(_followup: &Followup) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_task_written_through_tx_commits_with_the_ack() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(scheduling.transactional());
                b.include(followed);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "emails").await;
        tb.advance(LATER + Duration::from_millis(100))
            .await
            .expect("the redelivery settles");
        let followups: Vec<Followup> = db
            .email_rows("email_jobs")
            .await
            .into_iter()
            .filter(|row| row.0 == "followups")
            .map(|row| serde_json::from_slice(&row.1).expect("a follow-up"))
            .collect();
        assert_eq!(
            followups,
            [Followup { attempt: 2 }],
            "the retry discarded the first attempt's task, the acknowledgement kept the second's"
        );
        // A row the handler wrote is the harness's to wait for only once a claim takes it.
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the follow-up settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("followups")
            .assert_called_once()
            .with(&Followup { attempt: 2 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // The pool's write comes first: on SQLite the delivery's transaction holds the database's one
    // write lock from its first write until it settles, and a write through the pool would wait
    // for it.
    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn twice_noted(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        Ctx(pool): Ctx<keys::Pool<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
    ) -> HandlerOutcome {
        let attempt = attempt.expect("the table counts attempts");
        sqlx::raw_sql(audit(job, &format!("pool {attempt}")))
            .execute(&pool)
            .await
            .expect("the pool's audit row writes");
        sqlx::raw_sql(audit(job, &format!("tx {attempt}")))
            .execute(&mut *tx)
            .await
            .expect("the transaction's audit row writes");
        if attempt == 1 {
            HandlerOutcome::retry_after(LATER)
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_writes_commit_on_their_own() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(twice_noted.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "emails").await;
        assert_eq!(notes(&db).await, ["pool 1"], "the retry kept the pool's write alone");
        tb.advance(LATER + Duration::from_millis(100))
            .await
            .expect("the redelivery settles");
        assert_eq!(
            notes(&db).await,
            ["pool 1", "pool 2", "tx 2"],
            "the pool's writes stay, the transaction's arrive once"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn counted(
        _job: &Job,
        Ctx(_tx): Ctx<keys::Tx<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
    ) -> HandlerOutcome {
        match attempt {
            Some(1) => HandlerOutcome::retry_after(LATER),
            Some(2) => HandlerOutcome::ack(),
            _ => HandlerOutcome::drop(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attempt_reads_beside_tx() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(counted.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "emails").await;
        tb.advance(LATER + Duration::from_millis(100))
            .await
            .expect("the redelivery settles");
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(2)
            .outcomes();
        assert_eq!(
            outcomes,
            [Outcome::Nack, Outcome::Ack],
            "the handler read attempt 1, then 2"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn pooled(
        job: &Job,
        Ctx(pool): Ctx<keys::Pool<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
    ) -> HandlerOutcome {
        let attempt = attempt.expect("the table counts attempts");
        sqlx::raw_sql(audit(job, &attempt.to_string()))
            .execute(&pool)
            .await
            .expect("the audit row writes");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plain_handler_reads_the_pool() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(pooled);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "plain").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(notes(&db).await, ["1"], "the handler wrote through the pool");
        assert_eq!(db.count("plain_jobs").await, 0, "and finished the job");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Fragile>::new("fragile"))]
    async fn referenced(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        Ctx(pool): Ctx<keys::Pool<Db>>,
    ) -> HandlerOutcome {
        // Through the pool: the write stays, and counts the deliveries.
        sqlx::raw_sql(audit(job, "delivered"))
            .execute(&pool)
            .await
            .expect("the audit row writes");
        let delivered: i64 = sqlx::query_scalar("SELECT count(*) FROM audit")
            .fetch_one(&pool)
            .await
            .expect("the audit counts");
        if delivered == 1 {
            // A reference to the job, in the delivery's transaction, makes its acknowledgement
            // fail.
            sqlx::raw_sql("INSERT INTO fragile_refs (job_id) SELECT id FROM fragile_jobs")
                .execute(&mut *tx)
                .await
                .expect("the reference writes");
        }
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_settlement_rolls_the_handler_back() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(referenced.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "fragile").await;
        // The failed acknowledgement returns the row at once, and its next delivery settles.
        tb.advance(Duration::from_millis(300))
            .await
            .expect("the redelivery settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("fragile")
            .assert_called(2);
        assert_eq!(
            db.count("fragile_refs").await,
            0,
            "the failed acknowledgement rolled back what the handler wrote with it"
        );
        assert_eq!(db.count("fragile_jobs").await, 0, "the second acknowledgement finished the job");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn half_done(job: &Job, Ctx(mut tx): Ctx<keys::Tx<Db>>) -> HandlerOutcome {
        sqlx::raw_sql(audit(job, "half"))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        panic!("the handler fails half way through its writes");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panic_never_commits_the_handler_writes() {
        let Some(db) = database().await else { return };
        let skip = FailurePolicies::default().with_panic(FailurePolicy::Skip);
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(half_done.transactional().on_failure(skip));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "plain").await;
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once();
        assert_eq!(db.count("plain_jobs").await, 0, "the panic policy acknowledged the job");
        assert!(
            notes(&db).await.is_empty(),
            "the acknowledgement of a panicked handler committed none of its writes"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    /// Where a handler keeps its delivery's transaction past its own end, and whether it did.
    #[derive(Clone, Default)]
    struct Kept(Arc<Mutex<(Option<Tx<Db>>, bool)>>);

    #[derive(Clone, FromRef)]
    struct Keeping {
        kept: Kept,
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn keeping(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        State(kept): State<Kept>,
    ) -> HandlerOutcome {
        sqlx::raw_sql(audit(job, "kept"))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        let mut slot = kept.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !slot.1 {
            // The first delivery's handler keeps its transaction, and ends.
            *slot = (Some(tx), true);
        }
        drop(slot);
        HandlerOutcome::ack()
    }

    // What the kept transaction wrote is read only once it ended: on SQLite a read of a table it
    // wrote waits for it. The one note at the end shows its write never committed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_transaction_kept_past_the_handler_settles_nothing() {
        let Some(db) = database().await else { return };
        let kept = Kept::default();
        let state = Keeping { kept: kept.clone() };
        let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(broker(&db.pool), |b| {
                b.include(keeping.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        published(&tb, "plain").await;
        assert_eq!(db.count("plain_jobs").await, 1, "the acknowledgement took no effect");
        // The kept transaction ends as it drops, and the server rolls it back. The row returns
        // at once where the transaction or its session held it, and once its lease runs out where
        // a lease does.
        drop(kept.0.lock().unwrap_or_else(PoisonError::into_inner).0.take());
        let returns = if LEASED { LEASE * 3 } else { Duration::from_millis(300) };
        tb.advance(returns).await.expect("the redelivery settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2);
        assert_eq!(notes(&db).await, ["kept"], "the second delivery's write alone commits");
        assert_eq!(db.count("plain_jobs").await, 0, "with its acknowledgement");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}

/// A lease in work under a transactional handler, in every stand's lease form: one that runs out
/// and another claim takes, and one the broker extends while the handler's transaction holds what
/// the extension needs.
mod lease_in_work {
    use super::*;

    /// Longer than half the lease: the broker extends the lease while the handler works.
    const OUTLASTS: Duration = Duration::from_millis(1200);

    /// Another claim's lease on every plain job, as a claim writes it once the delivery's lease ran
    /// out: an expiry an hour away, and the attempt counted. Text for the dialect `dialect` names.
    fn taken_elsewhere(dialect: &str) -> &'static str {
        match dialect {
            "postgres" => {
                "UPDATE plain_jobs SET locked_until = now() + interval '1 hour', \
                 attempt = attempt + 1"
            }
            "mysql" => {
                "UPDATE plain_jobs SET locked_until = UTC_TIMESTAMP() + INTERVAL 1 HOUR, \
                 attempt = attempt + 1"
            }
            _ => {
                "UPDATE plain_jobs SET locked_until = \
                 strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', '+1 hour'), attempt = attempt + 1"
            }
        }
    }

    /// What the broker's extension of a plain job's lease waits for, taken in a transaction: the
    /// job's row on a server, the database's one write lock on SQLite.
    fn held_up(dialect: &str) -> &'static str {
        match dialect {
            "postgres" | "mysql" => "SELECT id FROM plain_jobs FOR UPDATE",
            _ => "UPDATE plain_jobs SET payload = payload",
        }
    }

    live::lease_stands! {
        fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
            SqlxBroker::new(pool.clone())
                .poll_interval(POLL)
                .lease(LEASE)
                .route::<Plain>("plain")
        }

        async fn notes(db: &live::Database<Db>) -> Vec<String> {
            sqlx::query_scalar("SELECT note FROM audit ORDER BY note")
                .fetch_all(&db.pool)
                .await
                .expect("the audit reads")
        }

        #[subscriber(InboxQueue::<Plain>::new("plain"))]
        async fn holding(job: &Job, Ctx(mut tx): Ctx<keys::Tx<Db>>) -> HandlerOutcome {
            sqlx::raw_sql(held_up(DIALECT.name()))
                .execute(&mut *tx)
                .await
                .expect("the transaction takes what the extension needs");
            sqlx::raw_sql(audit(job, "held"))
                .execute(&mut *tx)
                .await
                .expect("the audit row writes");
            // The handler's own work, through a round of extensions, which waits for it.
            tokio::time::sleep(OUTLASTS).await;
            HandlerOutcome::ack()
        }

        // The acknowledgement takes the lease ahead of the extension waiting for its transaction:
        // waiting for that extension instead would wait for itself.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn an_extension_held_up_by_the_handler_never_holds_up_its_ack() {
            let Some(db) = database().await else { return };
            let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
                .with_broker(broker(&db.pool), |b| {
                    b.include(holding.transactional());
                });
            let tb = TestApp::start_live(app).await.expect("the app starts");
            tb.broker::<SqlxBroker<Db>>()
                .message(&JOB)
                .to("plain")
                .publish()
                .await
                .expect("the publish settles");
            assert_eq!(notes(&db).await, ["held"], "the acknowledgement kept the handler's write");
            assert_eq!(db.count("plain_jobs").await, 0, "and finished the job");
            tb.shutdown().await.expect("the app stops");
            db.finish().await;
        }

        // The other claim comes first: on SQLite the delivery's transaction holds the database's
        // one write lock from its first write, and the other claim would wait for it.
        #[subscriber(InboxQueue::<Plain>::new("plain"))]
        async fn overtaken(
            job: &Job,
            Ctx(mut tx): Ctx<keys::Tx<Db>>,
            Ctx(pool): Ctx<keys::Pool<Db>>,
        ) -> HandlerOutcome {
            sqlx::raw_sql(taken_elsewhere(DIALECT.name()))
                .execute(&pool)
                .await
                .expect("another claim takes the row");
            sqlx::raw_sql(audit(job, "overtaken"))
                .execute(&mut *tx)
                .await
                .expect("the audit row writes");
            HandlerOutcome::ack()
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_lost_lease_rolls_the_handler_back() {
            let Some(db) = database().await else { return };
            let app = RustStream::new(AppInfo::new("transactional", "0.0.0"))
                .with_broker(broker(&db.pool), |b| {
                    b.include(overtaken.transactional());
                });
            let tb = TestApp::start_live(app).await.expect("the app starts");
            tb.broker::<SqlxBroker<Db>>()
                .message(&JOB)
                .to("plain")
                .publish()
                .await
                .expect("the publish settles");
            tb.broker::<SqlxBroker<Db>>()
                .subscriber("plain")
                .assert_called_once()
                .settled(HandlerOutcome::ack());
            let notes = notes(&db).await;
            assert!(notes.is_empty(), "the lost lease rolled back the handler's write: {notes:?}");
            assert_eq!(
                db.count("plain_jobs").await,
                1,
                "the acknowledgement took no effect: the row is the other claim's"
            );
            tb.shutdown().await.expect("the app stops");
            db.finish().await;
        }
    }
}

/// The advisory lock form on Postgres, where the database shows each session's locks and state.
#[cfg(feature = "postgres")]
mod postgres_session {
    use sqlx::Postgres;

    use super::*;
    use crate::live::postgres::{advisory_locks, database, idle_in_transaction};
    use crate::live::rows::advisory::Plain;

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn watched(
        job: &Job,
        Ctx(mut tx): Ctx<keys::Tx<Postgres>>,
        Ctx(pool): Ctx<keys::Pool<Postgres>>,
    ) -> HandlerOutcome {
        let seen = format!(
            "locks {}, idle in transaction {}",
            advisory_locks(&pool).await,
            idle_in_transaction(&pool).await
        );
        sqlx::raw_sql(audit(job, &seen))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_transactional_session_holds_its_lock_while_the_handler_writes() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<Plain>("plain");
        let app =
            RustStream::new(AppInfo::new("transactional", "0.0.0")).with_broker(broker, |b| {
                b.include(watched.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Postgres>>()
            .message(&JOB)
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        let notes: Vec<String> = sqlx::query_scalar("SELECT note FROM audit")
            .fetch_all(&db.pool)
            .await
            .expect("the audit reads");
        assert_eq!(
            notes,
            ["locks 1, idle in transaction 1"],
            "while the handler wrote, its session held the key's lock and the open transaction"
        );
        assert_eq!(
            (
                advisory_locks(&db.pool).await,
                idle_in_transaction(&db.pool).await
            ),
            (0, 0),
            "the acknowledgement committed, released the key and ended the transaction"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}

/// SQLite's modes, on a database in a file: a table in `immediate` mode opens its delivery's
/// transaction with the database's write lock taken, so a second writer that asks for the lock
/// meets it held while the handler runs; without a mode the transaction takes the lock at its
/// first write.
#[cfg(feature = "sqlite")]
mod sqlite_modes {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use chrono::{DateTime, Utc};
    use ruststream::OutgoingMessage;
    use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Error, FromRow, Sqlite, SqliteConnection, SqlitePool};

    use super::*;

    /// How long the second writer waits for the write lock: longer than any claim holds it, far
    /// shorter than the test waits for its delivery.
    const SECOND_WRITER_WAITS: Duration = Duration::from_millis(500);

    /// The plain queue by lease, its transactions opened with the write lock taken.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", mode = immediate)]
    pub(crate) struct LeasedImmediately {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl<DB> Publish<DB> for LeasedImmediately
    where
        DB: QueueDatabase,
        Self: Insert<DB::Connection>,
    {
        async fn publish(
            conn: &mut DB::Connection,
            message: &OutgoingMessage<'_>,
        ) -> Result<(), Error> {
            let job = Self {
                id: 0,
                attempt: 1,
                locked_until: None,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// The plain queue by advisory lock, its transactions opened with the write lock taken.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", advisory_lock = "plain_jobs-{id}", mode = immediate)]
    pub(crate) struct AdvisedImmediately {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl<DB> Publish<DB> for AdvisedImmediately
    where
        DB: QueueDatabase,
        Self: Insert<DB::Connection>,
    {
        async fn publish(
            conn: &mut DB::Connection,
            message: &OutgoingMessage<'_>,
        ) -> Result<(), Error> {
            let job = Self {
                id: 0,
                attempt: 1,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// A database of the test's own in a file, with the stand's schema applied.
    struct FileDatabase {
        pool: SqlitePool,
        path: PathBuf,
    }

    impl FileDatabase {
        async fn open() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "rs_sqlx_modes_{}_{}.db",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let pool = SqlitePoolOptions::new()
                .max_connections(8)
                .acquire_timeout(Duration::from_secs(5))
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(&path)
                        .create_if_missing(true),
                )
                .await
                .expect("the database file opens");
            sqlx::raw_sql(include_str!("schema/sqlite.sql"))
                .execute(&pool)
                .await
                .expect("the test schema applies");
            Self { pool, path }
        }

        async fn notes(&self) -> Vec<String> {
            sqlx::query_scalar("SELECT note FROM audit ORDER BY note")
                .fetch_all(&self.pool)
                .await
                .expect("the audit reads")
        }

        /// Closes the pool and removes the file, with the journals beside it.
        async fn finish(self) {
            self.pool.close().await;
            for suffix in ["", "-wal", "-shm", "-journal"] {
                let mut file = self.path.clone().into_os_string();
                file.push(suffix);
                let _ = std::fs::remove_file(file);
            }
        }
    }

    /// What a second connection meets when it asks for the database's write lock: `free` when it
    /// took the lock, which it gives back at once, or the database's refusal.
    async fn second_writer(pool: &SqlitePool) -> String {
        let options = (*pool.connect_options())
            .clone()
            .busy_timeout(SECOND_WRITER_WAITS);
        let mut conn = SqliteConnection::connect_with(&options)
            .await
            .expect("a second connection opens");
        let met = match sqlx::raw_sql("BEGIN IMMEDIATE").execute(&mut conn).await {
            Ok(_) => {
                sqlx::raw_sql("ROLLBACK")
                    .execute(&mut conn)
                    .await
                    .expect("the second writer gives the lock back");
                "free".to_owned()
            }
            Err(error) => error
                .as_database_error()
                .map_or_else(|| error.to_string(), |refused| refused.message().to_owned()),
        };
        conn.close().await.expect("the second connection closes");
        met
    }

    /// Notes through the delivery's transaction what a second writer met, and acknowledges.
    async fn note_the_second_writer(
        job: &Job,
        tx: &mut Tx<Sqlite>,
        pool: &SqlitePool,
    ) -> HandlerOutcome {
        let met = second_writer(pool).await;
        sqlx::raw_sql(audit(job, &met))
            .execute(&mut **tx)
            .await
            .expect("the note writes");
        HandlerOutcome::ack()
    }

    /// Runs `app` on its own, publishes the job to it, and returns the notes its handler wrote.
    async fn noted(db: &FileDatabase, app: RustStream) -> Vec<String> {
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Sqlite>>()
            .message(&JOB)
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Sqlite>>()
            .subscriber("plain")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.notes().await
    }

    mod lease {
        use super::*;
        use crate::live::rows::lease::Plain;

        #[subscriber(InboxQueue::<LeasedImmediately>::new("plain"))]
        async fn immediate(
            job: &Job,
            Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
            Ctx(pool): Ctx<keys::Pool<Sqlite>>,
        ) -> HandlerOutcome {
            note_the_second_writer(job, &mut tx, &pool).await
        }

        #[subscriber(InboxQueue::<Plain>::new("plain"))]
        async fn deferred(
            job: &Job,
            Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
            Ctx(pool): Ctx<keys::Pool<Sqlite>>,
        ) -> HandlerOutcome {
            note_the_second_writer(job, &mut tx, &pool).await
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn sqlite_immediate_mode_takes_the_write_lock_at_begin() {
            let db = FileDatabase::open().await;
            let broker = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .route::<LeasedImmediately>("plain");
            let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
                b.include(immediate.transactional());
            });
            assert_eq!(
                noted(&db, app).await,
                ["database is locked"],
                "the delivery's transaction held the write lock from its start"
            );
            db.finish().await;

            let db = FileDatabase::open().await;
            let broker = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .route::<Plain>("plain");
            let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
                b.include(deferred.transactional());
            });
            assert_eq!(
                noted(&db, app).await,
                ["free"],
                "without a mode, the transaction takes the lock at its first write"
            );
            db.finish().await;
        }
    }

    mod advisory {
        use super::*;
        use crate::live::rows::advisory::Plain;

        #[subscriber(InboxQueue::<AdvisedImmediately>::new("plain"))]
        async fn immediate(
            job: &Job,
            Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
            Ctx(pool): Ctx<keys::Pool<Sqlite>>,
        ) -> HandlerOutcome {
            note_the_second_writer(job, &mut tx, &pool).await
        }

        #[subscriber(InboxQueue::<Plain>::new("plain"))]
        async fn deferred(
            job: &Job,
            Ctx(mut tx): Ctx<keys::Tx<Sqlite>>,
            Ctx(pool): Ctx<keys::Pool<Sqlite>>,
        ) -> HandlerOutcome {
            note_the_second_writer(job, &mut tx, &pool).await
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn sqlite_immediate_mode_takes_the_write_lock_at_begin() {
            let db = FileDatabase::open().await;
            let broker = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .route::<AdvisedImmediately>("plain");
            let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
                b.include(immediate.transactional());
            });
            assert_eq!(
                noted(&db, app).await,
                ["database is locked"],
                "the delivery's transaction held the write lock from its start"
            );
            db.finish().await;

            let db = FileDatabase::open().await;
            let broker = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .route::<Plain>("plain");
            let app = RustStream::new(AppInfo::new("modes", "0.0.0")).with_broker(broker, |b| {
                b.include(deferred.transactional());
            });
            assert_eq!(
                noted(&db, app).await,
                ["free"],
                "without a mode, the transaction takes the lock at its first write"
            );
            db.finish().await;
        }
    }
}

/// A lease table whose transactions open at REPEATABLE READ, with a handler that outlives a lease
/// extension inside its transaction.
mod repeatable_read {
    use chrono::{DateTime, Utc};
    use ruststream::OutgoingMessage;
    use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase};
    use sqlx::{Error, FromRow};

    use super::*;

    /// Longer than half the lease: the broker extends the lease while the transaction is open.
    const OUTLASTS: Duration = Duration::from_millis(1200);

    /// The plain queue by lease, its transactions at REPEATABLE READ.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", isolation = repeatable_read)]
    pub(crate) struct Repeatable {
        #[field(id, generated)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl<DB> Publish<DB> for Repeatable
    where
        DB: QueueDatabase,
        Self: Insert<DB::Connection>,
    {
        async fn publish(
            conn: &mut DB::Connection,
            message: &OutgoingMessage<'_>,
        ) -> Result<(), Error> {
            let job = Self {
                id: 0,
                attempt: 1,
                locked_until: None,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// The suite's items on one stand whose database runs REPEATABLE READ: its `Db` and
    /// `database` in scope where it expands.
    macro_rules! outlasting_an_extension {
        () => {
            fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
                SqlxBroker::new(pool.clone())
                    .poll_interval(POLL)
                    .lease(LEASE)
                    .route::<Repeatable>("plain")
            }

            #[subscriber(InboxQueue::<Repeatable>::new("plain"))]
            async fn outlasting(job: &Job, Ctx(mut tx): Ctx<keys::Tx<Db>>) -> HandlerOutcome {
                sqlx::raw_sql(audit(job, "outlasted"))
                    .execute(&mut *tx)
                    .await
                    .expect("the audit row writes");
                // The handler's own work, long enough for the broker to extend the lease.
                tokio::time::sleep(OUTLASTS).await;
                HandlerOutcome::ack()
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_handler_that_outlives_an_extension_commits_with_its_ack() {
                let Some(db) = database().await else { return };
                let app = RustStream::new(AppInfo::new("transactional", "0.0.0")).with_broker(
                    broker(&db.pool),
                    |b| {
                        b.include(outlasting.transactional());
                    },
                );
                let tb = TestApp::start_live(app).await.expect("the app starts");
                tb.broker::<SqlxBroker<Db>>()
                    .message(&JOB)
                    .to("plain")
                    .publish()
                    .await
                    .expect("the publish settles");
                let notes: Vec<String> = sqlx::query_scalar("SELECT note FROM audit")
                    .fetch_all(&db.pool)
                    .await
                    .expect("the audit reads");
                assert_eq!(
                    notes,
                    ["outlasted"],
                    "the acknowledgement found the extended lease and kept the handler's write"
                );
                assert_eq!(db.count("plain_jobs").await, 0, "and finished the job");
                tb.shutdown().await.expect("the app stops");
                db.finish().await;
            }
        };
    }

    live::mysql_stands! {
        outlasting_an_extension!();
    }

    /// Postgres reads every row of a REPEATABLE READ transaction as its first statement found it,
    /// so the acknowledgement would not see the lease extended after it: the subscription refuses
    /// transactional mode there, and runs as ever without it.
    #[cfg(feature = "postgres")]
    mod postgres {
        use ruststream::testing::TestError;
        use ruststream_sqlx::SqlxBrokerError;
        use sqlx::Postgres;

        use super::*;
        use crate::live::postgres::database;

        #[subscriber(InboxQueue::<Repeatable>::new("plain"))]
        async fn outlasting(job: &Job, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
            sqlx::raw_sql(audit(job, "outlasted"))
                .execute(&mut *tx)
                .await
                .expect("the audit row writes");
            HandlerOutcome::ack()
        }

        #[subscriber(InboxQueue::<Repeatable>::new("plain"))]
        async fn plainly(_job: &Job) -> HandlerOutcome {
            HandlerOutcome::ack()
        }

        fn broker(db: &live::Database<Postgres>) -> SqlxBroker<Postgres> {
            SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .lease(LEASE)
                .route::<Repeatable>("plain")
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_transactional_lease_at_repeatable_read_refuses_to_start() {
            let Some(db) = database().await else { return };
            let app = RustStream::new(AppInfo::new("transactional", "0.0.0")).with_broker(
                broker(&db),
                |b| {
                    b.include(outlasting.transactional());
                },
            );
            let refused = match TestApp::start_live(app).await {
                Err(TestError::Subscribe(refused)) => refused,
                Err(other) => panic!("the app failed for another reason: {other}"),
                Ok(_) => panic!("the subscription started at a level that hides lease extensions"),
            };
            assert!(
                matches!(
                    refused.downcast_ref::<SqlxBrokerError>(),
                    Some(SqlxBrokerError::Declaration { reason, .. })
                        if reason.contains("`isolation = repeatable_read`")
                ),
                "{refused}"
            );
            let app =
                RustStream::new(AppInfo::new("plain", "0.0.0")).with_broker(broker(&db), |b| {
                    b.include(plainly);
                });
            let tb = TestApp::start_live(app)
                .await
                .expect("without transactional mode the table starts");
            tb.broker::<SqlxBroker<Postgres>>()
                .message(&JOB)
                .to("plain")
                .publish()
                .await
                .expect("the publish settles");
            assert_eq!(db.count("plain_jobs").await, 0, "and settles its rows");
            tb.shutdown().await.expect("the app stops");
            db.finish().await;
        }
    }
}
