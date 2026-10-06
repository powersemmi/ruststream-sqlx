//! Transactional mode, run as an application through `TestApp::start_live` on every stand and in
//! every form: the handler writes through the delivery's transaction, which acknowledgement commits
//! and every other outcome discards; writes through the pool commit on their own; the keys after
//! the transaction read the delivery's attempt and the pool.

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::testing::{Outcome, TestApp};
use ruststream_sqlx::Tx;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::Pool;

use super::{JOB, Job, LEASE, POLL, audit};
use crate::live;

/// How long a handler's delayed retry waits before its row comes back.
const LATER: Duration = Duration::from_millis(300);

/// A task a handler schedules in its delivery's transaction.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Followup {
    attempt: u64,
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
