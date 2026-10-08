//! Transactional mode on dedicated threads: the delivery's transaction, opened by the claim on
//! the app's runtime, carries the handler's writes on the thread and commits there with the
//! acknowledgement; a retry discards them.

use std::convert::Infallible;
use std::time::Duration;

use ruststream_sqlx::prelude::*;
use sqlx::AssertSqlSafe;

use crate::live;
use crate::locks::HeldLocks;
use crate::pool::{every_connection_answers, fresh_pool};
use crate::probe::{GUARD, Probe, until};

const POLL: Duration = Duration::from_millis(20);

/// The rows the suite writes; the first one is retried once.
const ROWS: i64 = 6;

live::matrix! {
    #[subscriber(InboxQueue::<Plain>::new("plain"), threads(2))]
    async fn audited(
        n: &i64,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
        State(probe): State<Probe>,
    ) -> HandlerOutcome {
        let attempt = attempt.expect("the table counts attempts");
        let write = format!("INSERT INTO audit (job_id, note) VALUES ({n}, '{attempt}')");
        sqlx::raw_sql(AssertSqlSafe(write))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        probe.handled("plain", *n);
        if *n == 0 && attempt == 1 {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_transaction_crosses_to_the_thread_and_commits_there() {
        let Some(db) = database().await else { return };
        let pool = fresh_pool(&db);
        let probe = Probe::default();
        let state = probe.clone();
        let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(SqlxBroker::new(pool.clone()).poll_interval(POLL), |b| {
                b.include(audited.transactional());
            });
        let running = tokio::time::timeout(GUARD, app.start())
            .await
            .expect("the service starts in time")
            .expect("the service starts");
        let payloads: Vec<String> = (0..ROWS).map(|n| n.to_string()).collect();
        db.plain(&payloads).await;
        probe.reached(usize::try_from(ROWS + 1).expect("fits")).await;
        until("every row settled", async || db.count("plain_jobs").await == 0).await;
        tokio::time::timeout(GUARD, running.shutdown())
            .await
            .expect("the service stops in time")
            .expect("the service stops");
        let mut notes: Vec<(i64, String)> =
            sqlx::query_as("SELECT job_id, note FROM audit")
                .fetch_all(&db.pool)
                .await
                .expect("the audit reads");
        notes.sort();
        let expected: Vec<(i64, String)> = (0..ROWS)
            .map(|n| (n, if n == 0 { "2" } else { "1" }.to_owned()))
            .collect();
        assert_eq!(notes, expected, "each acknowledgement kept its write, the retry dropped its own");
        let ids: Vec<i64> = (1..=ROWS).collect();
        assert_eq!(
            Db::held_locks(&db.pool, "plain_jobs", &ids).await,
            0,
            "the stopped service holds no lock"
        );
        every_connection_answers(&pool, "after the run").await;
        pool.close().await;
        db.finish().await;
    }
}
