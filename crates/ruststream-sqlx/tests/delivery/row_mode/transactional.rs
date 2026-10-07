//! A row-mode delivery in transactional mode, on every stand and in every form: the handler takes
//! the row beside the transaction its delivery settles in, a retry discards what the handler wrote,
//! and the acknowledgement commits it with the row's settlement.

use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use sqlx::{AssertSqlSafe, Pool};

use super::{POLL, SETTLED};
use crate::live;

/// The audit row noting that the mail `job_id` went to `recipient` at `attempt`, written as text
/// so one statement serves every stand.
fn sent_to(job_id: i64, recipient: &str, attempt: u64) -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        "INSERT INTO audit (job_id, note) VALUES ({job_id}, '{recipient} at {attempt}')"
    ))
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    /// The audit rows' notes, in order.
    async fn notes(db: &live::Database<Db>) -> Vec<String> {
        sqlx::query_scalar("SELECT note FROM audit ORDER BY note")
            .fetch_all(&db.pool)
            .await
            .expect("the audit reads")
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"))]
    async fn sent(
        mail: &Mail,
        Ctx(mut tx): Ctx<keys::Tx<Db>>,
        Ctx(attempt): Ctx<keys::Attempt>,
    ) -> HandlerOutcome {
        let attempt = attempt.expect("the table counts attempts");
        sqlx::raw_sql(sent_to(mail.job_id, &mail.recipient, attempt))
            .execute(&mut *tx)
            .await
            .expect("the audit row writes");
        if attempt == 1 {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_acknowledgement_commits_the_handler_writes_with_the_row() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("mail", "ops@example.com", None)]).await;
        let written: Mail = db.mails().await.remove(0);
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(sent.transactional());
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("both deliveries settle");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        assert_eq!(lent.len(), 2, "the retry brought the row back once: {lent:?}");
        let counted = Mail {
            attempt: written.attempt + 1,
            ..written
        };
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_called(2)
            .with_value(&counted.leased_as(&lent[1]))
            .settled(HandlerOutcome::ack());
        assert_eq!(
            notes(&db).await,
            ["ops@example.com at 2"],
            "the retry discarded the first write, the acknowledgement kept the second"
        );
        assert_eq!(db.count("mail_jobs").await, 0, "and finished the mail with it");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
