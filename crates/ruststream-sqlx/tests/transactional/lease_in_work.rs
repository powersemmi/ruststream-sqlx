//! A lease in work under a transactional handler, in every stand's lease form: one that runs out
//! and another claim takes, and one the broker extends while the handler's transaction holds what
//! the extension needs.

use std::time::Duration;

use ruststream::testing::TestApp;
use ruststream_sqlx::dialect::Dialect;
use ruststream_sqlx::prelude::*;
use sqlx::Pool;

use super::{JOB, Job, LEASE, POLL, audit};
use crate::live;

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
