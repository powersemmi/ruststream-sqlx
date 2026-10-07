//! A lease table whose transactions open at REPEATABLE READ, with a handler that outlives a lease
//! extension inside its transaction.

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase};
use sqlx::{Error, FromRow, Pool};

use super::{JOB, Job, LEASE, POLL, audit};
use crate::live;

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
        let app =
            RustStream::new(AppInfo::new("transactional", "0.0.0")).with_broker(broker(&db), |b| {
                b.include(outlasting.transactional());
            });
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
        let app = RustStream::new(AppInfo::new("plain", "0.0.0")).with_broker(broker(&db), |b| {
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
