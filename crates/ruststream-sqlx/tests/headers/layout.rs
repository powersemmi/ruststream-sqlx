//! A message assembled from a headers struct, run as an application through
//! `TestApp::start_live` against each stand and form: the handler takes the assembled row, a
//! retried row comes back with its attempt counted, a batch handler takes the rows as one slice,
//! and a message column the table lacks stops the app at startup.

use ruststream::testing::TestApp;
use ruststream_sqlx::prelude::*;
use sqlx::{AssertSqlSafe, Pool};

use super::{POLL, SETTLED};
use crate::live;

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    /// Writes `jobs` through their headers struct's generated insert, then each one's note: the
    /// message's own column, which the headers struct does not write.
    async fn write(db: &live::Database<Db>, jobs: &[OrderJob]) {
        let headers: Vec<OrderHeaders> = jobs.iter().map(|job| job.headers.clone()).collect();
        db.mail(&headers).await;
        for job in jobs {
            if let Some(note) = &job.note {
                sqlx::raw_sql(AssertSqlSafe(format!(
                    "UPDATE headed_jobs SET note = '{note}' WHERE order_id = {}",
                    job.headers.order_id
                )))
                .execute(&db.pool)
                .await
                .expect("the note writes");
            }
        }
    }

    /// The jobs as the table holds them, in id order.
    async fn written(db: &live::Database<Db>) -> Vec<OrderJob> {
        sqlx::query_as("SELECT * FROM headed_jobs ORDER BY job_id")
            .fetch_all(&db.pool)
            .await
            .expect("the jobs read")
    }

    #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
    async fn take(job: &OrderJob) -> HandlerOutcome {
        // A job whose note asks for it goes back to the queue once.
        if job.note.as_deref() == Some("retry") && job.headers.attempt == 1 {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_takes_the_row_its_headers_struct_and_its_own_columns_assemble() {
        let Some(db) = database().await else { return };
        write(&db, &[OrderJob::queued("orders", "acme", Some("t-1"), 7).noted("fragile")]).await;
        let written = written(&db).await.remove(0);
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(take);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the row settles");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .received_values::<OrderJob>();
        assert_eq!(lent.len(), 1, "{lent:?}");
        assert_eq!(lent[0].note.as_deref(), Some("fragile"), "the default fetch read the note");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .with_value(&written.leased_as(&lent[0]))
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "the acknowledgement deleted the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_row_comes_back_with_its_attempt_counted() {
        let Some(db) = database().await else { return };
        write(&db, &[OrderJob::queued("orders", "acme", None, 7).noted("retry")]).await;
        let written = written(&db).await.remove(0);
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(take);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("both deliveries settle");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .received_values::<OrderJob>();
        assert_eq!(lent.len(), 2, "the retry brought the row back once: {lent:?}");
        assert_eq!(lent[1].headers.attempt, 2, "the table counted the first delivery");
        let counted = OrderJob {
            headers: OrderHeaders {
                attempt: written.headers.attempt + 1,
                ..written.headers.clone()
            },
            ..written
        };
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called(2)
            .with_value(&counted.leased_as(&lent[1]))
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "the second delivery acknowledged the row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
    async fn take_all(jobs: &[OrderJob]) -> Vec<HandlerOutcome> {
        jobs.iter().map(|_| HandlerOutcome::ack()).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_handler_takes_the_rows_as_one_slice_in_claim_order() {
        let Some(db) = database().await else { return };
        let jobs: Vec<OrderJob> = (1..=5)
            .map(|order| OrderJob::queued("orders", "acme", None, order).noted("batched"))
            .collect();
        write(&db, &jobs).await;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(take_all.batch(nonzero!(3)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the batches settle");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .received_values::<OrderJob>();
        let orders: Vec<i64> = lent.iter().map(|job| job.headers.order_id).collect();
        assert_eq!(orders, [1, 2, 3, 4, 5], "the rows in claim order");
        assert!(lent.iter().all(|job| job.note.as_deref() == Some("batched")));
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_batch_sizes(&[3, 2])
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "the acknowledgements deleted every row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<MissingJob>::new("orders"))]
    async fn misread(_job: &MissingJob) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_message_column_the_table_lacks_stops_the_app_at_startup() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(misread);
            });
        let Err(refused) = TestApp::start_live(app).await else {
            panic!("the app started with a column its table lacks");
        };
        let refused = format!("{refused:?}");
        // The default fetch names `missing`, and the database refused the statement that reads it.
        assert!(
            refused.contains("Schema") && refused.contains("\"orders\"") && refused.contains("missing"),
            "{refused}"
        );
        db.finish().await;
    }
}
