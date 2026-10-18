//! A message assembled from a headers struct, against each stand and form: a delivery lends the
//! assembled row, a retried row comes back with its attempt counted, a batch lends the rows as one
//! slice, and a message column the table lacks stops the app at startup. What a delivery lends is
//! read off the subscription itself, from rows written before it opens.

use std::pin::pin;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{
    BatchSubscriber, Carries, CarriesBatch, ConnectedBroker, IncomingMessage, Subscriber,
    SubscriptionSource,
};
use ruststream_sqlx::prelude::*;
use sqlx::{AssertSqlSafe, Pool};

use super::POLL;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_lends_the_row_its_headers_struct_and_its_own_columns_assemble() {
        let Some(db) = database().await else { return };
        write(&db, &[OrderJob::queued("orders", "acme", Some("t-1"), 7).noted("fragile")]).await;
        let written = written(&db).await.remove(0);
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<OrderJob>::new("orders")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let delivery = deliveries.next().await.expect("a delivery").expect("the claim");
            let lent = delivery.carried().expect("the row").clone();
            assert_eq!(lent.note.as_deref(), Some("fragile"), "the default fetch read the note");
            assert_eq!(lent, written.leased_as(&lent));
            delivery.ack().await.expect("the delivery acknowledges");
        }
        drop(subscriber);
        assert_eq!(db.count("headed_jobs").await, 0, "the acknowledgement deleted the row");
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_row_comes_back_with_its_attempt_counted() {
        let Some(db) = database().await else { return };
        write(&db, &[OrderJob::queued("orders", "acme", None, 7).noted("retry")]).await;
        let written = written(&db).await.remove(0);
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<OrderJob>::new("orders")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let first = deliveries.next().await.expect("a delivery").expect("the claim");
            first.nack(true).await.expect("the retry returns the row");
            let second = deliveries.next().await.expect("a delivery").expect("the claim");
            let lent = second.carried().expect("the row").clone();
            assert_eq!(lent.headers.attempt, 2, "the table counted the first delivery");
            let counted = OrderJob {
                headers: OrderHeaders {
                    attempt: written.headers.attempt + 1,
                    ..written.headers.clone()
                },
                ..written
            };
            assert_eq!(lent, counted.leased_as(&lent));
            second.ack().await.expect("the delivery acknowledges");
        }
        drop(subscriber);
        assert_eq!(db.count("headed_jobs").await, 0, "the second delivery acknowledged the row");
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_lends_the_rows_as_one_slice_in_claim_order() {
        let Some(db) = database().await else { return };
        let jobs: Vec<OrderJob> = (1..=5)
            .map(|order| OrderJob::queued("orders", "acme", None, order).noted("batched"))
            .collect();
        write(&db, &jobs).await;
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<OrderJob>::new("orders")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut batches = pin!(subscriber.batches(nonzero!(3_usize)));
            let mut orders = Vec::new();
            let mut sizes = Vec::new();
            while orders.len() < jobs.len() {
                let batch = batches.next().await.expect("a batch").expect("the claim");
                sizes.push(batch.carried().len());
                for job in batch.carried() {
                    assert_eq!(job.note.as_deref(), Some("batched"));
                    orders.push(job.headers.order_id);
                }
                for delivery in batch {
                    delivery.ack().await.expect("the delivery acknowledges");
                }
            }
            assert_eq!(orders, [1, 2, 3, 4, 5], "the rows in claim order");
            assert_eq!(sizes, [3, 2], "a full claim, then the rest");
        }
        drop(subscriber);
        assert_eq!(db.count("headed_jobs").await, 0, "the acknowledgements deleted every row");
        connected.shutdown().await.expect("the broker shuts down");
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
