//! A message assembled by a fetch of the service's own, which joins each job of `headed_jobs` to
//! its order in `customer_orders`, run against each stand and form: the handler takes the joined
//! row; a job whose order is missing joins no row, so its delivery fails to decode, the decode
//! policy settles it and its headers are empty; a batch lends the rows that joined and settles the
//! rest by the policy.

use std::pin::pin;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, SubscriptionSource};
use ruststream_sqlx::prelude::*;
use sqlx::{AssertSqlSafe, Pool};

use super::{POLL, SETTLED};
use crate::live;

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    /// Writes a job of the queue `orders` for each of `orders`, through the headers struct's
    /// generated insert, then the orders of `placed` as `(id, customer, total)`.
    async fn write(db: &live::Database<Db>, orders: &[i64], placed: &[(i64, &str, i64)]) {
        let jobs: Vec<OrderHeaders> = orders
            .iter()
            .map(|&order| OrderJob::queued("orders", "acme", None, order).headers)
            .collect();
        db.mail(&jobs).await;
        for (id, customer, total) in placed {
            sqlx::raw_sql(AssertSqlSafe(format!(
                "INSERT INTO customer_orders (id, customer, total) VALUES ({id}, '{customer}', {total})"
            )))
            .execute(&db.pool)
            .await
            .expect("the order writes");
        }
    }

    /// The jobs joined to their orders as the tables hold them, in id order.
    async fn joined(db: &live::Database<Db>) -> Vec<OrderMail> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT {ORDERED} FROM headed_jobs j JOIN customer_orders o ON o.id = j.order_id \
             ORDER BY j.job_id"
        )))
        .fetch_all(&db.pool)
        .await
        .expect("the joined jobs read")
    }

    #[subscriber(InboxQueue::<OrderMail>::new("orders"), on_failure(decode = drop))]
    async fn take(_mail: &OrderMail, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
        // A first delivery reports the attempt as it stood before its claim, whatever the row
        // the service's fetch read holds by then.
        if attempt == Some(1) {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::retry()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handler_takes_the_row_the_services_fetch_joins() {
        let Some(db) = database().await else { return };
        write(&db, &[7], &[(7, "ada", 1200)]).await;
        let written = joined(&db).await.remove(0);
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(take);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the row settles");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .received_values::<OrderMail>();
        assert_eq!(lent.len(), 1, "{lent:?}");
        assert_eq!((lent[0].customer.as_str(), lent[0].total), ("ada", 1200), "the join read the order");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .with_value(&written.claimed_as(&lent[0]))
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "the acknowledgement deleted the job");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_whose_order_is_missing_settles_by_the_decode_policy() {
        let Some(db) = database().await else { return };
        // No order 8: the claim takes the job's id, and the join finds no row for it.
        write(&db, &[8], &[]).await;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(take);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the job settles");
        // The handler would acknowledge or retry: a drop is the policy's.
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop())
            .assert_last_failed_to_decode();
        assert_eq!(db.count("headed_jobs").await, 0, "the policy dropped the job");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // The subject is what the delivery of a missing row carries, which no handler sees: the
    // deliveries are read off the subscription itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_delivery_of_a_missing_row_has_no_headers() {
        let Some(db) = database().await else { return };
        write(&db, &[7, 8], &[(7, "ada", 1200)]).await;
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<OrderMail>::new("orders")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut batches = pin!(subscriber.batches(nonzero!(2_usize)));
            let batch = batches.next().await.expect("a batch").expect("the claim");
            let mut seen = Vec::new();
            for delivery in batch {
                let headers = delivery.headers();
                seen.push((headers.len(), headers.get_str("order_id").map(str::to_owned)));
                delivery.ack().await.expect("the delivery settles");
            }
            // The joined job's headers, then none for the job whose order is missing.
            assert_eq!(seen, [(2, Some("7".to_owned())), (0, None)]);
        }
        drop(subscriber);
        assert_eq!(db.count("headed_jobs").await, 0, "both deliveries were acknowledged");
        connected.shutdown().await.expect("the broker stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<OrderMail>::new("orders"), on_failure(decode = drop))]
    async fn take_all(mails: &[OrderMail]) -> Vec<HandlerOutcome> {
        mails.iter().map(|_| HandlerOutcome::ack()).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_lends_the_joined_rows_and_settles_a_missing_one_by_the_policy() {
        let Some(db) = database().await else { return };
        // Three jobs, and no order 8.
        write(&db, &[7, 8, 9], &[(7, "ada", 1200), (9, "grace", 300)]).await;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(take_all.batch(nonzero!(3)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the batch settles");
        let lent: Vec<(i64, String)> = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .received_values::<OrderMail>()
            .into_iter()
            .map(|mail| (mail.headers.order_id, mail.customer))
            .collect();
        assert_eq!(lent, [(7, "ada".to_owned()), (9, "grace".to_owned())], "the joined rows");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_batch_sizes(&[2])
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "the policy dropped the missing order's job");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
