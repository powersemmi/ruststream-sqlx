//! The delivery's headers hold the headers struct's fields without a role under their columns'
//! names, built on the first read: `tenant` and `order_id`, and `trace` only where it is not
//! `NULL`; the mechanics stay in the typed struct. A batch builds each delivery's headers the
//! same way as a single delivery does.

use std::pin::pin;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, SubscriptionSource};
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::Pool;

use super::{POLL, SETTLED};
use crate::live;

/// The header contract of an order's job, as a handler reads it.
#[derive(Debug, Deserialize, PartialEq)]
struct OrderMeta {
    tenant: String,
    trace: Option<String>,
    order_id: String,
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    /// The header contract `job`'s fields without a role spell.
    fn meta_of(job: &OrderJob) -> OrderMeta {
        OrderMeta {
            tenant: job.headers.tenant.clone(),
            trace: job.headers.trace.clone(),
            order_id: job.headers.order_id.to_string(),
        }
    }

    #[subscriber(InboxQueue::<OrderJob>::new("orders"), on_failure(decode = drop))]
    async fn typed(job: &OrderJob, Headers(meta): Headers<OrderMeta>) -> HandlerOutcome {
        // The headers spell the row's own fields: an acknowledgement says they do.
        if meta == meta_of(job) {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::retry()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_typed_header_contract_reads_the_headers_structs_fields() {
        let Some(db) = database().await else { return };
        db.mail(&[
            OrderJob::queued("orders", "acme", Some("t-1"), 7).headers,
            OrderJob::queued("orders", "globex", None, 8).headers,
        ])
        .await;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(typed);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the rows settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("headed_jobs").await, 0, "both rows were acknowledged");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
    async fn listed(job: &OrderJob, ctx: &mut Context<'_>) -> HandlerOutcome {
        let names: Vec<&str> = ctx.headers().iter().map(|(name, _)| name).collect();
        // The mechanics stay in the typed struct; a `NULL` trace is no header.
        let expected: &[&str] = if job.headers.trace.is_some() {
            &["tenant", "trace", "order_id"]
        } else {
            &["tenant", "order_id"]
        };
        if names == expected {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::drop()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_headers_are_the_fields_without_a_role_and_a_null_is_left_out() {
        let Some(db) = database().await else { return };
        db.mail(&[
            OrderJob::queued("orders", "acme", Some("t-1"), 7).headers,
            OrderJob::queued("orders", "globex", None, 8).headers,
        ])
        .await;
        let app = RustStream::new(AppInfo::new("shop", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(listed);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the rows settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // The subject is what each delivery of a batch carries, which a batch handler does not see:
    // the deliveries are read off the subscription itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_delivery_of_a_batch_builds_its_own_headers() {
        let Some(db) = database().await else { return };
        db.mail(&[
            OrderJob::queued("orders", "acme", Some("t-1"), 7).headers,
            OrderJob::queued("orders", "globex", None, 8).headers,
        ])
        .await;
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<OrderJob>::new("orders")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut batches = pin!(subscriber.batches(nonzero!(2_usize)));
            let batch = batches.next().await.expect("a batch").expect("the claim");
            let mut seen = Vec::new();
            for delivery in batch {
                let headers = delivery.headers();
                seen.push((
                    headers.get_str("tenant").map(str::to_owned),
                    headers.get_str("trace").map(str::to_owned),
                    headers.get_str("order_id").map(str::to_owned),
                ));
                delivery.ack().await.expect("the delivery settles");
            }
            assert_eq!(
                seen,
                [
                    (Some("acme".to_owned()), Some("t-1".to_owned()), Some("7".to_owned())),
                    (Some("globex".to_owned()), None, Some("8".to_owned())),
                ]
            );
        }
        drop(subscriber);
        assert_eq!(db.count("headed_jobs").await, 0, "both deliveries were acknowledged");
        connected.shutdown().await.expect("the broker stops");
        db.finish().await;
    }
}
