//! Two claimers on one table never take one row, in either form, on every stand.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::collections::BTreeSet;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};

const ROWS: usize = 200;

/// The id a delivery's payload names: each row of the suite carries its own id as text.
trait PayloadId {
    fn payload_id(&self) -> i64;
}

impl<Delivery: IncomingMessage> PayloadId for Delivery {
    fn payload_id(&self) -> i64 {
        str::from_utf8(self.payload())
            .expect("the payload is text")
            .parse()
            .expect("the payload is an id")
    }
}

live::matrix! {
    // Real concurrency: two brokers, each with its own claim loop, on a multi-threaded runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_claimers_never_share_a_row() {
        let Some(db) = database().await else { return };
        let ids: BTreeSet<i64> = db.plain_ids(ROWS).await;
        let claim = async move |pool: sqlx::Pool<Db>| -> Vec<i64> {
            let connected = SqlxBroker::new(pool)
                .poll_interval(Duration::from_millis(10))
                .connect()
                .await
                .expect("connects");
            let mut subscriber = InboxQueue::<Plain>::new("plain")
                .subscribe(&connected)
                .await
                .expect("opens");
            let mut taken = Vec::new();
            {
                let mut deliveries = pin!(subscriber.stream());
                while let Ok(Some(next)) =
                    tokio::time::timeout(Duration::from_millis(500), deliveries.next()).await
                {
                    let delivery = next.expect("a claim");
                    taken.push(delivery.payload_id());
                    delivery.ack().await.expect("the ack");
                }
            }
            drop(subscriber);
            connected.shutdown().await.expect("stops");
            taken
        };
        let (first, second) = tokio::join!(
            tokio::spawn(claim(db.pool.clone())),
            tokio::spawn(claim(db.pool.clone())),
        );
        let (first, second) = (first.expect("joins"), second.expect("joins"));
        let shared: Vec<_> = first.iter().filter(|id| second.contains(id)).collect();
        assert!(shared.is_empty(), "rows delivered twice: {shared:?}");
        let all: BTreeSet<i64> = first.into_iter().chain(second).collect();
        assert_eq!(all, ids, "every row was delivered once");
        db.finish().await;
    }
}
