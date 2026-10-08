//! A subscription's claims never take the pool's last connection, in every form, on every stand.

use std::time::Duration;

use futures::StreamExt;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};

use crate::live;

/// Rows enough to fill the stands' pools of eight twice over.
const ROWS: usize = 16;

/// How long a stream stays quiet before the suite takes it to wait: no claim on a stand takes
/// this long.
const QUIET: Duration = Duration::from_millis(500);

/// How long a connection of the pool may take to lend: well below the pool's acquire timeout, so
/// a pool its subscription emptied fails the test instead of waiting it out.
const LEND: Duration = Duration::from_secs(5);

live::matrix! {
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_subscription_leaves_the_pool_its_last_connection() {
        let Some(db) = database().await else { return };
        db.plain_ids(ROWS).await;
        let size = usize::try_from(db.pool.options().get_max_connections()).expect("fits");
        let connected = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_secs(60))
            .connect()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("opens");
        {
            let mut deliveries = std::pin::pin!(subscriber.stream());
            // Every delivery stays in work, as with workers that never finish.
            let mut held = Vec::new();
            while let Ok(Some(next)) = tokio::time::timeout(QUIET, deliveries.next()).await {
                held.push(next.expect("a claim"));
            }
            // A lease holds no connection, so every row goes into work; a delivery of the other
            // forms holds one, and the subscription stops one short of the pool.
            let expected = if LEASED { ROWS } else { size - 1 };
            assert_eq!(held.len(), expected, "deliveries in work");
            let lent = tokio::time::timeout(LEND, db.pool.acquire()).await;
            let conn = lent.expect("the pool lends its last connection").expect("a connection");
            drop(conn);
            if !LEASED {
                // A settlement makes room, and the next claim takes it.
                held.pop().expect("a delivery").ack().await.expect("the ack");
                let next = tokio::time::timeout(LEND, deliveries.next()).await;
                held.push(next.expect("a claim after the settlement").expect("a row").expect("a claim"));
            }
            for delivery in held {
                delivery.ack().await.expect("the ack");
            }
        }
        drop(subscriber);
        connected.shutdown().await.expect("stops");
        db.finish().await;
    }
}
