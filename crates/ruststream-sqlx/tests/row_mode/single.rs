//! A row-mode delivery lends the row the driver read, on every stand and in every form: the whole
//! row, a retried row with its attempt counted, and a headers column taken into the delivery's
//! headers. The subject is what the delivery carries, so each test reads the deliveries off the
//! subscription itself, from rows written before it opens.

use std::collections::BTreeMap;
use std::pin::pin;

use futures::StreamExt;
use ruststream::{Carries, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::prelude::*;
use sqlx::Pool;
use sqlx::types::Json;

use super::POLL;
use crate::live;

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delivery_lends_the_row_itself() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("mail", "ops@example.com", None)]).await;
        let written: Mail = db.mails().await.remove(0);
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<Mail>::new("mail")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let delivery = deliveries.next().await.expect("a delivery").expect("the claim");
            let lent = delivery.carried().expect("the row").clone();
            // The row as the table holds it, with the id the database generated.
            assert_eq!(lent, written.leased_as(&lent));
            delivery.ack().await.expect("the delivery acknowledges");
        }
        drop(subscriber);
        assert_eq!(db.count("mail_jobs").await, 0, "the acknowledgement deleted the row");
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_row_comes_back_with_its_attempt_counted() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("mail", "ops@example.com", None)]).await;
        let written: Mail = db.mails().await.remove(0);
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<Mail>::new("mail")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let first = deliveries.next().await.expect("a delivery").expect("the claim");
            let lent = first.carried().expect("the row").clone();
            assert_eq!(
                lent,
                written.clone().leased_as(&lent),
                "the first delivery lent the row as it was written"
            );
            first.nack(true).await.expect("the retry returns the row");
            let second = deliveries.next().await.expect("a delivery").expect("the claim");
            let again = second.carried().expect("the row").clone();
            // The table counted the first delivery, and the row comes back with it.
            let counted = Mail {
                attempt: written.attempt + 1,
                ..written
            };
            assert_eq!(again, counted.leased_as(&again));
            second.ack().await.expect("the delivery acknowledges");
        }
        drop(subscriber);
        assert_eq!(db.count("mail_jobs").await, 0, "the second delivery acknowledged the row");
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_headers_column_reaches_the_delivery_and_the_row_lends_it_empty() {
        let Some(db) = database().await else { return };
        let headers = BTreeMap::from([("x-tenant".to_owned(), "acme".to_owned())]);
        db.mail(&[Mail {
            meta: Some(Json(headers)),
            ..Mail::queued("mail", "ops@example.com", None)
        }])
        .await;
        let written: Mail = db.mails().await.remove(0);
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<Mail>::new("mail")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let delivery = deliveries.next().await.expect("a delivery").expect("the claim");
            // The delivery carries the headers the row's column held, as middleware reads them.
            assert_eq!(delivery.headers().get_str("x-tenant"), Some("acme"));
            // The claim took the headers into the delivery, and left the row's column empty.
            let lent = delivery.carried().expect("the row").clone();
            let emptied = Mail {
                meta: Some(Json(BTreeMap::new())),
                ..written
            };
            assert_eq!(lent, emptied.leased_as(&lent));
            delivery.ack().await.expect("the delivery acknowledges");
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}
