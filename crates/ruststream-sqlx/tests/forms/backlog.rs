//! Rows a service wrote with its own SQL before the test started, run as an application through
//! `TestApp::start` against each stand and form: the harness waits for the rows the subscription
//! can claim when it opens, and not for rows it cannot claim yet.

use std::time::Duration;

use chrono::{TimeDelta, Utc};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{InboxQueue, Insert, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::Pool;
use tokio::time::timeout;

use crate::live;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Email {
    to: String,
}

/// The payload of an email to `to`.
fn email(to: &str) -> Vec<u8> {
    serde_json::to_vec(&Email { to: to.to_owned() }).expect("json")
}

/// How long a settle that waits for nothing takes at most, far above what it needs.
const SETTLED: Duration = Duration::from_secs(10);

live::matrix! {
    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn send(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    fn mailer(pool: &Pool<Db>) -> RustStream {
        let broker = SqlxBroker::new(pool.clone()).poll_interval(Duration::from_millis(20));
        RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker, |b| {
            b.include(send);
        })
    }

    /// Writes `jobs` with the service's own SQL, as a seed or a migration would.
    async fn seed(pool: &Pool<Db>, jobs: Vec<SendEmail>) {
        let mut conn = pool.acquire().await.expect("the pool lends a connection");
        for job in jobs {
            job.insert(&mut *conn).await.expect("the job writes");
        }
    }

    // The test runs on one thread, so nothing runs between the settle's return and the assertion:
    // a harness that did not count the rows returns before the first claim.
    #[tokio::test]
    async fn in_process_the_harness_waits_for_rows_written_before_the_app_started() {
        let Some(db) = database().await else { return };
        seed(
            &db.pool,
            vec![
                SendEmail::queued("emails", email("ada@example.com")),
                SendEmail::queued("emails", email("grace@example.com")),
                SendEmail::queued("emails", email("edsger@example.com")),
            ],
        )
        .await;
        let tb = TestApp::start(mailer(&db.pool)).await.expect("the app starts");
        tb.settle().await.expect("the rows settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(3);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // A row of another group, and a row of the subscription's group due in an hour: neither is
    // claimable when the subscription opens, so the harness waits for the one due row alone. A
    // count of either would keep the settle waiting; the timeout turns that into a failure.
    #[tokio::test]
    async fn in_process_the_harness_waits_only_for_rows_the_subscription_can_claim() {
        let Some(db) = database().await else { return };
        let later = SendEmail {
            retry_after: Utc::now() + TimeDelta::hours(1),
            ..SendEmail::queued("emails", email("later@example.com"))
        };
        seed(
            &db.pool,
            vec![
                SendEmail::queued("invoices", email("billing@example.com")),
                later,
                SendEmail::queued("emails", email("ada@example.com")),
            ],
        )
        .await;
        let tb = TestApp::start(mailer(&db.pool)).await.expect("the app starts");
        timeout(SETTLED, tb.settle())
            .await
            .expect("the settle waits for no row the subscription cannot claim")
            .expect("the row settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once()
            .with(&Email {
                to: "ada@example.com".to_owned(),
            });
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
