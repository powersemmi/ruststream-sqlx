//! A row-mode batch lends its handler the claim's rows as one slice, `&[Row]`, in claim order, on
//! every stand and in every form: what the driver could not read and the ids without a row settle
//! after the slice by the decode policy, and a batch dropped before the runtime takes its
//! deliveries returns its rows at once.

use std::collections::BTreeMap;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{BatchSubscriber, CarriesBatch, ConnectedBroker, SubscriptionSource};
use ruststream_sqlx::prelude::*;
use sqlx::Pool;
use sqlx::types::Json;

use super::{POLL, SETTLED};
use crate::live;

/// How long a dropped batch may take to return its rows: far less than a lease, so a row that
/// waited for its lease to run out fails the test.
const RETURNED: Duration = Duration::from_secs(5);

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone()).poll_interval(POLL)
    }

    /// The recipients of `mails`, in order.
    fn recipients<'m>(mails: impl IntoIterator<Item = &'m Mail>) -> Vec<&'m str> {
        mails.into_iter().map(|mail| mail.recipient.as_str()).collect()
    }

    #[subscriber(InboxQueue::<Mail>::new("mail"), on_failure(decode = drop))]
    async fn send(mails: &[Mail]) -> Vec<HandlerOutcome> {
        // A mail whose subject asks for it goes back to the queue once; its place in the slice
        // names the delivery that settles it.
        mails
            .iter()
            .map(|mail| {
                if mail.subject.as_deref() == Some("retry") && mail.attempt == 1 {
                    HandlerOutcome::retry()
                } else {
                    HandlerOutcome::ack()
                }
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claim_is_one_slice_of_rows_in_claim_order() {
        let Some(db) = database().await else { return };
        let written = ["a", "b", "c", "d", "e", "f"].map(|to| format!("{to}@example.com"));
        let mails: Vec<Mail> = written
            .iter()
            .map(|to| Mail::queued("mail", to, None))
            .collect();
        db.mail(&mails).await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(send.batch(nonzero!(4)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the batches settle");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        assert_eq!(recipients(&lent), written, "the rows in claim order");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_batch_sizes(&[4, 2])
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("mail_jobs").await, 0, "the acknowledgements deleted every row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreadable_row_settles_after_the_slice_by_the_decode_policy() {
        let Some(db) = database().await else { return };
        db.mail(&[Mail::queued("mail", "a@example.com", Some("retry"))]).await;
        db.unreadable_mail("mail").await;
        db.mail(&[Mail::queued("mail", "b@example.com", None)]).await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(send.batch(nonzero!(4)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the batches settle");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        // The retry reached `a` and the acknowledgement `b`: the slice's places are the first
        // deliveries', and the unreadable row's delivery came after them.
        assert_eq!(
            recipients(&lent),
            ["a@example.com", "b@example.com", "a@example.com"],
            "the readable rows lent, `a` once more after its retry"
        );
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_batch_sizes(&[2, 1])
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("mail_jobs").await, 0, "the policy dropped the unreadable row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_without_a_readable_row_settles_by_the_decode_policy_alone() {
        let Some(db) = database().await else { return };
        db.unreadable_mail("mail").await;
        db.unreadable_mail("mail").await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(send.batch(nonzero!(4)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the rows settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_not_called();
        assert_eq!(db.count("mail_jobs").await, 0, "the policy dropped both rows");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<FetchedMail>::new("mail"), on_failure(decode = drop))]
    async fn fetched(_mails: &[FetchedMail]) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fetch_out_of_order_lends_the_slice_in_claim_order() {
        let Some(db) = database().await else { return };
        // The service's fetch reads the rows newest first and leaves out the `gone` one.
        db.mail(&[
            FetchedMail::queued("mail", "a@example.com", None),
            FetchedMail::queued("mail", "ops@example.com", Some("gone")),
            FetchedMail::queued("mail", "b@example.com", None),
            FetchedMail::queued("mail", "c@example.com", None),
        ])
        .await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(fetched.batch(nonzero!(4)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the batches settle");
        let lent: Vec<String> = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<FetchedMail>()
            .into_iter()
            .map(|mail| mail.recipient)
            .collect();
        assert_eq!(
            lent,
            ["a@example.com", "b@example.com", "c@example.com"],
            "claim order, not the fetch's"
        );
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        assert_eq!(db.count("mail_jobs").await, 0, "the policy dropped the missing id's row");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_headers_column_reaches_a_batch_handler_empty() {
        let Some(db) = database().await else { return };
        let headers = BTreeMap::from([("x-tenant".to_owned(), "acme".to_owned())]);
        db.mail(&[
            Mail::queued("mail", "a@example.com", None),
            Mail {
                meta: Some(Json(headers)),
                ..Mail::queued("mail", "b@example.com", None)
            },
        ])
        .await;
        let written: Vec<Mail> = db.mails().await;
        let app = RustStream::new(AppInfo::new("mailer", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(send.batch(nonzero!(4)));
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.advance(SETTLED).await.expect("the batch settles");
        let lent = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .received_values::<Mail>();
        // The claim took `b`'s headers into its delivery, and left its column empty.
        let [plain, tenanted]: [Mail; 2] = written.try_into().expect("two mails");
        let emptied = Mail {
            meta: Some(Json(BTreeMap::new())),
            ..tenanted
        };
        assert_eq!(
            lent,
            [plain.leased_as(&lent[0]), emptied.leased_as(&lent[1])],
        );
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("mail")
            .assert_batch_sizes(&[2])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_dropped_untaken_returns_its_rows_at_once() {
        let Some(db) = database().await else { return };
        let written = ["a", "b", "c"].map(|to| format!("{to}@example.com"));
        let mails: Vec<Mail> = written
            .iter()
            .map(|to| Mail::queued("mail", to, None))
            .collect();
        db.mail(&mails).await;
        let connected = broker(&db.pool).connect().await.expect("the broker connects");
        let mut subscriber = InboxQueue::<Mail>::new("mail")
            .subscribe(&connected)
            .await
            .expect("the subscription opens");
        {
            let mut batches = pin!(subscriber.batches(nonzero!(3_usize)));
            let batch = batches.next().await.expect("a batch").expect("the claim");
            assert_eq!(recipients(batch.carried()), written);
            // A handler that panicked, or an app that shut down, drops its batch untaken.
            drop(batch);
            // Each row returns as its delivery's release runs, so a claim may find some of them
            // first; the batches stay held until all three came back.
            let mut returned: Vec<String> = Vec::new();
            let mut held = Vec::new();
            tokio::time::timeout(RETURNED, async {
                while returned.len() < written.len() {
                    let batch = batches.next().await.expect("a batch").expect("the claim");
                    returned.extend(recipients(batch.carried()).into_iter().map(str::to_owned));
                    held.push(batch);
                }
            })
            .await
            .expect("the rows returned at once");
            returned.sort();
            assert_eq!(returned, written, "every row came back");
            drop(held);
        }
        drop(subscriber);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}
