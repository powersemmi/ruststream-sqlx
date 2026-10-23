//! A spent row's move in process: the harness waits for the subscription that reads where the row
//! moved, a dead-letter group or a dead-letter table, as it waits for any other row.

use std::num::NonZeroU32;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Pool};
use tokio::time::timeout;

use crate::live;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Letter {
    to: String,
}

fn letter() -> Letter {
    Letter {
        to: "ada@down.example".to_owned(),
    }
}

/// How long a publish that waits for nothing takes at most, far above what it needs.
const SETTLED: Duration = Duration::from_secs(10);

/// Past the broker's own poll interval, a second.
const PAST_A_POLL: Duration = Duration::from_secs(2);

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<SendEmail>("emails")
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
    async fn unanswered(_letter: &Letter) -> HandlerOutcome {
        HandlerOutcome::retry()
    }

    #[subscriber(InboxQueue::<SendEmail>::new("emails.dead"))]
    async fn review(_letter: &Letter) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    fn mailer(pool: &Pool<Db>, attempts: NonZeroU32) -> RustStream {
        RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker(pool), |b| {
            b.include(unanswered)
                .max_attempts(attempts)
                .dead_letter("emails.dead");
            b.include(review);
        })
    }

    // The test runs on one thread, so nothing else runs between the publish's return and the
    // assertions, and the dead-letter group's subscription reaches the moved row only at its next
    // poll: a harness that did not count the move returns before the claim.
    #[tokio::test]
    async fn in_process_the_harness_waits_for_a_row_moved_to_the_dead_letter_group() {
        let Some(db) = database().await else { return };
        let tb = TestApp::start(mailer(&db.pool, nonzero!(1u32)))
            .await
            .expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&letter())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called_once();
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails.dead")
            .assert_called_once()
            .with(&letter())
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // The same service on a paused clock: five deliveries, the fifth moving the row, which the
    // dead-letter group's subscription takes once the clock passes its poll.
    #[tokio::test]
    async fn in_process_on_a_paused_clock_the_dead_letter_group_takes_the_moved_row() {
        let Some(db) = database().await else { return };
        tokio::time::pause();
        let tb = TestApp::start(mailer(&db.pool, nonzero!(5u32)))
            .await
            .expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&letter())
            .to("emails")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails")
            .assert_called(5)
            .with(&letter());
        tb.advance(PAST_A_POLL).await.expect("the clock moves");
        tb.settle().await.expect("the deliveries settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("emails.dead")
            .assert_called_once()
            .with(&letter())
            .settled(HandlerOutcome::ack());
        let rows = live::unpaused(db.email_rows("email_jobs")).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].0.as_str(), rows[0].3),
            ("emails.dead", true),
            "the review finished the moved row"
        );
        tb.shutdown().await.expect("the app stops");
        live::unpaused(db.finish()).await;
    }
}

/// A table without groups moves its spent rows into a table of their own.
mod into_a_table {
    use super::*;

    live::lease_stands! {
        /// The dead-letter table of `plain_jobs`, read as a queue of its own.
        #[derive(Debug, Inbox, FromRow)]
        #[inbox(table = "plain_jobs_dead")]
        struct PlainDead {
            #[field(id)]
            id: i64,
            #[field(attempt, generated)]
            attempt: i16,
            #[field(locked_until)]
            locked_until: Option<DateTime<Utc>>,
            #[field(payload)]
            payload: Vec<u8>,
        }

        #[subscriber(InboxQueue::<Plain>::new("plain"))]
        async fn plain_unanswered(_letter: &Letter) -> HandlerOutcome {
            HandlerOutcome::retry()
        }

        #[subscriber(InboxQueue::<PlainDead>::new("plain.dead"))]
        async fn plain_review(_letter: &Letter) -> HandlerOutcome {
            HandlerOutcome::ack()
        }

        fn plain_mailer(pool: &Pool<Db>) -> RustStream {
            let broker = SqlxBroker::new(pool.clone())
                .poll_interval(Duration::from_millis(20))
                .route::<Plain>("plain");
            RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker, |b| {
                b.include(plain_unanswered)
                    .max_attempts(nonzero!(1u32))
                    .dead_letter("plain_jobs_dead");
                b.include(plain_review);
            })
        }

        // As for a group, on one thread: a harness that did not count the move into the table returns
        // before the table's subscription claims the row.
        #[tokio::test]
        async fn in_process_the_harness_waits_for_a_row_moved_into_the_dead_letter_table() {
            let Some(db) = database().await else { return };
            let tb = TestApp::start(plain_mailer(&db.pool))
                .await
                .expect("the app starts");
            tb.broker::<SqlxBroker<Db>>()
                .message(&letter())
                .to("plain")
                .publish()
                .await
                .expect("the publish settles");
            tb.broker::<SqlxBroker<Db>>()
                .subscriber("plain")
                .assert_called_once();
            tb.broker::<SqlxBroker<Db>>()
                .subscriber("plain.dead")
                .assert_called_once()
                .with(&letter())
                .settled(HandlerOutcome::ack());
            assert_eq!(db.count("plain_jobs_dead").await, 0, "the reviewed row left the table");
            let _ = |row: PlainDead| (row.id, row.attempt, row.locked_until, row.payload);
            tb.shutdown().await.expect("the app stops");
            db.finish().await;
        }

        // A move whose table went away fails and leaves the row where it was: nothing reaches the
        // dead-letter table, and the harness does not wait for it. A count left for that table
        // would keep the publish waiting; the timeout turns that into a failure.
        #[tokio::test]
        async fn in_process_a_move_that_failed_is_not_waited_for() {
            let Some(db) = database().await else { return };
            let tb = TestApp::start(plain_mailer(&db.pool))
                .await
                .expect("the app starts");
            sqlx::query("ALTER TABLE plain_jobs_dead RENAME TO plain_jobs_gone")
                .execute(&db.pool)
                .await
                .expect("the dead-letter table goes");
            let letter = letter();
            let published = tb
                .broker::<SqlxBroker<Db>>()
                .message(&letter)
                .to("plain")
                .publish();
            timeout(SETTLED, published)
                .await
                .expect("the publish returns without waiting for the row")
                .expect("the publish settles");
            tb.broker::<SqlxBroker<Db>>()
                .subscriber("plain")
                .assert_called_once();
            tb.broker::<SqlxBroker<Db>>()
                .subscriber("plain.dead")
                .assert_not_called();
            assert_eq!(db.count("plain_jobs").await, 1, "the row stayed in its queue");
            tb.shutdown().await.expect("the app stops");
            db.finish().await;
        }
    }
}
