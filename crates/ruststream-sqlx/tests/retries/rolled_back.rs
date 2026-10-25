//! A spent row's move that a row lock batch rolls back, in process: the row stays in its queue, and
//! the harness does not wait for it where the move would have taken it.

use std::time::Duration;

use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Pool};
use tokio::time::timeout;

use crate::live;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Letter {
    to: String,
}

/// The payload of a letter to `to`.
fn letter(to: &str) -> Vec<u8> {
    serde_json::to_vec(&Letter { to: to.to_owned() }).expect("json")
}

/// How long a settle that waits for nothing takes at most, far above what it needs.
const SETTLED: Duration = Duration::from_secs(10);

/// A constraint that keeps a long letter out of the dead-letter table, so its move fails.
const SHORT_ONLY: &str =
    "ALTER TABLE plain_jobs_dead ADD CONSTRAINT short_payload CHECK (length(payload) < 20)";

/// A letter the dead-letter table takes, and one it refuses.
fn letters() -> [Vec<u8>; 2] {
    [
        letter("ada"),
        letter("someone.with.a.long.address@example.com"),
    ]
}

live::row_lock_stands! {
    use crate::live::rows::row_lock::Plain;

    /// The dead-letter table of `plain_jobs`, read as a queue of its own.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs_dead")]
    struct PlainDead {
        #[field(id)]
        id: i64,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(payload)]
        payload: Vec<u8>,
    }

    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn unanswered(letters: &[Letter]) -> HandlerOutcome {
        let _ = letters;
        HandlerOutcome::retry()
    }

    #[subscriber(InboxQueue::<PlainDead>::new("plain.dead"))]
    async fn review(_letter: &Letter) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    fn mailer(pool: &Pool<Db>) -> RustStream {
        // A poll far above the test's own steps: the rows the rollback returns wait for it.
        let broker = SqlxBroker::new(pool.clone()).poll_interval(Duration::from_secs(5));
        RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker, |b| {
            // The review opens first, so the harness counts what the batch moves to it.
            b.include(review);
            b.include(unanswered.batch(nonzero!(3)))
                .max_attempts(nonzero!(1u32))
                .dead_letter("plain_jobs_dead");
        })
    }

    // One batch of two spent rows, each moved by its own statement in the batch's transaction. The
    // dead-letter table refuses the second row, so the batch rolls back and the first row's move,
    // which succeeded, is undone with it: both rows stay in `plain_jobs`. A count left for the move
    // would keep the settle waiting for a row that never reached the dead-letter table; the
    // timeout turns that into a failure.
    #[tokio::test]
    async fn in_process_a_move_the_batch_rolled_back_is_not_waited_for() {
        let Some(db) = database().await else { return };
        sqlx::query(SHORT_ONLY)
            .execute(&db.pool)
            .await
            .expect("the dead-letter table takes short payloads alone");
        db.plain(&letters()).await;
        let tb = TestApp::start(mailer(&db.pool)).await.expect("the app starts");
        timeout(SETTLED, tb.settle())
            .await
            .expect("the settle does not wait for a move the rollback undid")
            .expect("the batch settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain.dead")
            .assert_not_called();
        assert_eq!(db.count("plain_jobs").await, 2, "both rows stay in their queue");
        assert_eq!(db.count("plain_jobs_dead").await, 0, "no row reached the dead-letter table");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // The same batch where the dead-letter table takes both rows: the batch commits, both moves
    // stand, and the harness waits for the review of each.
    #[tokio::test]
    async fn in_process_the_moves_of_a_committed_batch_are_waited_for() {
        let Some(db) = database().await else { return };
        db.plain(&letters()).await;
        let tb = TestApp::start(mailer(&db.pool)).await.expect("the app starts");
        tb.settle().await.expect("the batch and the reviews settle");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain.dead")
            .assert_called(2);
        assert_eq!(db.count("plain_jobs").await, 0, "both rows moved");
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}
