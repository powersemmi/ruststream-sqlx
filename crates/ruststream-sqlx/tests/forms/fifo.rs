//! FIFO groups on every stand and form: a group keeps its order under `workers(n)`, a head that
//! retries with a delay moves behind the rows of its group due earlier, and while a row of a group
//! is in work no other row of the group is claimed, not the row behind it, not a row that enters
//! the group ahead of it, and not by a second broker. The advisory lock form keeps a group in order
//! by a lock key that names the group (`ledger-{account}`), and its rows behave alike.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

use std::fmt::Display;
use std::pin::{Pin, pin};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use ruststream::prelude::*;
use ruststream::testing::{InProcess, TestApp};
use ruststream::{ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::dialect::{Dialect, Opening, Param, Statement};
use ruststream_sqlx::keys::Attempt;
use ruststream_sqlx::{InboxQueue, InboxRow, Insert, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, Pool, Row};

use crate::live;
use crate::live::rows::PUBLISHED_PRIORITY;

const POLL: Duration = Duration::from_millis(20);

/// How long the head that retries waits before it comes back: long enough for the two rows
/// published after it to be written and handled while it waits, on a loaded stand too.
const DELAY: Duration = Duration::from_secs(1);

/// The group the suite's rows belong to.
const ACCOUNT: &str = "acct-a";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Outgoing)]
struct Posting {
    n: u32,
}

/// Writes `entry` into the ledger on a connection of `pool`, as a producer outside the service
/// would.
async fn write<DB, Row>(pool: &Pool<DB>, entry: Row)
where
    DB: sqlx::Database,
    Row: Insert<DB::Connection>,
{
    let mut conn = pool.acquire().await.expect("a connection");
    entry.insert(&mut conn).await.expect("the entry writes");
}

/// Asserts that `deliveries` hands out nothing over three claims.
///
/// On a paused clock an in-process connection's claims run while the clock stands still, so a wait
/// of two and a half poll intervals spans three claims and ends while the stream waits for its
/// next one, with no claim cut off midway.
async fn assert_claims_nothing<Deliveries, Delivery, Failure>(deliveries: &mut Pin<&mut Deliveries>)
where
    Deliveries: Stream<Item = Result<Delivery, Failure>>,
    Failure: Display,
{
    tokio::time::pause();
    let claimed = tokio::time::timeout(POLL * 5 / 2, deliveries.next()).await;
    tokio::time::resume();
    match claimed {
        Err(_) => {}
        Ok(Some(Ok(_))) => panic!("a second row of the group went into work beside the first"),
        Ok(Some(Err(failed))) => panic!("a claim failed: {failed}"),
        Ok(None) => panic!("the stream ended"),
    }
}

live::matrix! {
    fn broker(pool: &Pool<Db>) -> SqlxBroker<Db> {
        SqlxBroker::new(pool.clone())
            .poll_interval(POLL)
            .route::<Entry>(ACCOUNT)
    }

    // Earlier postings take longer, so a row of the group in work beside an earlier one would
    // finish first and break the order the harness records.
    #[subscriber(InboxQueue::<Entry>::new(ACCOUNT))]
    async fn post(posting: &Posting) -> HandlerOutcome {
        let work = 9_u32.saturating_sub(posting.n) * 10;
        tokio::time::sleep(Duration::from_millis(u64::from(work))).await;
        HandlerOutcome::ack()
    }

    // In process, each publish reaches the table in the order the test made it and the eight
    // settle together, so the rows wait in the table side by side while four workers poll the
    // group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_group_keeps_its_order_under_workers() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("ledger", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(post.workers(nonzero!(4)));
            });
        let tb = TestApp::start(app).await.expect("the app starts");
        let postings: Vec<Posting> = (1..=8).map(|n| Posting { n }).collect();
        try_join_all(postings.iter().map(|posting| {
            tb.broker::<SqlxBroker<Db>>()
                .message(posting)
                .to(ACCOUNT)
                .publish()
        }))
        .await
        .expect("the postings settle");
        let order: Vec<u32> = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber(ACCOUNT)
            .received::<Posting>()
            .into_iter()
            .map(|posting| posting.n)
            .collect();
        assert_eq!(order, (1..=8).collect::<Vec<_>>());
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Entry>::new(ACCOUNT))]
    async fn deferred(posting: &Posting, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        if posting.n == 1 && attempt == Some(1) {
            HandlerOutcome::retry_after(DELAY)
        } else {
            HandlerOutcome::ack()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_delayed_head_moves_behind_rows_due_earlier() {
        let Some(db) = database().await else { return };
        let app = RustStream::new(AppInfo::new("ledger", "0.0.0"))
            .with_broker(broker(&db.pool), |b| {
                b.include(deferred);
            });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        // Each publish returns once its posting was handled: the first one retried, the next two
        // published while it waits.
        for n in 1..=3 {
            tb.broker::<SqlxBroker<Db>>()
                .message(&Posting { n })
                .to(ACCOUNT)
                .publish()
                .await
                .expect("the posting settles");
        }
        tb.advance(DELAY + Duration::from_millis(100))
            .await
            .expect("the delayed head comes back");
        let order: Vec<u32> = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber(ACCOUNT)
            .received::<Posting>()
            .into_iter()
            .map(|posting| posting.n)
            .collect();
        assert_eq!(order, [1, 2, 3, 1]);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    // The clock is paused while the second broker claims, so it runs on the current thread.
    #[tokio::test]
    async fn a_row_entering_ahead_of_the_head_waits() {
        let Some(db) = database().await else { return };
        write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY, Utc::now(), b"head")).await;
        write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY, Utc::now(), b"next")).await;
        let holder = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let other = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .connect_in_process()
            .await
            .expect("the broker connects");
        let mut holding = InboxQueue::<Entry>::new(ACCOUNT)
            .subscribe(&holder)
            .await
            .expect("the subscription opens");
        let mut waiting = InboxQueue::<Entry>::new(ACCOUNT)
            .subscribe(&other)
            .await
            .expect("the subscription opens");
        let head = pin!(holding.stream())
            .next()
            .await
            .expect("the stream goes on")
            .expect("the claim takes the head");
        assert_eq!(head.payload(), b"head");
        // A row enters the group ahead of the head in work: a smaller priority, due a minute
        // earlier. It waits for the head to settle.
        let earlier = Utc::now() - TimeDelta::minutes(1);
        write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY - 1, earlier, b"ahead")).await;
        {
            let mut deliveries = pin!(waiting.stream());
            assert_claims_nothing(&mut deliveries).await;
            head.ack().await.expect("the head settles");
            let ahead = deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row that entered ahead");
            assert_eq!(ahead.payload(), b"ahead");
            ahead.ack().await.expect("the row settles");
        }
        drop((holding, waiting));
        holder.shutdown().await.expect("the broker shuts down");
        other.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }
}

/// The claim statement of a FIFO group run by hand beside a subscription, in the forms whose claim
/// takes a group's head in one statement. The advisory lock form keeps a group in order by the lock
/// its key names, which a statement run by hand does not take.
mod by_hand {
    #[allow(unused_imports)]
    use super::*;

    crate::live::fifo_matrix! {
        /// The payloads of the rows `claim` hands out for the suite's group, run by hand on `pool`
        /// in a transaction of its own that it rolls back, its times read from the host's clock.
        async fn claimed_by_hand(pool: &Pool<Db>, claim: &Statement) -> Vec<Vec<u8>> {
            let now = Utc::now();
            let mut query = sqlx::query::<Db>(AssertSqlSafe(claim.sql().to_owned()));
            for param in claim.params() {
                query = match param {
                    Param::Group => query.bind(ACCOUNT),
                    Param::Now | Param::LeaseNow => query.bind(now),
                    Param::Lease => query.bind(now + TimeDelta::seconds(30)),
                    other => panic!("the ledger's claim binds {other:?}"),
                };
            }
            let mut tx = pool.begin().await.expect("a transaction opens");
            let rows = query.fetch_all(&mut *tx).await.expect("the claim runs");
            tx.rollback().await.expect("the transaction rolls back");
            rows.iter()
                .map(|row| row.get::<Vec<u8>, _>("payload"))
                .collect()
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_head_in_work_holds_back_its_group() {
            let Some(db) = database().await else { return };
            write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY, Utc::now(), b"head")).await;
            write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY, Utc::now(), b"next")).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Entry>::new(ACCOUNT)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            let claim = Entry::fifo_claim(&DIALECT);
            {
                let mut deliveries = pin!(subscriber.stream());
                let head = deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the head");
                assert_eq!(head.payload(), b"head");
                // The held head stays the head: the claim takes nothing, not the row behind it.
                assert_eq!(claimed_by_hand(&db.pool, &claim).await, Vec::<Vec<u8>>::new());
                head.ack().await.expect("the head settles");
                assert_eq!(claimed_by_hand(&db.pool, &claim).await, [b"next".to_vec()]);
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }
    }
}

/// Two brokers on one group, on the stands whose claims run side by side on connections of their
/// own: SQLite's one writer keeps two claims apart.
mod side_by_side {
    #[allow(unused_imports)]
    use super::*;

    crate::live::server_matrix! {
        // A claim's transaction holds the group from its guard to its end: in the row lock form
        // until its delivery settles, in the lease form until it commits the lease. The clock is
        // paused while the subscription claims, so it runs on the current thread.
        #[tokio::test]
        async fn a_claim_takes_nothing_while_another_claim_holds_the_group() {
            let Some(db) = database().await else { return };
            write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY, Utc::now(), b"head")).await;
            let connected = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut subscriber = InboxQueue::<Entry>::new(ACCOUNT)
                .subscribe(&connected)
                .await
                .expect("the subscription opens");
            // Another claim opened its transaction as the dialect opens one and took the group.
            let begin = DIALECT
                .begin(Opening::Default)
                .expect("the dialect opens a claim")
                .unwrap_or("BEGIN");
            let guard = DIALECT
                .fifo_guard(&Entry::SPEC)
                .expect("the dialect guards the ledger")
                .expect("a server takes a group with a guard");
            let mut other = db.pool.begin_with(begin).await.expect("a transaction opens");
            let mut take = sqlx::query_scalar::<Db, i64>(AssertSqlSafe(guard.sql().to_owned()));
            for param in guard.params() {
                assert_eq!(*param, Param::Group, "the guard binds the group alone");
                take = take.bind(ACCOUNT);
            }
            let taken = take.fetch_one(&mut *other).await.expect("the guard runs");
            assert_ne!(taken, 0, "the other claim takes the group");
            {
                let mut deliveries = pin!(subscriber.stream());
                assert_claims_nothing(&mut deliveries).await;
                other.rollback().await.expect("the other claim ends");
                let head = deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the head");
                assert_eq!(head.payload(), b"head");
                head.ack().await.expect("the head settles");
            }
            drop(subscriber);
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }

        // The clock is paused while the second broker claims, so it runs on the current thread.
        #[tokio::test]
        async fn two_claimers_take_one_row_of_a_group() {
            let Some(db) = database().await else { return };
            for payload in [b"first", b"again", b"third"] {
                write(&db.pool, Entry::new(ACCOUNT, PUBLISHED_PRIORITY, Utc::now(), payload)).await;
            }
            let first = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let second = SqlxBroker::new(db.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut holding = InboxQueue::<Entry>::new(ACCOUNT)
                .subscribe(&first)
                .await
                .expect("the subscription opens");
            let mut waiting = InboxQueue::<Entry>::new(ACCOUNT)
                .subscribe(&second)
                .await
                .expect("the subscription opens");
            let held = pin!(holding.stream())
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the head");
            assert_eq!(held.payload(), b"first");
            {
                let mut deliveries = pin!(waiting.stream());
                // Rows of the group are due, and the second broker takes none of them while the
                // first holds its delivery.
                assert_claims_nothing(&mut deliveries).await;
                held.ack().await.expect("the delivery settles");
                let next = deliveries
                    .next()
                    .await
                    .expect("the stream goes on")
                    .expect("the claim takes the next row");
                assert_eq!(next.payload(), b"again");
                next.ack().await.expect("the delivery settles");
            }
            drop((holding, waiting));
            first.shutdown().await.expect("the broker shuts down");
            second.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }
    }
}

/// What MySQL and MariaDB refuse: a group that keeps its order, claimed at SERIALIZABLE.
#[cfg(feature = "mysql")]
mod at_serializable {
    use chrono::DateTime;
    use ruststream_sqlx::dialect::StatementError;
    use ruststream_sqlx::{Inbox, SqlxBrokerError};
    use sqlx::FromRow;

    use super::*;

    /// The ledger in the row lock form, its claims opened at SERIALIZABLE.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "ledger", isolation = serializable)]
    struct SerialEntry {
        #[field(id, generated)]
        id: i64,
        #[field(group, fifo = true)]
        account: String,
        #[field(retry_after)]
        retry_after: DateTime<Utc>,
        #[field(processed_at)]
        processed_at: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    crate::live::mysql_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_fifo_row_lock_table_at_serializable_is_refused() {
            let Some(db) = database().await else { return };
            let connected = SqlxBroker::new(db.pool.clone())
                .connect_in_process()
                .await
                .expect("the broker connects");
            let refused = InboxQueue::<SerialEntry>::new(ACCOUNT)
                .subscribe(&connected)
                .await;
            // The guard refuses the level, and the subscription stops when it starts.
            assert!(
                matches!(
                    &refused,
                    Err(SqlxBrokerError::Dialect {
                        subscription,
                        source: StatementError::FifoAtSerializable { dialect: "mysql" },
                        ..
                    }) if subscription == ACCOUNT
                ),
                "{refused:?}"
            );
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
        }
    }
}
