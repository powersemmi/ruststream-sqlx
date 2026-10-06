//! The lease form against the stands: a lease that runs out passes the row on, and its late holder
//! cannot settle; a lease in work is extended while its handler runs, and a delivery dropped
//! unsettled releases its row at once; a delivery reads the attempt before its claim's count,
//! whoever fetches the row; the service's own events run only while the lease holds.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::cell::Cell;
use std::pin::pin;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use ruststream::prelude::*;
use ruststream::testing::{InProcess, Outcome, TestApp};
use ruststream::{
    AckError, Broker, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource,
};
use ruststream_sqlx::keys::Attempt;
use ruststream_sqlx::{Clock, Inbox, InboxQueue, Insert, SqlxBroker, SqlxBrokerError};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

const LEASE: Duration = Duration::from_secs(2);

thread_local! {
    /// How many times this thread read [`Counting`].
    static READS: Cell<usize> = const { Cell::new(0) };
}

/// The host's clock, counting its reads on the thread that makes them.
struct Counting;

impl Clock for Counting {
    fn now() -> SystemTime {
        READS.with(|reads| reads.set(reads.get() + 1));
        SystemTime::now()
    }
}

/// The reads of [`Counting`] on this thread so far.
fn reads() -> usize {
    READS.with(Cell::get)
}

/// The email queue on the counting clock: its claim binds a time for `retry_after` and one for
/// the lease, and its acknowledgement one for `processed_at`.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "email_jobs", clock = Counting)]
struct Counted {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Task {
    n: u32,
}

fn lost(error: &AckError) -> bool {
    let AckError::Broker(source) = error else {
        return false;
    };
    matches!(
        source.downcast_ref::<SqlxBrokerError>(),
        Some(SqlxBrokerError::LeaseLost { .. })
    )
}

live::stands! {
    use crate::live::rows::lease::{Fetched, Plain};

    // The in-process mode keeps a paused clock still while the database answers and reads "now"
    // from the tokio clock, so a lease runs out when the test moves the clock.
    #[tokio::test]
    async fn a_late_holder_cannot_settle_after_its_lease_ran_out() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let first = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect_in_process()
            .await
            .expect("connects");
        let second = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect_in_process()
            .await
            .expect("connects");
        tokio::time::pause();
        let mut stalled = InboxQueue::<Plain>::new("plain")
            .subscribe(&first)
            .await
            .expect("opens");
        let late = {
            let mut deliveries = pin!(stalled.stream());
            deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row")
        };
        // The first holder stalls: its subscription is gone and its lease runs out.
        drop(stalled);
        tokio::time::advance(LEASE * 2).await;
        let mut current = InboxQueue::<Plain>::new("plain")
            .subscribe(&second)
            .await
            .expect("opens");
        let taken = {
            let mut deliveries = pin!(current.stream());
            deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the row whose lease ran out is claimable")
        };
        let refused = late
            .ack()
            .await
            .expect_err("the late holder's lease is gone");
        assert!(lost(&refused), "{refused:?}");
        assert_eq!(
            live::unpaused(db.count("plain_jobs")).await,
            1,
            "the late acknowledgement deleted nothing"
        );
        taken.ack().await.expect("the current holder settles");
        assert_eq!(live::unpaused(db.count("plain_jobs")).await, 0);
        tokio::time::resume();
        drop(current);
        first.shutdown().await.expect("the broker stops");
        second.shutdown().await.expect("the broker stops");
        db.finish().await;
    }

    // A lease ends on a whole second, which every temporal column holds exactly: on the MySQL
    // stands `locked_until` is a `DATETIME` without fractions, which would round a token with
    // fractions, and the settlement by token would then find no row.
    #[tokio::test]
    async fn a_lease_token_survives_a_column_without_fractions() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("opens");
        let delivery = {
            let mut deliveries = pin!(subscriber.stream());
            deliveries.next().await.expect("goes on").expect("claims")
        };
        delivery
            .ack()
            .await
            .expect("the token the claim wrote is the one the column holds");
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "the acknowledgement deleted the row"
        );
        drop(subscriber);
        connected.shutdown().await.expect("the broker stops");
        db.finish().await;
    }

    #[tokio::test]
    async fn a_claim_commits_at_once_and_counts_the_attempt() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect_in_process()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("opens");
        let delivery = {
            let mut deliveries = pin!(subscriber.stream());
            deliveries.next().await.expect("goes on").expect("claims")
        };
        // Another connection sees the claim: no transaction stays open for the handler.
        let (attempt, leased) = db.plain_lease().await;
        assert_eq!(attempt, 2, "the claim counted the attempt and committed");
        assert!(leased, "the claim wrote locked_until and committed");
        assert_eq!(
            delivery.redelivery_count(),
            Some(1),
            "the delivery reads the attempt before the claim's count"
        );
        delivery
            .nack(true)
            .await
            .expect("the retry releases the row");
        assert_eq!(
            db.plain_lease().await,
            (2, false),
            "a retry releases without counting again"
        );
        drop(subscriber);
        connected.shutdown().await.expect("the broker stops");
        db.finish().await;
    }

    // A claim reads "now" once: the rows it finds due, the leases it finds ended and the expiry it
    // writes start from one instant. The acknowledgement reads it once more, for `processed_at`.
    // The test's thread runs the claim and the settlement, and with an hour's lease no extension
    // round, which reads the clock too, comes before the test ends.
    #[tokio::test]
    async fn a_lease_claim_reads_the_clock_once() {
        let Some(db) = database().await else { return };
        let job = Counted {
            job_id: 0,
            name: "counted".to_owned(),
            retry_after: Utc::now(),
            attempt: 1,
            processed_at: None,
            locked_until: None,
            payload: b"x".to_vec(),
        };
        let mut conn = db.pool.acquire().await.expect("a connection");
        job.insert(&mut *conn).await.expect("the row is written");
        drop(conn);
        let connected = SqlxBroker::new(db.pool.clone())
            .connect()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<Counted>::new("counted")
            .lease(Duration::from_secs(3600))
            .subscribe(&connected)
            .await
            .expect("opens");
        let before = reads();
        let delivery = {
            let mut deliveries = pin!(subscriber.stream());
            deliveries.next().await.expect("goes on").expect("claims")
        };
        assert_eq!(reads() - before, 1, "the claim reads the clock once");
        delivery.ack().await.expect("the acknowledgement settles");
        assert_eq!(
            reads() - before,
            2,
            "the acknowledgement reads the clock once"
        );
        drop(subscriber);
        connected.shutdown().await.expect("the broker stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Plain>::new("plain").lease(Duration::from_secs(1)))]
    async fn longer_than_the_lease(_task: &Task) -> HandlerOutcome {
        // The handler's own work outlasts two leases; the keeper extends the row meanwhile.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lease_extension_keeps_a_long_handlers_row() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(50))
            .route::<Plain>("plain");
        let app = RustStream::new(AppInfo::new("leases", "0.0.0")).with_broker(broker, |b| {
            // A second worker claims whatever is claimable: it would take the row if the lease
            // lapsed.
            b.include(longer_than_the_lease.workers(nonzero!(2)));
        });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&Task { n: 1 })
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_secs(3))
            .await
            .expect("the handler finishes");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "the long handler's acknowledgement took effect"
        );
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }

    #[tokio::test]
    async fn a_lease_delivery_dropped_unsettled_returns_its_row_at_once() {
        let Some(db) = database().await else { return };
        db.plain(&[b"x".as_slice()]).await;
        let connected = SqlxBroker::new(db.pool.clone())
            .lease(Duration::from_secs(600))
            .poll_interval(Duration::from_millis(20))
            .connect_in_process()
            .await
            .expect("connects");
        tokio::time::pause();
        let mut subscriber = InboxQueue::<Plain>::new("plain")
            .subscribe(&connected)
            .await
            .expect("opens");
        {
            let mut deliveries = pin!(subscriber.stream());
            let first = deliveries.next().await.expect("goes on").expect("claims");
            drop(first);
            // The release runs at once; a ten-minute lease would otherwise hold the row.
            let again = tokio::time::timeout(Duration::from_secs(5), deliveries.next())
                .await
                .expect("the row is back before its lease ends")
                .expect("goes on")
                .expect("claims");
            again.ack().await.expect("settles");
        }
        tokio::time::resume();
        drop(subscriber);
        connected.shutdown().await.expect("stops");
        db.finish().await;
    }

    #[subscriber(InboxQueue::<Fetched>::new("plain"))]
    async fn retried(_task: &Task, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        match attempt {
            // The first delivery reads what the insert wrote, the second one more; any other
            // reading acknowledges, which the outcomes would show.
            Some(1 | 2) => HandlerOutcome::retry(),
            _ => HandlerOutcome::ack(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_services_own_fetch_reads_the_attempt_before_the_claims_count() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<Fetched>("plain");
        let app = RustStream::new(AppInfo::new("leases", "0.0.0")).with_broker(broker, |b| {
            b.include(retried)
                .max_attempts(nonzero!(2u32))
                .dead_letter("plain_jobs_dead");
        });
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&Task { n: 1 })
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.advance(Duration::from_millis(500))
            .await
            .expect("the retries settle");
        // Two deliveries, read as attempts 1 and 2: the second retry spent the row's attempts.
        let outcomes = tb
            .broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called(2)
            .outcomes();
        assert_eq!(outcomes, [Outcome::Nack, Outcome::Nack]);
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "the cap moved the row after its second delivery"
        );
        assert_eq!(db.count("plain_jobs_dead").await, 1);
        tb.shutdown().await.expect("the app stops");
        db.finish().await;
    }
}

/// What only Postgres runs: a service's own acknowledgement and extension, written in its SQL.
#[cfg(feature = "postgres")]
mod on_postgres {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::{Ack, ConnectedSqlxBroker, Extend, Inbox};
    use sqlx::{Error, FromRow, PgConnection, PgPool, Postgres};

    use super::*;
    use crate::live::Database;
    use crate::live::postgres::database;

    /// A leased job whose acknowledgement is the service's own: it marks the row instead of
    /// deleting it.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "acked_jobs", custom(ack))]
    struct LeasedAcked {
        #[field(id, generated)]
        id: i64,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl Ack<Postgres> for LeasedAcked {
        async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), Error> {
            mark(conn, id).await
        }
    }

    /// The same job, whose extension is the service's own as well.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "acked_jobs", custom(ack, extend))]
    struct OwnLease {
        #[field(id, generated)]
        id: i64,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    impl Ack<Postgres> for OwnLease {
        async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), Error> {
            mark(conn, id).await
        }
    }

    impl Extend<Postgres> for OwnLease {
        async fn extend(
            conn: &mut PgConnection,
            id: &i64,
            held: &DateTime<Utc>,
            until: &DateTime<Utc>,
        ) -> Result<bool, Error> {
            let extended = sqlx::query(
                "UPDATE acked_jobs SET locked_until = $1 WHERE id = $2 AND locked_until = $3",
            )
            .bind(until)
            .bind(id)
            .bind(held)
            .execute(conn)
            .await?;
            Ok(extended.rows_affected() == 1)
        }
    }

    /// The service's acknowledgement: it marks the job, whose lease it does not name.
    async fn mark(conn: &mut PgConnection, id: &i64) -> Result<(), Error> {
        sqlx::query("UPDATE acked_jobs SET acked = true WHERE id = $1")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }

    async fn acked(pool: &PgPool) -> Vec<bool> {
        sqlx::query_scalar("SELECT acked FROM acked_jobs ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("the table reads")
    }

    /// One job whose first holder stalls past its lease while a second claim takes it; both
    /// acknowledge, the late one first. Returns the late one's error and the `acked` marks after
    /// each acknowledgement.
    async fn late_then_current<Row>(db: &Database<Postgres>) -> (AckError, Vec<bool>, Vec<bool>)
    where
        InboxQueue<Row>: SubscriptionSource<ConnectedSqlxBroker<Postgres>>,
    {
        sqlx::query("INSERT INTO acked_jobs (payload) VALUES ('x')")
            .execute(&db.pool)
            .await
            .expect("the job writes");
        let first = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect_in_process()
            .await
            .expect("connects");
        let second = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect_in_process()
            .await
            .expect("connects");
        tokio::time::pause();
        let mut stalled = InboxQueue::<Row>::new("acked")
            .subscribe(&first)
            .await
            .expect("opens");
        let late = {
            let mut deliveries = pin!(stalled.stream());
            deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row")
        };
        drop(stalled);
        tokio::time::advance(LEASE * 2).await;
        let mut current = InboxQueue::<Row>::new("acked")
            .subscribe(&second)
            .await
            .expect("opens");
        let taken = {
            let mut deliveries = pin!(current.stream());
            deliveries
                .next()
                .await
                .expect("the stream goes on")
                .expect("the row whose lease ran out is claimable")
        };
        let refused = late
            .ack()
            .await
            .expect_err("the late holder's lease is gone");
        let after_late = live::unpaused(acked(&db.pool)).await;
        taken
            .ack()
            .await
            .expect("the current holder's acknowledgement runs");
        let after_current = live::unpaused(acked(&db.pool)).await;
        tokio::time::resume();
        drop(current);
        first.shutdown().await.expect("the broker stops");
        second.shutdown().await.expect("the broker stops");
        (refused, after_late, after_current)
    }

    #[tokio::test]
    async fn a_late_holders_own_ack_does_not_run() {
        let Some(db) = database().await else { return };
        let (refused, after_late, after_current) = late_then_current::<LeasedAcked>(&db).await;
        assert!(lost(&refused), "{refused:?}");
        assert_eq!(
            after_late,
            [false],
            "the late holder's own acknowledgement did not run"
        );
        assert_eq!(after_current, [true]);
        let _ = |row: LeasedAcked| (row.id, row.locked_until, row.payload);
        db.finish().await;
    }

    #[tokio::test]
    async fn the_services_own_extension_confirms_the_lease_its_own_ack_runs_under() {
        let Some(db) = database().await else { return };
        let (refused, after_late, after_current) = late_then_current::<OwnLease>(&db).await;
        assert!(lost(&refused), "{refused:?}");
        assert_eq!(
            after_late,
            [false],
            "the service's extension found the lease gone, so its acknowledgement did not run"
        );
        assert_eq!(after_current, [true]);
        let _ = |row: OwnLease| (row.id, row.locked_until, row.payload);
        db.finish().await;
    }
}

/// What only SQLite runs: its claim returns the rows it updated, and it keeps times as text.
#[cfg(feature = "sqlite")]
mod on_sqlite {
    use chrono::{DateTime, TimeDelta, TimeZone, Utc};
    use ruststream_sqlx::Inbox;
    use sqlx::{Encode, FromRow, Pool, Sqlite, Type};

    use super::*;
    use crate::live::sqlite::database;

    /// What a struct reads beside its roles, in a struct of its own.
    #[derive(Debug, FromRow)]
    struct Recipient {
        customer: Option<String>,
    }

    /// An email that flattens its recipient, so its claim returns `*`, the attempt as counted.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "email_jobs")]
    struct FlatEmail {
        #[field(id, generated)]
        job_id: i64,
        #[field(group)]
        name: String,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
        #[sqlx(flatten)]
        recipient: Recipient,
    }

    #[tokio::test]
    async fn a_struct_that_flattens_reports_the_attempt_before_the_claim() {
        let Some(db) = database().await else { return };
        sqlx::query(
            "INSERT INTO email_jobs (name, customer, payload) VALUES ('emails', 'acme', x'00')",
        )
        .execute(&db.pool)
        .await
        .expect("the row writes");
        let connected = SqlxBroker::new(db.pool.clone())
            .lease(LEASE)
            .connect()
            .await
            .expect("connects");
        let mut subscriber = InboxQueue::<FlatEmail>::new("emails")
            .subscribe(&connected)
            .await
            .expect("opens");
        let delivery = {
            let mut deliveries = pin!(subscriber.stream());
            deliveries.next().await.expect("goes on").expect("claims")
        };
        let counted: i16 = sqlx::query_scalar("SELECT attempt FROM email_jobs")
            .fetch_one(&db.pool)
            .await
            .expect("the row reads");
        assert_eq!(counted, 2, "the claim counted the attempt and committed");
        assert_eq!(
            delivery.redelivery_count(),
            Some(1),
            "the delivery reads the attempt before the claim's count"
        );
        delivery.ack().await.expect("settles");
        drop(subscriber);
        connected.shutdown().await.expect("the broker stops");
        let _ = |row: FlatEmail| {
            (
                row.job_id,
                row.name,
                row.attempt,
                row.locked_until,
                row.payload,
                row.recipient.customer,
            )
        };
        db.finish().await;
    }

    /// Whether SQLite finds `left <= right` for two times bound as the crate binds them.
    async fn sorted<Time>(pool: &Pool<Sqlite>, left: Time, right: Time) -> bool
    where
        Time: for<'q> Encode<'q, Sqlite> + Type<Sqlite> + Send,
    {
        sqlx::query_scalar("SELECT ? <= ?")
            .bind(left)
            .bind(right)
            .fetch_one(pool)
            .await
            .expect("SQLite compares the two")
    }

    // A lease ends on a whole second, and "now" and a delayed retry carry fractions of any length:
    // the text sqlx writes for `chrono` times sorts as the times themselves.
    #[tokio::test]
    async fn chrono_times_compare_as_the_times_they_hold() {
        let Some(db) = database().await else { return };
        let noon: DateTime<Utc> = Utc
            .with_ymd_and_hms(2026, 10, 5, 12, 0, 0)
            .single()
            .expect("a valid time");
        let times = [
            noon - TimeDelta::nanoseconds(1),
            noon - TimeDelta::milliseconds(1),
            noon,
            noon + TimeDelta::nanoseconds(1),
            noon + TimeDelta::microseconds(1),
            noon + TimeDelta::milliseconds(1),
            noon + TimeDelta::milliseconds(500),
            noon + TimeDelta::microseconds(500_001),
            noon + TimeDelta::seconds(1),
        ];
        for left in times {
            for right in times {
                assert_eq!(
                    sorted(&db.pool, left, right).await,
                    left <= right,
                    "{left} <= {right}"
                );
            }
        }
        db.finish().await;
    }

    // `time` writes UTC as `Z` and trims the fraction: its text sorts two times right when they
    // fall in different seconds, and within one second it sorts the whole second last.
    #[cfg(feature = "time")]
    #[tokio::test]
    async fn time_values_compare_to_the_second() {
        use time::{Duration, OffsetDateTime};

        let Some(db) = database().await else { return };
        // 2026-10-05 12:00:00 UTC.
        let noon = OffsetDateTime::from_unix_timestamp(1_791_201_600).expect("a valid time");
        let times = [
            noon - Duration::nanoseconds(1),
            noon,
            noon + Duration::microseconds(1),
            noon + Duration::seconds(1),
            noon + Duration::milliseconds(1500),
            noon + Duration::seconds(2),
        ];
        for left in times {
            for right in times {
                if left.unix_timestamp() != right.unix_timestamp() {
                    assert_eq!(
                        sorted(&db.pool, left, right).await,
                        left <= right,
                        "{left} <= {right}"
                    );
                }
            }
        }
        assert!(
            !sorted(&db.pool, noon, noon + Duration::milliseconds(500)).await,
            "`12:00:00Z` sorts after `12:00:00.5Z`"
        );
        db.finish().await;
    }
}
