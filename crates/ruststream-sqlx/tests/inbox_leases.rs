//! The lease form against the stands: a lease that runs out passes the row on, and its late holder
//! cannot settle; the service's own events run only while the lease holds.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{AckError, ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker, SqlxBrokerError};

const LEASE: Duration = Duration::from_secs(2);

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
    use crate::live::rows::lease::Plain;

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
            db.count("plain_jobs").await,
            1,
            "the late acknowledgement deleted nothing"
        );
        taken.ack().await.expect("the current holder settles");
        assert_eq!(db.count("plain_jobs").await, 0);
        tokio::time::resume();
        drop(current);
        first.shutdown().await.expect("the broker stops");
        second.shutdown().await.expect("the broker stops");
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
}

/// What only Postgres runs: a service's own acknowledgement and extension, written in its SQL.
#[cfg(feature = "postgres")]
mod on_postgres {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::{Ack, ConnectedSqlxBroker, Extend, Inbox};
    use sqlx::{PgConnection, PgPool, Postgres};

    use super::*;
    use crate::live::Database;
    use crate::live::postgres::database;

    /// A leased job whose acknowledgement is the service's own: it marks the row instead of
    /// deleting it.
    #[derive(Debug, Inbox, sqlx::FromRow)]
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
        async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
            mark(conn, id).await
        }
    }

    /// The same job, whose extension is the service's own as well.
    #[derive(Debug, Inbox, sqlx::FromRow)]
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
        async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
            mark(conn, id).await
        }
    }

    impl Extend<Postgres> for OwnLease {
        async fn extend(
            conn: &mut PgConnection,
            id: &i64,
            held: &DateTime<Utc>,
            until: &DateTime<Utc>,
        ) -> Result<bool, sqlx::Error> {
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
    async fn mark(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
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
        let after_late = acked(&db.pool).await;
        taken
            .ack()
            .await
            .expect("the current holder's acknowledgement runs");
        let after_current = acked(&db.pool).await;
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
