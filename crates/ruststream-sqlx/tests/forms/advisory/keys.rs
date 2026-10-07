//! Which rows a key locks: each database keeps its own locks, so two databases whose rows render
//! one key lock their rows apart; a key that names a column its table lacks stops the subscription
//! at the statement the database refused.

use std::pin::pin;

use futures::StreamExt;
use ruststream::testing::InProcess;
use ruststream::{ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
use ruststream_sqlx::{InboxQueue, SqlxBroker};

use super::{AT_ONCE, POLL};

/// Two services, each in a database of its own on one stand, whose rows render the same key: each
/// locks its own row, as each database keeps its own locks.
mod per_database {
    use super::*;

    crate::live::advisory_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn two_databases_lock_their_rows_apart() {
            let Some(one) = database().await else { return };
            let Some(other) = database().await else { return };
            one.plain(&[b"one".as_slice()]).await;
            other.plain(&[b"other".as_slice()]).await;
            let first = SqlxBroker::new(one.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let second = SqlxBroker::new(other.pool.clone())
                .poll_interval(POLL)
                .connect_in_process()
                .await
                .expect("the broker connects");
            let mut holding = InboxQueue::<Plain>::new("plain")
                .subscribe(&first)
                .await
                .expect("the subscription opens");
            let mut taking = InboxQueue::<Plain>::new("plain")
                .subscribe(&second)
                .await
                .expect("the subscription opens");
            let held = pin!(holding.stream())
                .next()
                .await
                .expect("the stream goes on")
                .expect("the claim takes the row");
            assert_eq!(held.payload(), b"one");
            {
                let taken = tokio::time::timeout(AT_ONCE, pin!(taking.stream()).next())
                    .await
                    .expect("the other database's row is claimable at once")
                    .expect("the stream goes on")
                    .expect("the claim takes the row");
                assert_eq!(taken.payload(), b"other");
                taken.ack().await.expect("the row settles");
            }
            held.ack().await.expect("the row settles");
            drop((holding, taking));
            first.shutdown().await.expect("the broker shuts down");
            second.shutdown().await.expect("the broker shuts down");
            one.finish().await;
            other.finish().await;
        }
    }
}

/// A table that does not match its struct, refused when the subscription starts: every stand
/// refuses a name that matches no column, SQLite too, which quotes it as a name.
mod refused {
    use ruststream::{Broker, ConnectedBroker, SubscriptionSource};
    use ruststream_sqlx::dialect::Advisory;
    use ruststream_sqlx::{Inbox, InboxQueue, InboxRow, SqlxBroker, SqlxBrokerError};
    use sqlx::FromRow;

    /// A job of `email_jobs` whose lock key names `tenant`, a column the table does not have.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "email_jobs", advisory_lock = "email_jobs-{tenant}")]
    struct Tenanted {
        #[field(id, generated)]
        job_id: i64,
        #[field(group)]
        name: String,
        tenant: String,
        #[field(payload)]
        payload: Vec<u8>,
    }

    crate::live::advisory_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_lock_key_of_a_column_the_table_lacks_stops_the_subscription_at_its_claim() {
            let Some(db) = database().await else { return };
            let connected = SqlxBroker::new(db.pool.clone())
                .connect()
                .await
                .expect("the broker connects");
            let refused = InboxQueue::<Tenanted>::new("tenants")
                .subscribe(&connected)
                .await;
            // The candidate claim renders the key, and the startup check prepares it first.
            let claim = DIALECT
                .advisory_claim(&Tenanted::SPEC)
                .expect("the dialect builds the candidate claim");
            assert!(
                matches!(&refused, Err(SqlxBrokerError::Schema { statement, .. })
                    if *statement == claim.sql()),
                "{refused:?}"
            );
            connected.shutdown().await.expect("the broker shuts down");
            db.finish().await;
            let _ = |row: Tenanted| (row.job_id, row.name, row.tenant, row.payload);
        }
    }
}
