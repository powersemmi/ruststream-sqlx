//! SQLite reads every name its statements quote as a name: a struct that names a column its table
//! lacks stops its subscription at the statement the database refused.

#![cfg(all(feature = "inbox", feature = "sqlite", feature = "chrono"))]

mod live;

use chrono::{DateTime, Utc};
use ruststream::{Broker, ConnectedBroker, SubscriptionSource};
use ruststream_sqlx::dialect::{ClaimShape, Lease};
use ruststream_sqlx::{Inbox, InboxQueue, InboxRow, SqlxBroker, SqlxBrokerError};
use sqlx::FromRow;

use crate::live::sqlite::{DIALECT, database};

/// A job of `plain_jobs`, in the lease form, whose payload field names `body`, a column the table
/// does not have.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs")]
struct Misnamed {
    #[field(id, generated)]
    id: i64,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    body: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_column_the_table_lacks_stops_a_lease_subscription_at_its_claim() {
    let db = database().await.expect("SQLite runs in memory");
    let connected = SqlxBroker::new(db.pool.clone())
        .connect()
        .await
        .expect("the broker connects");
    let refused = InboxQueue::<Misnamed>::new("misnamed")
        .subscribe(&connected)
        .await;
    // The claim returns the payload, and the startup check prepares the claim first.
    let claim = DIALECT
        .lease_claim(&Misnamed::SPEC, ClaimShape::Rows)
        .expect("the dialect builds the claim");
    assert!(
        matches!(&refused, Err(SqlxBrokerError::Schema { statement, source, .. })
            if *statement == claim.sql() && source.to_string().contains("no such column: body")),
        "{refused:?}"
    );
    connected.shutdown().await.expect("the broker shuts down");
    db.finish().await;
}
