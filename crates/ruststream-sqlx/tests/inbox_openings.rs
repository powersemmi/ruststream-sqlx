//! What a table's transactions open at, as its struct declares: the level a claim's transaction
//! runs at, read from inside it on Postgres; claims and settlements at SERIALIZABLE and REPEATABLE
//! READ on every stand that locks rows; and an `AnyPool` that refuses, when the subscription
//! starts, a level or a mode its database does not open.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::{Inbox, InboxQueue, Insert, Publish, QueueDatabase, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::{Error, FromRow};

const POLL: Duration = Duration::from_millis(20);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    n: u32,
}

const JOB: Job = Job { n: 1 };

/// A row of `plain_jobs` named `$name`, whose table adds `$inbox` to its `#[inbox(..)]`, and
/// whose publish writes it through the generated insert.
macro_rules! plain_job {
    ($(#[$doc:meta])* $name:ident, $($inbox:tt)*) => {
        $(#[$doc])*
        #[derive(Debug, Inbox, FromRow)]
        #[inbox(table = "plain_jobs", $($inbox)*)]
        struct $name {
            #[field(id, generated)]
            id: i64,
            #[field(attempt, generated)]
            attempt: i16,
            #[field(payload)]
            payload: Vec<u8>,
        }

        impl<DB> Publish<DB> for $name
        where
            DB: QueueDatabase,
            Self: Insert<DB::Connection>,
        {
            async fn publish(
                conn: &mut DB::Connection,
                message: &OutgoingMessage<'_>,
            ) -> Result<(), Error> {
                let job = Self {
                    id: 0,
                    attempt: 1,
                    payload: message.payload().to_vec(),
                };
                job.insert(conn).await
            }
        }
    };
}

plain_job! {
    /// The plain queue at SERIALIZABLE, claimed by the crate.
    Serializable, isolation = serializable
}

plain_job! {
    /// The plain queue at REPEATABLE READ, claimed by the crate.
    RepeatableRead, isolation = repeatable_read
}

live::row_lock_stands! {
    #[subscriber(InboxQueue::<Serializable>::new("plain"))]
    async fn serializable(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<RepeatableRead>::new("plain"))]
    async fn repeatable_read(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_serializable_table_claims_and_settles() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<Serializable>("plain");
        let app = RustStream::new(AppInfo::new("openings", "0.0.0")).with_broker(broker, |b| {
            b.include(serializable);
        });
        claimed_and_settled(app, &db).await;
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_repeatable_read_table_claims_and_settles() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<RepeatableRead>("plain");
        let app = RustStream::new(AppInfo::new("openings", "0.0.0")).with_broker(broker, |b| {
            b.include(repeatable_read);
        });
        claimed_and_settled(app, &db).await;
        db.finish().await;
    }

    /// Publishes one job through `app`'s route `plain`, and checks that its handler acknowledged
    /// it and the acknowledgement deleted the row.
    async fn claimed_and_settled(app: RustStream, db: &live::Database<Db>) {
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Db>>()
            .message(&JOB)
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Db>>()
            .subscriber("plain")
            .assert_called_once()
            .with(&JOB)
            .settled(HandlerOutcome::ack());
        assert_eq!(
            db.count("plain_jobs").await,
            0,
            "the acknowledgement deleted the row"
        );
        tb.shutdown().await.expect("the app stops");
    }
}

/// The level a claim's transaction runs at, read from inside it: the service's claim writes
/// `current_setting('transaction_isolation')` into `seen_isolation` before it takes its ids.
///
/// Postgres alone tells a transaction its own level. MySQL serves it from a cache it refreshes at
/// most every 100 ms, and MariaDB on the stand runs without `performance_schema`, so on them the
/// unit tests pin the statement a claim opens with.
#[cfg(feature = "postgres")]
mod seen {
    use ruststream::OutgoingMessage;
    use ruststream::prelude::*;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::{
        Claim, Fetch, Inbox, InboxQueue, Insert, Publish, QueueDatabase, SqlxBroker,
    };
    use sqlx::{Error, FromRow, PgConnection, Postgres};

    use super::{JOB, Job, POLL};
    use crate::live::Database;
    use crate::live::postgres::database;
    use crate::live::rows::own_fetch;

    /// The columns the rows decode.
    const COLUMNS: &str = "id, attempt, payload";

    /// Writes the level of the claim's transaction into `seen_isolation`, then takes ids as the
    /// crate's claim of the row lock form does.
    async fn recorded(conn: &mut PgConnection, limit: i64) -> Result<Vec<i64>, Error> {
        sqlx::query("INSERT INTO seen_isolation SELECT current_setting('transaction_isolation')")
            .execute(&mut *conn)
            .await?;
        sqlx::query_scalar("SELECT id FROM plain_jobs ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED")
            .bind(limit)
            .fetch_all(conn)
            .await
    }

    /// `Claim` and `Fetch` on Postgres for `$name`: its claim records its level.
    macro_rules! recorded {
        ($name:ident) => {
            impl Claim<Postgres> for $name {
                async fn claim(
                    conn: &mut PgConnection,
                    _queue: &str,
                    limit: i64,
                ) -> Result<Vec<i64>, Error> {
                    recorded(conn, limit).await
                }
            }

            impl Fetch<Postgres> for $name {
                async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
                    own_fetch::postgres(conn, COLUMNS, ids).await
                }
            }
        };
    }

    plain_job! {
        /// The plain queue at REPEATABLE READ, claimed by the service's recording claim.
        SeenRepeatableRead, isolation = repeatable_read, custom(claim, fetch)
    }
    recorded!(SeenRepeatableRead);

    plain_job! {
        /// The plain queue at SERIALIZABLE, claimed by the service's recording claim.
        SeenSerializable, isolation = serializable, custom(claim, fetch)
    }
    recorded!(SeenSerializable);

    plain_job! {
        /// The plain queue at the database's default level, claimed by the service's recording
        /// claim.
        SeenDefault, custom(claim, fetch)
    }
    recorded!(SeenDefault);

    #[subscriber(InboxQueue::<SeenRepeatableRead>::new("plain"))]
    async fn repeatable_read(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<SeenSerializable>::new("plain"))]
    async fn serializable(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<SeenDefault>::new("plain"))]
    async fn default(_job: &Job) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    /// The levels the claims of `app`'s subscription to `plain` ran at, read once it took one job
    /// and acknowledged it.
    async fn levels(app: RustStream, db: &Database<Postgres>) -> Vec<String> {
        let tb = TestApp::start_live(app).await.expect("the app starts");
        tb.broker::<SqlxBroker<Postgres>>()
            .message(&JOB)
            .to("plain")
            .publish()
            .await
            .expect("the publish settles");
        tb.broker::<SqlxBroker<Postgres>>()
            .subscriber("plain")
            .assert_called_once()
            .with(&JOB)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await.expect("the app stops");
        sqlx::query_scalar("SELECT DISTINCT level FROM seen_isolation ORDER BY level")
            .fetch_all(&db.pool)
            .await
            .expect("the levels read")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_repeatable_read_table_claims_at_repeatable_read() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<SeenRepeatableRead>("plain");
        let app = RustStream::new(AppInfo::new("openings", "0.0.0")).with_broker(broker, |b| {
            b.include(repeatable_read);
        });
        assert_eq!(levels(app, &db).await, ["repeatable read"]);
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_serializable_table_claims_at_serializable() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<SeenSerializable>("plain");
        let app = RustStream::new(AppInfo::new("openings", "0.0.0")).with_broker(broker, |b| {
            b.include(serializable);
        });
        assert_eq!(levels(app, &db).await, ["serializable"]);
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_table_without_a_level_claims_at_the_databases_default() {
        let Some(db) = database().await else { return };
        let broker = SqlxBroker::new(db.pool.clone())
            .poll_interval(POLL)
            .route::<SeenDefault>("plain");
        let app = RustStream::new(AppInfo::new("openings", "0.0.0")).with_broker(broker, |b| {
            b.include(default);
        });
        assert_eq!(levels(app, &db).await, ["read committed"]);
        db.finish().await;
    }
}

/// What an `AnyPool` refuses when a subscription starts. Its database is known only then, so every
/// level and mode compiles, and the picked backend's dialect refuses the ones it does not open.
#[cfg(feature = "any")]
mod on_any {
    use std::time::Duration;

    use ruststream::{Broker, ConnectedBroker, SubscriptionSource};
    use ruststream_sqlx::dialect::StatementError;
    use ruststream_sqlx::{Inbox, InboxQueue, SqlxBroker, SqlxBrokerError};
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use sqlx::{AnyPool, ConnectOptions, FromRow};

    /// The plain queue in a SQLite mode, of the types an `AnyPool` reads.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", mode = immediate)]
    struct Immediate {
        #[field(id, generated)]
        id: i64,
        #[field(payload)]
        payload: Vec<u8>,
    }

    /// The plain queue at READ UNCOMMITTED, which Postgres runs as READ COMMITTED.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", isolation = read_uncommitted)]
    struct ReadUncommitted {
        #[field(id, generated)]
        id: i64,
        #[field(payload)]
        payload: Vec<u8>,
    }

    /// An `AnyPool` on the database `url` names.
    async fn any_pool(url: &str) -> AnyPool {
        install_default_drivers();
        AnyPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .expect("the database accepts connections")
    }

    crate::live::row_lock_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_server_backend_refuses_a_sqlite_mode() {
            let Some(db) = database().await else { return };
            let pool = any_pool(db.pool.connect_options().to_url_lossy().as_str()).await;
            let connected = SqlxBroker::new(pool.clone())
                .connect()
                .await
                .expect("connects");
            let refused = InboxQueue::<Immediate>::new("plain")
                .subscribe(&connected)
                .await
                .expect_err("no server opens a SQLite mode");
            assert!(
                matches!(&refused, SqlxBrokerError::Dialect {
                    subscription,
                    table,
                    source: StatementError::UnsupportedOpening { opening: "mode `immediate`", .. },
                    ..
                } if subscription == "plain" && table == "plain_jobs"),
                "{refused:?}"
            );
            connected.shutdown().await.expect("stops");
            pool.close().await;
            db.finish().await;
        }
    }

    #[cfg(feature = "postgres")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_postgres_backend_refuses_read_uncommitted() {
        let Some(db) = crate::live::postgres::database().await else {
            return;
        };
        let pool = any_pool(db.pool.connect_options().to_url_lossy().as_str()).await;
        let connected = SqlxBroker::new(pool.clone())
            .connect()
            .await
            .expect("connects");
        let refused = InboxQueue::<ReadUncommitted>::new("plain")
            .subscribe(&connected)
            .await
            .expect_err("Postgres runs READ UNCOMMITTED as READ COMMITTED");
        assert!(
            matches!(
                &refused,
                SqlxBrokerError::Dialect {
                    source: StatementError::UnsupportedOpening {
                        dialect: "postgres",
                        opening: "isolation `read_uncommitted`",
                    },
                    ..
                }
            ),
            "{refused:?}"
        );
        connected.shutdown().await.expect("stops");
        pool.close().await;
        db.finish().await;
    }
}
