//! An `AnyPool`: the broker picks the dialect of the database the pool reaches when it connects,
//! and its queue rows hold the types `sqlx::Any` carries. The servers take the row lock form, and
//! every backend takes the advisory lock form.

#![cfg(all(feature = "inbox", feature = "any", feature = "testing"))]

mod live;

use ruststream::DescribeServer;
use ruststream_sqlx::SqlxBroker;
use sqlx::any::{AnyPoolOptions, install_default_drivers};

/// The queue rows every backend's tests read, the message they carry, and the pool they read them
/// through.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod jobs {
    use std::time::Duration;

    use ruststream::{Outgoing, OutgoingMessage};
    use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase};
    use serde::{Deserialize, Serialize};
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use sqlx::{AnyPool, Error, FromRow};

    /// The message every test publishes.
    #[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
    pub(crate) struct Email {
        to: String,
    }

    /// An email to one address.
    pub(crate) fn email() -> Email {
        Email {
            to: "a@example.com".to_owned(),
        }
    }

    /// An email job of the types an `AnyPool` reads: integers, text and bytes, and no time.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "email_jobs")]
    pub(crate) struct AnyEmail {
        #[field(id, generated)]
        pub(crate) job_id: i64,
        #[field(group)]
        pub(crate) name: String,
        #[field(attempt, generated)]
        pub(crate) attempt: i32,
        #[field(payload)]
        pub(crate) payload: Vec<u8>,
    }

    impl<DB> Publish<DB> for AnyEmail
    where
        DB: QueueDatabase,
        Self: Insert<DB::Connection>,
    {
        async fn publish(
            conn: &mut DB::Connection,
            message: &OutgoingMessage<'_>,
        ) -> Result<(), Error> {
            let job = Self {
                job_id: 0,
                name: message.name().to_owned(),
                attempt: 1,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// The same email job in the advisory lock form: each job locked by its id.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "email_jobs", advisory_lock = "email_jobs-{job_id}")]
    pub(crate) struct AdvisoryEmail {
        #[field(id, generated)]
        pub(crate) job_id: i64,
        #[field(group)]
        pub(crate) name: String,
        #[field(attempt, generated)]
        pub(crate) attempt: i32,
        #[field(payload)]
        pub(crate) payload: Vec<u8>,
    }

    impl<DB> Publish<DB> for AdvisoryEmail
    where
        DB: QueueDatabase,
        Self: Insert<DB::Connection>,
    {
        async fn publish(
            conn: &mut DB::Connection,
            message: &OutgoingMessage<'_>,
        ) -> Result<(), Error> {
            let job = Self {
                job_id: 0,
                name: message.name().to_owned(),
                attempt: 1,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// The tenant every keyed job belongs to.
    const TENANT: &str = "acme";

    /// A job of `plain_jobs` in the advisory lock form, locked by its tenant: the jobs of one
    /// tenant share one key, so they go into work one at a time.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", advisory_lock = "plain-{tenant}")]
    pub(crate) struct KeyedJob {
        #[field(id, generated)]
        pub(crate) id: i64,
        #[field(attempt, generated)]
        pub(crate) attempt: i32,
        pub(crate) tenant: String,
        #[field(payload)]
        pub(crate) payload: Vec<u8>,
    }

    impl<DB> Publish<DB> for KeyedJob
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
                tenant: TENANT.to_owned(),
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// A pool on the database `url` names, through the driver its scheme picks.
    pub(crate) async fn any_pool(url: &str) -> AnyPool {
        install_default_drivers();
        AnyPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .expect("the database accepts connections")
    }
}

/// What an `AnyPool` does on the servers that lock rows: Postgres, MySQL and MariaDB.
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod on_servers {
    use std::time::Duration;

    use ruststream::prelude::*;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqlx::keys::Attempt;
    use ruststream_sqlx::{InboxQueue, SqlxBroker};
    use sqlx::{Any, ConnectOptions};

    use crate::jobs::{AnyEmail, Email, any_pool, email};
    use crate::live::Database;

    #[subscriber(InboxQueue::<AnyEmail>::new("emails"))]
    async fn retry_once(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        if attempt < Some(2) {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    #[subscriber("emails")]
    async fn by_name_retry_once(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        if attempt < Some(2) {
            HandlerOutcome::retry()
        } else {
            HandlerOutcome::ack()
        }
    }

    crate::live::row_lock_stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn an_any_pool_claims_and_settles() {
            let Some(db) = database().await else { return };
            let pool = any_pool(db.pool.connect_options().to_url_lossy().as_str()).await;
            let broker = SqlxBroker::new(pool.clone())
                .poll_interval(Duration::from_millis(20))
                .route::<AnyEmail>("emails");
            let app = RustStream::new(AppInfo::new("any", "0.0.0")).with_broker(broker, |b| {
                b.include(retry_once);
            });
            retried_then_acknowledged(app, &db).await;
            pool.close().await;
            let _ = |row: AnyEmail| (row.job_id, row.name, row.attempt, row.payload);
            db.finish().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_by_name_subscription_on_an_any_pool_reads_its_role_columns() {
            let Some(db) = database().await else { return };
            let pool = any_pool(db.pool.connect_options().to_url_lossy().as_str()).await;
            let broker = SqlxBroker::new(pool.clone())
                .poll_interval(Duration::from_millis(20))
                .route::<AnyEmail>("emails");
            let app = RustStream::new(AppInfo::new("any", "0.0.0")).with_broker(broker, |b| {
                b.include(by_name_retry_once);
            });
            retried_then_acknowledged(app, &db).await;
            pool.close().await;
            db.finish().await;
        }

        /// Publishes one email through `app`'s route, whose handler retries it once, and checks
        /// that it ran twice, read the attempt each time, and left no row behind.
        async fn retried_then_acknowledged(app: RustStream, db: &Database<Db>) {
            let tb = TestApp::start_live(app).await.expect("the app starts");
            tb.broker::<SqlxBroker<Any>>()
                .message(&email())
                .to("emails")
                .publish()
                .await
                .expect("the publish settles");
            tb.advance(Duration::from_millis(500))
                .await
                .expect("the retry settles");
            let outcomes = tb
                .broker::<SqlxBroker<Any>>()
                .subscriber("emails")
                .assert_called(2)
                .with(&email())
                .outcomes();
            assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
            assert_eq!(
                db.count("email_jobs").await,
                0,
                "the acknowledgement deleted the row"
            );
            tb.shutdown().await.expect("the app stops");
        }
    }
}

/// What an `AnyPool` does on SQLite, which locks no rows.
#[cfg(feature = "sqlite")]
mod on_sqlite {
    use ruststream::{Broker, ConnectedBroker, SubscriptionSource};
    use ruststream_sqlx::dialect::StatementError;
    use ruststream_sqlx::{InboxQueue, SqlxBroker, SqlxBrokerError};

    use crate::jobs::{AnyEmail, any_pool};

    // Under `Any` the backend is known only at connect, so the subscription refuses the table
    // when it starts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sqlite_backend_refuses_a_row_lock_table() {
        let pool = any_pool("sqlite::memory:").await;
        let connected = SqlxBroker::new(pool.clone())
            .connect()
            .await
            .expect("connects");
        let refused = InboxQueue::<AnyEmail>::new("emails")
            .subscribe(&connected)
            .await
            .expect_err("SQLite serves no row lock table");
        assert!(
            matches!(&refused, SqlxBrokerError::Dialect {
                subscription,
                source: StatementError::UnsupportedForm { dialect: "sqlite", form: "row lock" },
                ..
            } if subscription == "emails"),
            "{refused:?}"
        );
        connected.shutdown().await.expect("stops");
        pool.close().await;
    }
}

/// The advisory lock form on every backend. The broker locks each delivery's key with the dialect
/// it picked at connect: in the database on the servers, in the process on SQLite. A settlement
/// frees the key.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod advisory {
    #[cfg(feature = "sqlite")]
    use std::future::ready;
    use std::time::Duration;

    use ruststream::prelude::*;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_sqlx::keys::Attempt;
    use ruststream_sqlx::{InboxQueue, SqlxBroker};
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    use sqlx::ConnectOptions;
    #[cfg(feature = "mysql")]
    use sqlx::MySql;
    #[cfg(feature = "postgres")]
    use sqlx::Postgres;
    #[cfg(feature = "sqlite")]
    use sqlx::Sqlite;
    use sqlx::{Any, AnyPool, Database, Pool};

    use crate::jobs::{AdvisoryEmail, Email, KeyedJob, any_pool, email};
    #[cfg(feature = "mysql")]
    use crate::live::mysql::{lock_held, lock_name};
    #[cfg(feature = "postgres")]
    use crate::live::postgres::advisory_locks;

    /// A stand's database as an `AnyPool` reaches it, and the advisory locks it keeps.
    trait AnyStand: Database {
        /// The URL an `AnyPool` opens the database `pool` reaches by.
        fn any_url(pool: &Pool<Self>) -> String;

        /// How many locks the database `pool` reaches holds now, of those the broker takes on
        /// `keys`; `None` where it keeps none, as SQLite does, whose keys in work live in the
        /// process.
        ///
        /// Postgres lists every advisory lock of a database, so it counts them all. MySQL and
        /// MariaDB list none, and answer for the lock a key names.
        fn locks_held(pool: &Pool<Self>, keys: &[&str])
        -> impl Future<Output = Option<i64>> + Send;
    }

    #[cfg(feature = "postgres")]
    impl AnyStand for Postgres {
        fn any_url(pool: &Pool<Self>) -> String {
            pool.connect_options().to_url_lossy().to_string()
        }

        async fn locks_held(pool: &Pool<Self>, _: &[&str]) -> Option<i64> {
            Some(advisory_locks(pool).await)
        }
    }

    #[cfg(feature = "mysql")]
    impl AnyStand for MySql {
        fn any_url(pool: &Pool<Self>) -> String {
            pool.connect_options().to_url_lossy().to_string()
        }

        async fn locks_held(pool: &Pool<Self>, keys: &[&str]) -> Option<i64> {
            let mut held = 0;
            for key in keys {
                if lock_held(pool, &lock_name(pool, key).await).await {
                    held += 1;
                }
            }
            Some(held)
        }
    }

    #[cfg(feature = "sqlite")]
    impl AnyStand for Sqlite {
        // The stand's database lives in memory under a name, and every connection of the process
        // that opens the name in shared cache reaches it.
        fn any_url(pool: &Pool<Self>) -> String {
            let name = pool.connect_options().get_filename().display().to_string();
            format!("sqlite:{name}?mode=memory&cache=shared")
        }

        fn locks_held(_: &Pool<Self>, _: &[&str]) -> impl Future<Output = Option<i64>> + Send {
            ready(None)
        }
    }

    /// Asserts the database `pool` reaches holds no lock the broker takes on `keys`, where it
    /// keeps them.
    async fn assert_no_lock<DB: AnyStand>(pool: &Pool<DB>, keys: &[&str]) {
        if let Some(held) = DB::locks_held(pool, keys).await {
            assert_eq!(held, 0, "the database holds advisory locks of the broker");
        }
    }

    /// A broker on `pool` that publishes to both advisory tables.
    fn broker(pool: &AnyPool) -> SqlxBroker<Any> {
        SqlxBroker::new(pool.clone())
            .poll_interval(Duration::from_millis(20))
            .route::<AdvisoryEmail>("emails")
            .route::<KeyedJob>("keyed")
    }

    #[subscriber(InboxQueue::<AdvisoryEmail>::new("emails"))]
    async fn acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<KeyedJob>::new("keyed"))]
    async fn keyed_acked(_email: &Email) -> HandlerOutcome {
        HandlerOutcome::ack()
    }

    #[subscriber(InboxQueue::<AdvisoryEmail>::new("emails"))]
    async fn retried_once(_email: &Email, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
        match attempt {
            Some(1) => HandlerOutcome::retry(),
            // The second delivery reads the attempt the first one's take counted; any other
            // reading drops the row, which the outcomes would show.
            Some(2) => HandlerOutcome::ack(),
            _ => HandlerOutcome::drop(),
        }
    }

    crate::live::stands! {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_delivery_acks_and_frees_its_key() {
            let Some(db) = database().await else { return };
            let pool = any_pool(&Db::any_url(&db.pool)).await;
            let app = RustStream::new(AppInfo::new("any", "0.0.0"))
                .with_broker(broker(&pool), |b| {
                    b.include(acked);
                    b.include(keyed_acked);
                });
            let tb = TestApp::start_live(app).await.expect("the app starts");
            tb.broker::<SqlxBroker<Any>>()
                .message(&email())
                .to("emails")
                .publish()
                .await
                .expect("the publish settles");
            // Two jobs of one tenant share one key: the second goes into work only once the
            // first's acknowledgement freed the key, in the database or in the process.
            for _ in 0..2 {
                tb.broker::<SqlxBroker<Any>>()
                    .message(&email())
                    .to("keyed")
                    .publish()
                    .await
                    .expect("the publish settles");
            }
            tb.settle().await.expect("the deliveries settle");
            tb.broker::<SqlxBroker<Any>>()
                .subscriber("emails")
                .assert_called_once()
                .settled(HandlerOutcome::ack());
            let keyed = tb
                .broker::<SqlxBroker<Any>>()
                .subscriber("keyed")
                .assert_called(2)
                .outcomes();
            assert_eq!(keyed, [Outcome::Ack, Outcome::Ack]);
            assert_eq!(db.count("email_jobs").await, 0, "the acknowledgement deleted the row");
            assert_eq!(db.count("plain_jobs").await, 0, "both keyed jobs are done");
            assert_no_lock(&db.pool, &["email_jobs-1", "plain-acme"]).await;
            tb.shutdown().await.expect("the app stops");
            pool.close().await;
            db.finish().await;
            let _ = |row: AdvisoryEmail| (row.job_id, row.name, row.attempt, row.payload);
            let _ = |row: KeyedJob| (row.id, row.attempt, row.tenant, row.payload);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_retry_returns_at_once_with_its_attempt_counted() {
            let Some(db) = database().await else { return };
            let pool = any_pool(&Db::any_url(&db.pool)).await;
            let app = RustStream::new(AppInfo::new("any", "0.0.0"))
                .with_broker(broker(&pool), |b| {
                    b.include(retried_once);
                });
            let tb = TestApp::start_live(app).await.expect("the app starts");
            tb.broker::<SqlxBroker<Any>>()
                .message(&email())
                .to("emails")
                .publish()
                .await
                .expect("the publish settles");
            tb.advance(Duration::from_millis(500))
                .await
                .expect("the retry settles");
            let outcomes = tb
                .broker::<SqlxBroker<Any>>()
                .subscriber("emails")
                .assert_called(2)
                .outcomes();
            assert_eq!(outcomes, [Outcome::Nack, Outcome::Ack]);
            assert_eq!(db.count("email_jobs").await, 0, "the acknowledgement deleted the row");
            assert_no_lock(&db.pool, &["email_jobs-1"]).await;
            tb.shutdown().await.expect("the app stops");
            pool.close().await;
            db.finish().await;
        }
    }

    /// What the servers show of a delivery in work: its session holds the lock its key names, the
    /// lock the probes above read.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    mod in_work {
        use std::pin::pin;

        use futures::StreamExt;
        use ruststream::testing::InProcess;
        use ruststream::{ConnectedBroker, IncomingMessage, Subscriber, SubscriptionSource};
        use ruststream_sqlx::Insert;

        use super::*;

        /// The longest a test waits for a row its broker should claim at once.
        const AT_ONCE: Duration = Duration::from_secs(5);

        crate::live::row_lock_stands! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_delivery_in_work_holds_its_key() {
                let Some(db) = database().await else { return };
                let pool = any_pool(&Db::any_url(&db.pool)).await;
                let job = AdvisoryEmail {
                    job_id: 0,
                    name: "emails".to_owned(),
                    attempt: 1,
                    payload: b"x".to_vec(),
                };
                let mut conn = pool.acquire().await.expect("a connection");
                job.insert(&mut *conn).await.expect("the job writes");
                drop(conn);
                let connected = broker(&pool)
                    .connect_in_process()
                    .await
                    .expect("the broker connects");
                let mut subscriber = InboxQueue::<AdvisoryEmail>::new("emails")
                    .subscribe(&connected)
                    .await
                    .expect("the subscription opens");
                {
                    let mut deliveries = pin!(subscriber.stream());
                    let held = tokio::time::timeout(AT_ONCE, deliveries.next())
                        .await
                        .expect("the row is claimable at once")
                        .expect("the stream goes on")
                        .expect("the claim takes the row");
                    assert_eq!(
                        Db::locks_held(&db.pool, &["email_jobs-1"]).await,
                        Some(1),
                        "the delivery's session holds its row's lock"
                    );
                    held.ack().await.expect("the row settles");
                    assert_eq!(
                        Db::locks_held(&db.pool, &["email_jobs-1"]).await,
                        Some(0),
                        "the settlement freed the key"
                    );
                }
                drop(subscriber);
                connected.shutdown().await.expect("the broker shuts down");
                pool.close().await;
                db.finish().await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_any_pool_is_described_by_its_scheme_never_its_credentials() {
    install_default_drivers();
    for (url, host, protocol) in [
        (
            "postgres://svc:s3cr3t@db.internal:6543/orders",
            Some("db.internal:6543"),
            "postgres",
        ),
        (
            "postgresql://svc:s3cr3t@db.internal:6543/orders",
            Some("db.internal:6543"),
            "postgres",
        ),
        (
            "mysql://svc:s3cr3t@db.internal:3307/orders",
            Some("db.internal:3307"),
            "mysql",
        ),
        (
            "mariadb://svc:s3cr3t@db.internal:3307/orders",
            Some("db.internal:3307"),
            "mysql",
        ),
        // SQLite runs inside the service: no host, and its path is configuration.
        ("sqlite:///var/lib/app/s3cr3t.db", None, "sqlite"),
        ("sqlite::memory:", None, "sqlite"),
    ] {
        let pool = AnyPoolOptions::new()
            .connect_lazy(url)
            .expect("a lazy pool is built without I/O");
        let server = SqlxBroker::new(pool).describe_server();
        assert_eq!(server.host.as_deref(), host, "{url}");
        assert_eq!(server.protocol, protocol, "{url}");
        assert!(!format!("{server:?}").contains("s3cr3t"), "{server:?}");
    }
}
