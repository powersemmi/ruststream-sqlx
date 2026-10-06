//! An `AnyPool`: the broker picks the dialect of the database the pool reaches when it connects,
//! and its queue rows hold the types `sqlx::Any` carries.

#![cfg(all(feature = "inbox", feature = "any", feature = "testing"))]

mod live;

use ruststream::DescribeServer;
use ruststream_sqlx::SqlxBroker;
use sqlx::any::{AnyPoolOptions, install_default_drivers};

/// The queue row every backend's tests read, and the pool they read it through.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
mod jobs {
    use std::time::Duration;

    use ruststream::OutgoingMessage;
    use ruststream_sqlx::{Inbox, Insert, Publish, QueueDatabase};
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use sqlx::{AnyPool, FromRow};

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
        ) -> Result<(), sqlx::Error> {
            let job = Self {
                job_id: 0,
                name: message.name().to_owned(),
                attempt: 1,
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
    use serde::{Deserialize, Serialize};
    use sqlx::{Any, ConnectOptions};

    use crate::jobs::{AnyEmail, any_pool};
    use crate::live::Database;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
    struct Email {
        to: String,
    }

    fn email() -> Email {
        Email {
            to: "a@example.com".to_owned(),
        }
    }

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
