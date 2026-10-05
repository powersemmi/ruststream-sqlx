//! The broker's lifecycle and its publishing side, against each stand and form: repositories,
//! routes, the `Closed` flag, the connect check, and the server it describes.

#![cfg(all(feature = "inbox", feature = "chrono", feature = "json"))]

mod live;

use chrono::{TimeDelta, Utc};
use ruststream::{Broker, ConnectedBroker, HeaderMap, OutgoingMessage, PublishPolicy, Publisher};
use ruststream_sqlx::{Insert, Repository, Routed, SqlxBroker, SqlxBrokerError};

live::matrix! {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_repository_publish_writes_a_row_through_the_services_publish() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .connect()
            .await
            .expect("the broker connects");
        let publisher = Repository::<SendEmail>::default()
            .pair(&connected)
            .await
            .expect("the repository pairs");
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        publisher
            .publish(
                OutgoingMessage::new("emails", b"\x00\xffbinary".as_slice()).with_headers(headers),
                None,
            )
            .await
            .expect("the publish writes");

        assert_eq!(
            db.email_rows("email_jobs").await,
            [("emails".to_owned(), b"\x00\xffbinary".to_vec(), 1, false)]
        );
        assert_eq!(db.email_header("x-tenant").await.as_deref(), Some("acme"));
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_route_leads_a_name_to_its_table() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<SendEmail>("emails")
            .route::<SendEmail>("reports.*")
            .route::<Plain>("plain")
            .connect()
            .await
            .expect("the broker connects");
        let publisher = Routed
            .pair(&connected)
            .await
            .expect("the route table pairs");
        for (name, payload) in [("emails", "a"), ("reports.daily", "b"), ("plain", "c")] {
            publisher
                .publish(OutgoingMessage::new(name, payload.as_bytes()), None)
                .await
                .unwrap_or_else(|err| panic!("a publish to {name} writes: {err}"));
        }
        let groups: Vec<String> = db
            .email_rows("email_jobs")
            .await
            .into_iter()
            .map(|(group, ..)| group)
            .collect();
        assert_eq!(groups, ["emails", "reports.daily"]);
        assert_eq!(db.plain_rows("plain_jobs").await, [b"c".to_vec()]);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_name_without_a_route_is_an_error() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<SendEmail>("emails")
            .connect()
            .await
            .expect("the broker connects");
        let publisher = Routed
            .pair(&connected)
            .await
            .expect("the route table pairs");
        let refused = publisher
            .publish(OutgoingMessage::new("orders", b"{}".as_slice()), None)
            .await;
        assert!(
            matches!(&refused, Err(SqlxBrokerError::NoRoute { name }) if name == "orders"),
            "a publish to a name no route leads anywhere must fail, got {refused:?}"
        );
        assert_eq!(db.email_rows("email_jobs").await, []);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_routed_write_that_fails_names_the_routes_table() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<Plain>("plain")
            .connect()
            .await
            .expect("the broker connects");
        let routed = Routed
            .pair(&connected)
            .await
            .expect("the route table pairs");
        let message = || OutgoingMessage::new("plain", b"a".as_slice());

        sqlx::raw_sql("ALTER TABLE plain_jobs RENAME TO plain_jobs_moved")
            .execute(&db.pool)
            .await
            .expect("the table moves");
        let refused = routed
            .publish(message(), None)
            .await
            .expect_err("an insert into a table that moved fails");
        assert!(
            matches!(&refused, SqlxBrokerError::Publish { name, table, row, .. }
                if name == "plain" && table == "plain_jobs" && row.ends_with("Plain")),
            "{refused}"
        );

        // The pool is the service's: once the service closes it, the route has no connection to
        // take.
        db.pool.close().await;
        let refused = routed
            .publish(message(), None)
            .await
            .expect_err("a publish without a connection fails");
        assert!(
            matches!(&refused, SqlxBrokerError::Publish { table, source, .. }
                if table == "plain_jobs" && matches!(**source, sqlx::Error::PoolClosed)),
            "{refused}"
        );
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_header_the_table_cannot_hold_refuses_the_publish() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<Plain>("plain")
            .connect()
            .await
            .expect("the broker connects");
        let routed = Routed
            .pair(&connected)
            .await
            .expect("the route table pairs");
        let typed = Repository::<SendEmail>::default()
            .pair(&connected)
            .await
            .expect("the repository pairs");

        // `plain_jobs` keeps no headers at all.
        let mut tenant = HeaderMap::new();
        tenant.insert("x-tenant", "acme");
        let refused = routed
            .publish(
                OutgoingMessage::new("plain", b"a".as_slice()).with_headers(tenant),
                None,
            )
            .await
            .expect_err("a header the table has no column for refuses the publish");
        assert!(
            matches!(&refused, SqlxBrokerError::Header { header, .. } if header == "x-tenant"),
            "{refused}"
        );

        // `email_jobs` keeps headers as JSON strings, which hold no bytes that are not UTF-8.
        let mut binary = HeaderMap::new();
        binary.insert("x-blob", b"\xff\xfe".as_slice());
        let refused = typed
            .publish(
                OutgoingMessage::new("emails", b"b".as_slice()).with_headers(binary),
                None,
            )
            .await
            .expect_err("a header value the column would rewrite refuses the publish");
        assert!(
            matches!(&refused, SqlxBrokerError::Header { header, .. } if header == "x-blob"),
            "{refused}"
        );

        assert_eq!(db.plain_rows("plain_jobs").await, Vec::<Vec<u8>>::new());
        assert_eq!(db.email_rows("email_jobs").await, []);
        connected.shutdown().await.expect("the broker shuts down");
        db.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_repository_publish_after_shutdown_is_refused() {
        let Some(db) = database().await else { return };
        let connected = SqlxBroker::new(db.pool.clone())
            .route::<SendEmail>("emails")
            .connect()
            .await
            .expect("the broker connects");
        let typed = Repository::<SendEmail>::default()
            .pair(&connected)
            .await
            .expect("the repository pairs");
        let routed = Routed
            .pair(&connected)
            .await
            .expect("the route table pairs");
        connected.shutdown().await.expect("the broker shuts down");

        // The service's pool is still open: only the broker's own flag can refuse.
        assert!(!db.pool.is_closed());
        let message = || OutgoingMessage::new("emails", b"late".as_slice());
        assert!(matches!(
            typed.publish(message(), None).await,
            Err(SqlxBrokerError::Closed)
        ));
        assert!(matches!(
            routed.publish(message(), None).await,
            Err(SqlxBrokerError::Closed)
        ));
        assert_eq!(db.email_rows("email_jobs").await, []);
        db.finish().await;
    }
}

live::stands! {
    use crate::live::rows::row_lock::SendEmail;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_generated_insert_writes_every_field_but_the_generated_ones() {
        let Some(db) = database().await else { return };
        let mut conn = db.pool.acquire().await.expect("a connection");
        let later = Utc::now() + TimeDelta::minutes(5);
        let job = SendEmail {
            job_id: -1,
            name: "emails".to_owned(),
            customer: Some("acme".to_owned()),
            retry_after: later,
            attempt: 9,
            processed_at: None,
            meta: None,
            payload: b"x".to_vec(),
        };
        job.insert(&mut *conn).await.expect("the insert writes");
        let (id, attempt): (i64, i16) = sqlx::query_as("SELECT job_id, attempt FROM email_jobs")
            .fetch_one(&mut *conn)
            .await
            .expect("the row reads");
        // The database filled the id and the attempt; the delayed task kept its time.
        assert!(id > 0);
        assert_eq!(attempt, 1);
        assert!(db.email_waits().await);
        drop(conn);
        db.finish().await;
    }
}

/// What only Postgres says: its URLs and its protocol's name.
#[cfg(feature = "postgres")]
mod on_postgres {
    use std::time::Duration;

    use ruststream::DescribeServer;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_fails_when_the_database_cannot_be_reached() {
        // Nothing listens on port 1 of the loopback.
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(2))
            .connect_lazy("postgres://nobody:secret@127.0.0.1:1/nothing")
            .expect("a lazy pool is built without I/O");
        let refused = SqlxBroker::new(pool).connect().await;
        assert!(
            matches!(refused, Err(SqlxBrokerError::Connect { .. })),
            "connect must check a connection, got {:?}",
            refused.map(|_| ())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_server_description_names_host_and_port_never_credentials() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://svc:s3cr3t@db.internal:6543/orders?sslmode=disable")
            .expect("a lazy pool is built without I/O");
        let server = SqlxBroker::new(pool).describe_server();
        assert_eq!(server.host.as_deref(), Some("db.internal:6543"));
        assert_eq!(server.protocol, "postgres");
        assert!(!format!("{server:?}").contains("s3cr3t"));
    }
}
