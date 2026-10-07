//! A table in row mode, in the lease form, with a partition key, an `ack` of the service's own and
//! the insert a producer writes a task with: the handler takes the row itself.

use ruststream::testing::TestApp;
use ruststream_sqlx::__private::Events;
use ruststream_sqlx::dialect::{Dialect, Sqlite as SqliteDialect};
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::{InboxRow, InboxTable, Insert};
use sqlx::{Sqlite, SqliteConnection, SqlitePool};

use super::{SETTLED, broker, database};

const SCHEMA: &str = "
CREATE TABLE mail_tasks (
    job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    tenant       TEXT NOT NULL,
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    recipient    TEXT NOT NULL
);
CREATE TABLE mail_done (job_id INTEGER NOT NULL, recipient TEXT NOT NULL);
";

/// The service's own `ack`: the finished mail moves to `mail_done`.
async fn move_done(conn: &mut SqliteConnection, id: i64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mail_done (job_id, recipient) SELECT job_id, recipient FROM mail_tasks \
         WHERE job_id = ?",
    )
    .bind(id)
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM mail_tasks WHERE job_id = ?")
        .bind(id)
        .execute(conn)
        .await?;
    Ok(())
}

mod derived {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::{Ack, Inbox};
    use sqlx::{Sqlite, SqliteConnection};

    #[derive(Debug, Clone, PartialEq, Inbox, sqlx::FromRow)]
    #[inbox(table = "mail_tasks", custom(ack))]
    pub(super) struct Mail {
        #[field(id, generated)]
        pub(super) job_id: i64,
        #[field(group)]
        pub(super) name: String,
        #[field(partition_key)]
        pub(super) tenant: String,
        #[field(attempt, generated)]
        pub(super) attempt: i16,
        #[field(locked_until)]
        pub(super) locked_until: Option<DateTime<Utc>>,
        pub(super) recipient: String,
    }

    impl Ack<Sqlite> for Mail {
        async fn ack(conn: &mut SqliteConnection, id: &i64) -> Result<(), sqlx::Error> {
            super::move_done(conn, *id).await
        }
    }
}

mod manual {
    use chrono::{DateTime, Utc};
    use ruststream::runtime::{Input, SoloCarried};
    use ruststream_sqlx::dialect::Column;
    use ruststream_sqlx::dialect::insert::{self, Sql};
    use ruststream_sqlx::spec::{Attempt, Key, Lease, own};
    use ruststream_sqlx::{Ack, AttemptRow, InboxSpec, InboxTable, Insert, KeyRow};
    use sqlx::{Sqlite, SqliteConnection};

    #[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
    pub(super) struct Mail {
        pub(super) job_id: i64,
        pub(super) name: String,
        pub(super) tenant: String,
        pub(super) attempt: i16,
        pub(super) locked_until: Option<DateTime<Utc>>,
        pub(super) recipient: String,
    }

    impl InboxTable for Mail {
        type Id = i64;
        type Table = InboxSpec<(Lease<DateTime<Utc>>, Key, Attempt, own::Ack)>;
        const TABLE: Self::Table = InboxSpec::new("mail_tasks", Column::new("job_id").generated())
            .lease(Column::new("locked_until"))
            .group(Column::new("name"))
            .partition_key(Column::new("tenant"))
            .attempt(Column::new("attempt").generated())
            .data(&[Column::new("recipient")])
            .own::<own::Ack>();

        fn id(&self) -> &i64 {
            &self.job_id
        }
    }

    // Row mode: the handler takes the row itself, on the core's carried lane.
    impl Input for Mail {
        type Axis = SoloCarried<Self>;
    }

    impl KeyRow for Mail {
        type Key = String;

        fn partition_key(&self) -> &String {
            &self.tenant
        }
    }

    impl AttemptRow for Mail {
        type Attempt = i16;

        fn attempt(&self) -> &i16 {
            &self.attempt
        }
    }

    impl Ack<Sqlite> for Mail {
        async fn ack(conn: &mut SqliteConnection, id: &i64) -> Result<(), sqlx::Error> {
            super::move_done(conn, *id).await
        }
    }

    /// The insert, written while the service compiles from the description the subscription
    /// reads.
    pub(super) const INSERT: Sql<256> = insert::sqlite(&Mail::TABLE.spec());

    impl Insert<SqliteConnection> for Mail {
        // The columns bind in the description's order: the roles, then the data columns.
        async fn insert(&self, conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
            sqlx::query(INSERT.as_str())
                .bind(&self.name)
                .bind(&self.tenant)
                .bind(self.locked_until)
                .bind(&self.recipient)
                .execute(conn)
                .await?;
            Ok(())
        }
    }
}

#[subscriber(InboxQueue::<derived::Mail>::new("mail"))]
async fn send_derived(_mail: &derived::Mail) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber(InboxQueue::<manual::Mail>::new("mail"))]
async fn send_manual(_mail: &manual::Mail) -> HandlerOutcome {
    HandlerOutcome::ack()
}

async fn done(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar("SELECT recipient FROM mail_done")
        .fetch_all(pool)
        .await
        .expect("the table reads")
}

async fn write(pool: &SqlitePool, row: &impl Insert<SqliteConnection>) {
    let mut conn = pool.acquire().await.expect("a connection");
    row.insert(&mut conn).await.expect("the row writes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_derived_row_runs_its_own_ack() {
    let db = database(SCHEMA).await;
    write(
        &db.pool,
        &derived::Mail {
            job_id: 0,
            name: "mail".into(),
            tenant: "acme".into(),
            attempt: 0,
            locked_until: None,
            recipient: "ops@example.com".into(),
        },
    )
    .await;
    let app = RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send_derived);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("mail")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    let lent = tb
        .broker::<SqlxBroker<Sqlite>>()
        .subscriber("mail")
        .received_values::<derived::Mail>();
    assert_eq!(
        (
            lent[0].recipient.as_str(),
            lent[0].tenant.as_str(),
            lent[0].attempt
        ),
        ("ops@example.com", "acme", 1)
    );
    assert_eq!(
        done(&db.pool).await,
        ["ops@example.com"],
        "the own ack moved the mail"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_manual_row_runs_its_own_ack() {
    let db = database(SCHEMA).await;
    write(
        &db.pool,
        &manual::Mail {
            job_id: 0,
            name: "mail".into(),
            tenant: "acme".into(),
            attempt: 0,
            locked_until: None,
            recipient: "ops@example.com".into(),
        },
    )
    .await;
    let app = RustStream::new(AppInfo::new("mailer", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send_manual);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("mail")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    let lent = tb
        .broker::<SqlxBroker<Sqlite>>()
        .subscriber("mail")
        .received_values::<manual::Mail>();
    assert_eq!(
        (
            lent[0].recipient.as_str(),
            lent[0].tenant.as_str(),
            lent[0].attempt
        ),
        ("ops@example.com", "acme", 1)
    );
    assert_eq!(
        done(&db.pool).await,
        ["ops@example.com"],
        "the own ack moved the mail"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[test]
fn both_forms_describe_the_same_table_events_and_insert() {
    assert_eq!(
        <derived::Mail as InboxRow>::SPEC,
        <manual::Mail as InboxRow>::SPEC
    );
    assert_eq!(
        <derived::Mail as Events<Sqlite>>::SHAPE,
        <manual::Mail as Events<Sqlite>>::SHAPE
    );
    let written = SqliteDialect
        .insert(&<manual::Mail as InboxTable>::TABLE.spec())
        .expect("the dialect writes the insert");
    assert_eq!(
        manual::INSERT.as_str(),
        written.sql(),
        "the const text is the dialect's"
    );
    let row = manual::Mail {
        job_id: 1,
        name: "mail".into(),
        tenant: "acme".into(),
        attempt: 1,
        locked_until: None,
        recipient: "ops@example.com".into(),
    };
    assert_eq!(
        <manual::Mail as Events<Sqlite>>::partition_key(&row),
        Some(b"acme".as_slice())
    );
    // Row mode: a by-name subscription reads no table in it, in either form.
    assert_eq!(<derived::Mail as Events<Sqlite>>::kinds(), None);
    assert_eq!(<manual::Mail as Events<Sqlite>>::kinds(), None);
}
