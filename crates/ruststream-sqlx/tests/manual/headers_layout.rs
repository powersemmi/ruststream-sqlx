//! The headers layout: the queue's mechanics and the service's header fields sit in one struct,
//! the message flattens it, and the delivery's header map is built from the fields on its first
//! read.

use ruststream::testing::TestApp;
use ruststream_sqlx::__private::Events;
use ruststream_sqlx::InboxRow;
use ruststream_sqlx::prelude::*;
use sqlx::Sqlite;

use super::{SETTLED, broker, count, database};

const SCHEMA: &str = "
CREATE TABLE shipping_jobs (
    job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    tenant       TEXT NOT NULL,
    trace        TEXT,
    note         TEXT
);
INSERT INTO shipping_jobs (name, tenant, trace, note) VALUES ('orders', 'acme', NULL, 'fragile');
";

const LEFT: &str = "SELECT count(*) FROM shipping_jobs";

mod derived {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::{Inbox, InboxHeaders};

    #[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
    #[inbox(table = "shipping_jobs")]
    pub(super) struct OrderHeaders {
        #[field(id, generated)]
        job_id: i64,
        #[field(group)]
        name: String,
        #[field(attempt, generated)]
        attempt: i16,
        #[field(locked_until)]
        locked_until: Option<DateTime<Utc>>,
        tenant: String,
        trace: Option<String>,
    }

    #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
    pub(super) struct OrderJob {
        #[field(headers)]
        #[sqlx(flatten)]
        headers: OrderHeaders,
        pub(super) note: Option<String>,
    }
}

mod manual {
    use chrono::{DateTime, Utc};
    use ruststream::HeaderMap;
    use ruststream::runtime::{Input, SoloCarried};
    use ruststream_sqlx::dialect::Column;
    use ruststream_sqlx::spec::{Attempt, HeaderFields as Fields, Lease};
    use ruststream_sqlx::{AttemptRow, HeaderFields, InboxSpec, InboxTable, put_header};

    // The service keeps its header fields in a struct of their own; the crate needs neither
    // struct to know about the other.
    #[derive(Debug, Clone, sqlx::FromRow)]
    pub(super) struct OrderHeaders {
        job_id: i64,
        attempt: i16,
        tenant: String,
        trace: Option<String>,
    }

    #[derive(Debug, Clone, sqlx::FromRow)]
    pub(super) struct OrderJob {
        #[sqlx(flatten)]
        headers: OrderHeaders,
        pub(super) note: Option<String>,
    }

    impl InboxTable for OrderJob {
        type Id = i64;
        type Table = InboxSpec<(Lease<DateTime<Utc>>, Attempt, Fields)>;
        const TABLE: Self::Table =
            InboxSpec::new("shipping_jobs", Column::new("job_id").generated())
                .lease(Column::new("locked_until"))
                .group(Column::new("name"))
                .attempt(Column::new("attempt").generated())
                .data(&[Column::new("tenant"), Column::new("trace")])
                .fetching(&[Column::new("note")])
                .header_fields();

        fn id(&self) -> &i64 {
            &self.headers.job_id
        }
    }

    impl Input for OrderJob {
        type Axis = SoloCarried<Self>;
    }

    impl AttemptRow for OrderJob {
        type Attempt = i16;

        fn attempt(&self) -> &i16 {
            &self.headers.attempt
        }
    }

    impl HeaderFields for OrderJob {
        const NAMES: &'static [&'static str] = &["tenant", "trace"];

        fn header_map(&self) -> HeaderMap {
            let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
            put_header(&mut headers, "tenant", &self.headers.tenant);
            put_header(&mut headers, "trace", &self.headers.trace);
            headers
        }
    }
}

/// Acknowledges a job whose headers and note the fields hold; anything else goes back.
fn settle(tenant: Option<&str>, trace: Option<&str>, note: Option<&str>) -> HandlerOutcome {
    // A NULL leaves its header out.
    if (tenant, trace, note) == (Some("acme"), None, Some("fragile")) {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

#[subscriber(InboxQueue::<derived::OrderJob>::new("orders"))]
async fn ship_derived(job: &derived::OrderJob, ctx: &mut Context<'_>) -> HandlerOutcome {
    let headers = ctx.headers();
    settle(
        headers.get_str("tenant"),
        headers.get_str("trace"),
        job.note.as_deref(),
    )
}

#[subscriber(InboxQueue::<manual::OrderJob>::new("orders"))]
async fn ship_manual(job: &manual::OrderJob, ctx: &mut Context<'_>) -> HandlerOutcome {
    let headers = ctx.headers();
    settle(
        headers.get_str("tenant"),
        headers.get_str("trace"),
        job.note.as_deref(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_derived_layout_builds_its_headers() {
    let db = database(SCHEMA).await;
    let app = RustStream::new(AppInfo::new("shop", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(ship_derived);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(count(&db.pool, LEFT).await, 0, "the ack deleted the row");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_manual_layout_builds_its_headers() {
    let db = database(SCHEMA).await;
    let app = RustStream::new(AppInfo::new("shop", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(ship_manual);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.advance(SETTLED).await.expect("the delivery settles");
    tb.broker::<SqlxBroker<Sqlite>>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(count(&db.pool, LEFT).await, 0, "the ack deleted the row");
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}

#[test]
fn both_forms_describe_the_same_table() {
    assert_eq!(
        <derived::OrderJob as InboxRow>::SPEC,
        <manual::OrderJob as InboxRow>::SPEC
    );
    assert_eq!(
        <derived::OrderJob as Events<Sqlite>>::SHAPE,
        <manual::OrderJob as Events<Sqlite>>::SHAPE
    );
    // A message assembled from fields is handed over in row mode, which no by-name subscription
    // reads, in either form.
    assert_eq!(<derived::OrderJob as Events<Sqlite>>::kinds(), None);
    assert_eq!(<manual::OrderJob as Events<Sqlite>>::kinds(), None);
}
