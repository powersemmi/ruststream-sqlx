//! The outbox's paths. On a service over `MemoryBroker` whose `relay` answers each command on `in`
//! with a reply on `out`, which `sink` consumes, a message the outbox does not track allocates what
//! it allocates without the middlewares, and a tracked delivery no more than a raw sqlx loop
//! running the same statements. A tracked publish allocates what the raw insert and a publish
//! carrying the id header do, plus the id's text.

use std::convert::Infallible;
use std::env;
use std::sync::Arc;

use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::dialect::{Column, Form, OutboxDialect, Postgres as PgDialect, TableSpec};
use ruststream_sqlx::outbox::OUTBOX_ID_HEADER;
use ruststream_sqlx::{Outbox, outbox};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres};

use ruststream::{Broker, Bytes, ConnectedBroker, HeaderMap, PublishPolicy, Publisher};

use super::{MESSAGES, WARMUP, blocks, live};

/// The variable that turns the outbox on in a test build.
const SWITCH: &str = "RUSTSTREAM_SQLX_OUTBOX";

/// What a tracked publish allocates beyond the record's insert and the same publish carrying a
/// static id: the id's text.
const ID_TEXT: u64 = 1;

const INSERT: &str = "INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id";

/// What `relay`'s reply encodes to, for the raw insert.
const REPLY: &[u8] = br#"{"id":1}"#;

/// The default statements' table, as the derive describes `Record`'s.
const TABLE: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    .group(Column::new("name"))
    .processed_at(Column::new("processed_at"))
    .payload(Column::new("payload"))
    .database_clock();

#[derive(Debug, ruststream_sqlx::Outbox, sqlx::FromRow)]
#[outbox(table = "outbox")]
struct Record {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(processed_at)]
    processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl outbox::Publish<Postgres> for Record {
    async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
        sqlx::query_scalar(INSERT)
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(conn)
            .await
    }
}

#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "in")]
struct Command {
    id: i64,
}

#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "out")]
struct Reply {
    id: i64,
}

#[subscriber("in", reply)]
async fn relay(cmd: &Command) -> Reply {
    Reply { id: cmd.id }
}

/// The default fetch and mark of `Record`, built once.
#[derive(Clone)]
struct Statements(Arc<(String, String)>);

/// The service's state: the pool and the statements the raw handlers run.
#[derive(Clone, FromRef)]
struct Db {
    pool: PgPool,
    statements: Statements,
}

/// `relay` writing the reply's record itself.
#[subscriber("in", reply)]
async fn relay_raw(_cmd: &Command, State(pool): State<PgPool>) -> Reply {
    let mut conn = pool.acquire().await.expect("a connection");
    let id = sqlx::query_scalar(INSERT)
        .bind("out")
        .bind(REPLY)
        .fetch_one(&mut *conn)
        .await
        .expect("the insert");
    Reply { id }
}

#[subscriber("out")]
async fn sink(_reply: &Reply) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// `sink` taking and marking the record itself, on two connections as the subscription layer
/// does: the fetch's goes back before the handler runs.
#[subscriber("out")]
async fn sink_raw(
    reply: &Reply,
    State(pool): State<PgPool>,
    State(Statements(statements)): State<Statements>,
) -> HandlerOutcome {
    let (fetch, mark) = &*statements;
    let record: Option<Record> = {
        let mut conn = pool.acquire().await.expect("a connection");
        sqlx::query_as(AssertSqlSafe(fetch.as_str()))
            .bind(reply.id)
            .fetch_optional(&mut *conn)
            .await
            .expect("the fetch")
    };
    let record = record.expect("the record");
    let mut conn = pool.acquire().await.expect("a connection");
    sqlx::query(AssertSqlSafe(mark.as_str()))
        .bind(record.id)
        .execute(&mut *conn)
        .await
        .expect("the mark");
    HandlerOutcome::ack()
}

/// How the service is assembled in each measure.
#[derive(Clone, Copy)]
enum Service {
    /// No middlewares.
    Bare,
    /// Both middlewares, tracking a name nothing publishes.
    Untracked,
    /// The publish middleware alone, tracking `out`.
    TrackedPublish,
    /// No middlewares; `relay_raw` writes the record.
    RawPublish,
    /// Both middlewares, tracking `out`.
    Tracked,
    /// No middlewares; `relay_raw` writes the record and `sink_raw` takes and marks it.
    Raw,
}

/// Allocations per command through the service assembled as `service`, after `WARMUP` of them.
async fn cost(pool: &PgPool, service: Service) -> u64 {
    let fetch = PgDialect.outbox_fetch(&TABLE).expect("the fetch");
    let mark = PgDialect.outbox_mark(&TABLE).expect("the mark");
    let state = Db {
        pool: pool.clone(),
        statements: Statements(Arc::new((fetch.sql().to_owned(), mark.sql().to_owned()))),
    };
    let app = RustStream::new(AppInfo::new("allocations", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state));
    let tracking = Outbox::new(pool.clone()).register::<Record>(match service {
        Service::Untracked => "elsewhere",
        _ => "out",
    });
    let tb = match service {
        Service::Bare => TestApp::start(app.with_broker(MemoryBroker::new(), |b| {
            b.include(relay).out_reply(Publish);
            b.include(sink);
        }))
        .await,
        Service::Untracked | Service::Tracked => TestApp::start(
            app.layer(tracking.layer())
                .publish_layer(tracking.publish_layer())
                .with_broker(MemoryBroker::new(), |b| {
                    b.include(relay).out_reply(Publish);
                    b.include(sink);
                }),
        )
        .await,
        Service::TrackedPublish => TestApp::start(
            app.publish_layer(tracking.publish_layer())
                .with_broker(MemoryBroker::new(), |b| {
                    b.include(relay).out_reply(Publish);
                    b.include(sink);
                }),
        )
        .await,
        Service::RawPublish => TestApp::start(app.with_broker(MemoryBroker::new(), |b| {
            b.include(relay_raw).out_reply(Publish);
            b.include(sink);
        }))
        .await,
        Service::Raw => TestApp::start(app.with_broker(MemoryBroker::new(), |b| {
            b.include(relay_raw).out_reply(Publish);
            b.include(sink_raw);
        }))
        .await,
    }
    .expect("the service starts");
    let mut start = 0;
    for command in 0..WARMUP + MESSAGES {
        if command == WARMUP {
            start = blocks();
        }
        tb.broker::<MemoryBroker>()
            .message(&Command { id: 1 })
            .publish()
            .await
            .expect("the command settles");
    }
    let per_command = (blocks() - start) / MESSAGES;
    tb.shutdown().await.expect("the service stops");
    per_command
}

/// Whether the outbox tracks in this process; `false` skips the measure.
///
/// # Panics
///
/// Panics when the switch is off and the live suites are required: the measure would compare
/// paths the outbox leaves untouched.
fn tracking_on() -> bool {
    if env::var(SWITCH).as_deref() == Ok("on") {
        return true;
    }
    assert!(
        env::var(live::REQUIRE_LIVE).map_or(true, |value| value.is_empty()),
        "{} is set, so the outbox's allocations must be measured, but {SWITCH} is not `on`",
        live::REQUIRE_LIVE,
    );
    eprintln!("{SWITCH} is not `on`; skipping the outbox's allocations");
    false
}

/// The outbox's paths against their references, on the test's database.
pub(super) async fn assert_outbox(pool: &PgPool) {
    if !tracking_on() {
        return;
    }
    let bare = cost(pool, Service::Bare).await;
    let untracked = cost(pool, Service::Untracked).await;
    dhat::assert_eq!(untracked, bare);

    // The harness keeps a copy of every published message, so a tracked reply's header costs it
    // more than it costs the service; the publish is measured below, outside the harness.
    let tracked_publish = cost(pool, Service::TrackedPublish).await;
    let raw_publish = cost(pool, Service::RawPublish).await;
    // A tracked delivery takes and marks its record as `sink_raw` does, and parses the id in place.
    let tracked = cost(pool, Service::Tracked).await;
    let raw = cost(pool, Service::Raw).await;
    dhat::assert!(
        tracked - tracked_publish <= raw - raw_publish,
        "a tracked delivery allocates {} where the raw loop allocates {}",
        tracked - tracked_publish,
        raw - raw_publish,
    );

    assert_tracked_publish(pool).await;
}

/// A publish through `wrap`, which records a message as the publish middleware does, against the
/// raw insert followed by the same publish with the id header's value static, on a
/// `MemoryBroker` nothing subscribes to.
async fn assert_tracked_publish(pool: &PgPool) {
    let connected = MemoryBroker::new().connect().await.expect("the broker connects");
    let plain = Publish.pair(&connected).await.expect("the publisher pairs");
    let tracking = Outbox::new(pool.clone()).register::<Record>("plain");
    let wrapped = tracking.wrap(Publish.pair(&connected).await.expect("the publisher pairs"));

    let mut start = 0;
    for message in 0..WARMUP + MESSAGES {
        if message == WARMUP {
            start = blocks();
        }
        wrapped
            .publish(OutgoingMessage::new("plain", b"\x01"), None)
            .await
            .expect("the tracked publish");
    }
    let tracked = (blocks() - start) / MESSAGES;

    for message in 0..WARMUP + MESSAGES {
        if message == WARMUP {
            start = blocks();
        }
        let mut headers = HeaderMap::new();
        headers.insert(OUTBOX_ID_HEADER, Bytes::from_static(b"1"));
        let msg = OutgoingMessage::new("plain", b"\x01").with_headers(headers);
        let mut conn = pool.acquire().await.expect("a connection");
        let _id: i64 = sqlx::query_scalar(INSERT)
            .bind(msg.name())
            .bind(msg.payload())
            .fetch_one(&mut *conn)
            .await
            .expect("the insert");
        drop(conn);
        plain.publish(msg, None).await.expect("the publish");
    }
    let raw = (blocks() - start) / MESSAGES;
    dhat::assert_eq!(tracked, raw + ID_TEXT);
    connected.shutdown().await.expect("the broker shuts down");
}
