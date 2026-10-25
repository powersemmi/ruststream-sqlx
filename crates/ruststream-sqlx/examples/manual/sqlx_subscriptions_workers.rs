//! The service of `examples/sqlx_subscriptions_workers.rs` without the macros: the tables are
//! described with `InboxSpec`, the ledger's partition key through `KeyRow`, and the workers are
//! steps of the mount chain.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_subscriptions_workers --features inbox,sqlite,chrono,testing
//! ```

use std::future::{Future, ready};

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Key, Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, KeyRow, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::{Sqlite, SqliteConnection};

// --8<-- [start:workers]
use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::sqlite::SqlitePoolOptions;

/// One row of `image_jobs`: images resize in any order.
#[derive(sqlx::FromRow)]
struct Resize {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for Resize {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("image_jobs", Column::new("id").generated())
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for Resize {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// One row of `ledger_jobs`: the postings of one account apply in the order they were written.
#[derive(sqlx::FromRow)]
struct Post {
    id: i64,
    account: String,
    payload: Vec<u8>,
}

impl InboxTable for Post {
    type Id = i64;
    type Table = InboxSpec<(Key, Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("ledger_jobs", Column::new("id").generated())
        .partition_key(Column::new("account"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl KeyRow for Post {
    type Key = String;

    fn partition_key(&self) -> &String {
        &self.account
    }
}

impl PayloadRow for Post {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

struct ResizeImage;

impl Handle<Image> for ResizeImage {
    fn handle(
        &self,
        image: &Image,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(path = %image.path, "resizing");
        ready(Ok(()))
    }
}

struct PostEntry;

impl Handle<Posting> for PostEntry {
    fn handle(
        &self,
        posting: &Posting,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(account = %posting.account, posting.n, "posting");
        ready(Ok(()))
    }
}

/// The service on the database `url` names.
fn service(url: &str) -> RustStream {
    let schema_url = url.to_owned();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_url(url, SqlitePoolOptions::new());
    let broker = broker.route::<Resize>("images").route::<Post>("postings");
    RustStream::new(AppInfo::new("back-office", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = SqliteConnection::connect(&schema_url).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            // Up to four images in work at once.
            let resize = subscriber("images", ResizeImage)
                .map_source(|_| InboxQueue::<Resize>::new("images"))
                .workers(nonzero!(4));
            b.include(resize.build());
            // Four workers, and the postings of one account on one of them, one after another.
            let post = subscriber("postings", PostEntry)
                .map_source(|_| InboxQueue::<Post>::new("postings"))
                .workers_by_key(nonzero!(4));
            b.include(post.build());
        })
}
// --8<-- [end:workers]

#[ruststream::app]
fn app() -> impl App {
    service("sqlite:back-office.db?mode=rwc")
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS image_jobs (
    id INTEGER PRIMARY KEY,
    locked_until TEXT,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_jobs (
    id INTEGER PRIMARY KEY,
    account TEXT NOT NULL,
    locked_until TEXT,
    payload BLOB NOT NULL
)";

impl Publish<Sqlite> for Resize {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO image_jobs (payload) VALUES (?)")
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

/// A publish takes the partition key out of the message: its `account` field.
impl Publish<Sqlite> for Post {
    async fn publish(
        conn: &mut SqliteConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO ledger_jobs (account, payload) \
             VALUES (json_extract(CAST(?1 AS TEXT), '$.account'), ?1)",
        )
        .bind(message.payload())
        .execute(conn)
        .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "images")]
struct Image {
    path: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "postings")]
struct Posting {
    account: String,
    n: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::try_join_all;
    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool};

    use super::{Image, Posting, SCHEMA, service};

    // An in-memory database every connection naming it shares, alive while one of them is open:
    // the test holds one beside the pool it reads the table through.
    async fn database(
        name: &str,
    ) -> Result<(String, SqlitePool, SqliteConnection), Box<dyn Error>> {
        let url = format!("sqlite:file:{name}?mode=memory&cache=shared");
        let options: SqliteConnectOptions = url.parse()?;
        let keeper = SqliteConnection::connect_with(&options).await?;
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .idle_timeout(None)
            .connect_with(options)
            .await?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await?;
        Ok((url, pool, keeper))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_image_is_resized_once() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_sqlx_subscriptions_workers_images").await?;
        let tb = TestApp::start(service(&url)).await?;
        let images: Vec<Image> = (1..=6)
            .map(|n| Image {
                path: format!("upload/{n}.png"),
            })
            .collect();
        let broker = tb.broker::<SqlxBroker<Sqlite>>();
        try_join_all(images.iter().map(|image| broker.message(image).publish())).await?;

        let mut resized: Vec<Image> = broker.subscriber("images").received();
        resized.sort_by(|left, right| left.path.cmp(&right.path));
        assert_eq!(resized, images);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_accounts_postings_keep_their_order() -> Result<(), Box<dyn Error>> {
        let (url, _pool, _keeper) = database("manual_sqlx_subscriptions_workers_postings").await?;
        let tb = TestApp::start(service(&url)).await?;
        let postings: Vec<Posting> = (1..=8)
            .map(|n| Posting {
                account: if n % 2 == 0 { "acme" } else { "globex" }.to_owned(),
                n,
            })
            .collect();
        let broker = tb.broker::<SqlxBroker<Sqlite>>();
        try_join_all(
            postings
                .iter()
                .map(|posting| broker.message(posting).publish()),
        )
        .await?;

        let received: Vec<Posting> = broker.subscriber("postings").received();
        for account in ["acme", "globex"] {
            let order: Vec<u32> = received
                .iter()
                .filter(|posting| posting.account == account)
                .map(|posting| posting.n)
                .collect();
            let written: Vec<u32> = postings
                .iter()
                .filter(|posting| posting.account == account)
                .map(|posting| posting.n)
                .collect();
            assert_eq!(order, written, "the postings of {account}");
        }
        Ok(())
    }
}
