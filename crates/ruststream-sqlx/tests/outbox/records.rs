//! The suite's outbox records, written out by hand with plain sqlx statements: `OrderRecord` over
//! `outbox`, which marks a processed record, and `RefundRecord` over `outbox_plain`, which deletes
//! it and whose retry event writes.

use std::collections::BTreeMap;
use std::future::{Future, ready};

use ruststream::{HeaderMap, OutgoingMessage};
use ruststream_sqlx::HeaderColumn;
use ruststream_sqlx::outbox::{Ack, Discard, Fetch, OutboxRow, Publish, Recover, Retry};
use sqlx::types::Json;
use sqlx::{Database, Error, FromRow, Postgres, Sqlite};

/// The `headers` column: a JSON object of strings, `NULL` for none.
pub(crate) type Headers = Option<Json<BTreeMap<String, String>>>;

/// A record of `outbox`.
#[derive(Debug, FromRow)]
pub(crate) struct OrderRecord {
    id: i64,
    name: String,
    payload: Vec<u8>,
    headers: Headers,
}

/// A record of `outbox_plain`.
#[derive(Debug, FromRow)]
pub(crate) struct RefundRecord {
    id: i64,
    name: String,
    payload: Vec<u8>,
    headers: Headers,
}

macro_rules! row {
    ($record:ty, retry_writes = $retry:literal) => {
        impl OutboxRow for $record {
            type Id = i64;

            const RETRY_WRITES: bool = $retry;

            fn id(&self) -> &i64 {
                &self.id
            }

            fn name(&self) -> &str {
                &self.name
            }

            fn payload(&self) -> &[u8] {
                &self.payload
            }

            fn take_headers(&mut self) -> HeaderMap {
                self.headers.take_headers()
            }
        }
    };
}

row!(OrderRecord, retry_writes = false);
row!(RefundRecord, retry_writes = true);

/// The six events of both records on one database, in SQL that Postgres and SQLite both read.
macro_rules! events {
    ($db:ty) => {
        impl Publish<$db> for OrderRecord {
            async fn publish(
                conn: &mut <$db as Database>::Connection,
                msg: &OutgoingMessage<'_>,
            ) -> Result<i64, Error> {
                sqlx::query_scalar(
                    "INSERT INTO outbox (name, payload, headers) VALUES ($1, $2, $3) RETURNING id",
                )
                .bind(msg.name())
                .bind(msg.payload())
                .bind(Headers::from_headers(msg.headers()))
                .fetch_one(conn)
                .await
            }
        }

        impl Fetch<$db> for OrderRecord {
            async fn fetch(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<Option<Self>, Error> {
                sqlx::query_as(
                    "SELECT id, name, payload, headers FROM outbox \
                     WHERE id = $1 AND processed_at IS NULL",
                )
                .bind(*id)
                .fetch_optional(conn)
                .await
            }
        }

        impl Ack<$db> for OrderRecord {
            async fn ack(conn: &mut <$db as Database>::Connection, id: &i64) -> Result<(), Error> {
                sqlx::query("UPDATE outbox SET processed_at = CURRENT_TIMESTAMP WHERE id = $1")
                    .bind(*id)
                    .execute(conn)
                    .await
                    .map(drop)
            }
        }

        // What the default retry does: nothing, so the record waits for the next startup.
        impl Retry<$db> for OrderRecord {
            fn retry(
                _conn: &mut <$db as Database>::Connection,
                _id: &i64,
            ) -> impl Future<Output = Result<(), Error>> + Send {
                ready(Ok(()))
            }
        }

        impl Discard<$db> for OrderRecord {
            async fn discard(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<(), Error> {
                <Self as Ack<$db>>::ack(conn, id).await
            }
        }

        impl Recover<$db> for OrderRecord {
            async fn recover(
                conn: &mut <$db as Database>::Connection,
                name: &str,
            ) -> Result<Vec<Self>, Error> {
                sqlx::query_as(
                    "SELECT id, name, payload, headers FROM outbox \
                     WHERE name = $1 AND processed_at IS NULL ORDER BY id",
                )
                .bind(name)
                .fetch_all(conn)
                .await
            }
        }

        impl Publish<$db> for RefundRecord {
            async fn publish(
                conn: &mut <$db as Database>::Connection,
                msg: &OutgoingMessage<'_>,
            ) -> Result<i64, Error> {
                sqlx::query_scalar(
                    "INSERT INTO outbox_plain (name, payload, headers) VALUES ($1, $2, $3) \
                     RETURNING id",
                )
                .bind(msg.name())
                .bind(msg.payload())
                .bind(Headers::from_headers(msg.headers()))
                .fetch_one(conn)
                .await
            }
        }

        impl Fetch<$db> for RefundRecord {
            async fn fetch(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<Option<Self>, Error> {
                sqlx::query_as("SELECT id, name, payload, headers FROM outbox_plain WHERE id = $1")
                    .bind(*id)
                    .fetch_optional(conn)
                    .await
            }
        }

        impl Ack<$db> for RefundRecord {
            async fn ack(conn: &mut <$db as Database>::Connection, id: &i64) -> Result<(), Error> {
                sqlx::query("DELETE FROM outbox_plain WHERE id = $1")
                    .bind(*id)
                    .execute(conn)
                    .await
                    .map(drop)
            }
        }

        impl Retry<$db> for RefundRecord {
            async fn retry(conn: &mut <$db as Database>::Connection, id: &i64) -> Result<(), Error> {
                sqlx::query("UPDATE outbox_plain SET retries = retries + 1 WHERE id = $1")
                    .bind(*id)
                    .execute(conn)
                    .await
                    .map(drop)
            }
        }

        impl Discard<$db> for RefundRecord {
            async fn discard(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<(), Error> {
                <Self as Ack<$db>>::ack(conn, id).await
            }
        }

        impl Recover<$db> for RefundRecord {
            async fn recover(
                conn: &mut <$db as Database>::Connection,
                name: &str,
            ) -> Result<Vec<Self>, Error> {
                sqlx::query_as(
                    "SELECT id, name, payload, headers FROM outbox_plain WHERE name = $1 ORDER BY id",
                )
                .bind(name)
                .fetch_all(conn)
                .await
            }
        }
    };
}

events!(Postgres);
events!(Sqlite);
