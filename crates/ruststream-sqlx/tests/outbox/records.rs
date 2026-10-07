//! The suite's outbox records, as a service writes them: `OrderRecord` over `outbox` takes every
//! default and marks a processed record; `RefundRecord` over `outbox_plain` deletes a processed
//! record and counts its retries with an event of its own; `ParcelRecord` over `outbox_taken` runs
//! the service's own statement for every event.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::outbox::{Ack, Discard, Fetch, Publish, Recover, Retry};
use ruststream_sqlx::{HeaderColumn, Outbox};
use sqlx::types::Json;
use sqlx::{Database, Error, FromRow, MySql, Postgres, Sqlite};

use crate::stand::Stand;

/// The `headers` column: a JSON object of strings, `NULL` for none.
pub(crate) type Headers = Option<Json<BTreeMap<String, String>>>;

/// A record of `outbox`, settled by the default events.
#[derive(Debug, Outbox, FromRow)]
#[outbox(table = "outbox")]
pub(crate) struct OrderRecord {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(headers)]
    headers: Headers,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
}

/// A record of `outbox_plain`, which has no `processed_at`: the default events delete a processed
/// record, and its own retry counts the attempt.
#[derive(Debug, Outbox, FromRow)]
#[outbox(table = "outbox_plain", custom(retry))]
pub(crate) struct RefundRecord {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(headers)]
    headers: Headers,
}

/// A record of `outbox_taken`, whose every event is the service's own statement.
#[derive(Debug, Outbox, FromRow)]
#[outbox(table = "outbox_taken", custom(fetch, ack, retry, discard, recover))]
pub(crate) struct ParcelRecord {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
    #[field(headers)]
    headers: Headers,
}

/// The record of a published message, in `$table`, for each record on one database.
macro_rules! publish {
    ($db:ty: $($record:ty => $table:literal),*) => {$(
        impl Publish<$db> for $record {
            async fn publish(
                conn: &mut <$db as Database>::Connection,
                msg: &OutgoingMessage<'_>,
            ) -> Result<i64, Error> {
                let insert = sqlx::query(<$db as Stand>::returning_id(concat!(
                    "INSERT INTO ", $table, " (name, payload, headers) VALUES (?, ?, ?)"
                )))
                .bind(msg.name())
                .bind(msg.payload())
                .bind(Headers::from_headers(msg.headers()));
                <$db as Stand>::inserted(conn, insert).await
            }
        }
    )*};
}

/// The events the records write themselves, on one database.
macro_rules! own_events {
    ($db:ty) => {
        publish!($db:
            OrderRecord => "outbox",
            RefundRecord => "outbox_plain",
            ParcelRecord => "outbox_taken"
        );

        impl Retry<$db> for RefundRecord {
            async fn retry(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<(), Error> {
                sqlx::query(<$db as Stand>::sql(
                    "UPDATE outbox_plain SET retries = retries + 1 WHERE id = ?",
                ))
                .bind(*id)
                .execute(conn)
                .await
                .map(drop)
            }
        }

        // MySQL has no `RETURNING`, so the take and the read are two statements; the take alone
        // decides, since it changes the record only while nothing has taken it.
        impl Fetch<$db> for ParcelRecord {
            async fn fetch(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<Option<Self>, Error> {
                let taken = sqlx::query(<$db as Stand>::sql(
                    "UPDATE outbox_taken SET taken_at = CURRENT_TIMESTAMP \
                     WHERE id = ? AND taken_at IS NULL AND processed_at IS NULL",
                ))
                .bind(*id)
                .execute(&mut *conn)
                .await?
                .rows_affected();
                if taken == 0 {
                    return Ok(None);
                }
                sqlx::query_as(<$db as Stand>::sql(
                    "SELECT id, name, payload, headers FROM outbox_taken WHERE id = ?",
                ))
                .bind(*id)
                .fetch_optional(conn)
                .await
            }
        }

        impl Ack<$db> for ParcelRecord {
            async fn ack(conn: &mut <$db as Database>::Connection, id: &i64) -> Result<(), Error> {
                sqlx::query(<$db as Stand>::sql(
                    "UPDATE outbox_taken SET processed_at = CURRENT_TIMESTAMP WHERE id = ?",
                ))
                .bind(*id)
                .execute(conn)
                .await
                .map(drop)
            }
        }

        impl Retry<$db> for ParcelRecord {
            async fn retry(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<(), Error> {
                sqlx::query(<$db as Stand>::sql(
                    "UPDATE outbox_taken SET taken_at = NULL, attempts = attempts + 1 WHERE id = ?",
                ))
                .bind(*id)
                .execute(conn)
                .await
                .map(drop)
            }
        }

        impl Discard<$db> for ParcelRecord {
            async fn discard(
                conn: &mut <$db as Database>::Connection,
                id: &i64,
            ) -> Result<(), Error> {
                sqlx::query(<$db as Stand>::sql("DELETE FROM outbox_taken WHERE id = ?"))
                    .bind(*id)
                    .execute(conn)
                    .await
                    .map(drop)
            }
        }

        impl Recover<$db> for ParcelRecord {
            async fn recover(
                conn: &mut <$db as Database>::Connection,
                name: &str,
            ) -> Result<Vec<Self>, Error> {
                sqlx::query_as(<$db as Stand>::sql(
                    "SELECT id, name, payload, headers FROM outbox_taken \
                     WHERE name = ? AND processed_at IS NULL AND taken_at IS NULL ORDER BY id",
                ))
                .bind(name)
                .fetch_all(conn)
                .await
            }
        }
    };
}

own_events!(Postgres);
own_events!(MySql);
own_events!(Sqlite);
