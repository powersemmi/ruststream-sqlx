use std::collections::BTreeMap;
use std::str;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream_sqlx::dialect::{ClaimShape, RowLock, Statement};
use ruststream_sqlx::{
    Fetch, HeaderColumn, Inbox, InboxHeaders, InboxRow, Insert, Publish, QueueDatabase,
};
use sqlx::types::Json;
use sqlx::{Error, Executor, FromRow};
#[cfg(feature = "mysql")]
use sqlx::{MySql, MySqlConnection};
#[cfg(feature = "postgres")]
use sqlx::{PgConnection, Postgres};

use super::{PUBLISHED_PRIORITY, mail_fetch, own_fetch};

/// The `attempt` a row keeps after `delivered` deliveries, the last of which settled it.
///
/// A claim counts nothing and a retry counts one, so the row keeps its count of deliveries.
pub(crate) const fn attempts_after(delivered: i16) -> i16 {
    delivered
}

/// Whether a lease holds the form's rows: no, the claim's transaction does.
pub(crate) const LEASED: bool = false;

/// The ledger: a FIFO group per account, whose rows are claimed one at a time, by priority,
/// then by `retry_after`.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "ledger")]
pub(crate) struct Entry {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(group, fifo = true)]
    pub(crate) account: String,
    #[field(priority)]
    pub(crate) priority: i16,
    #[field(retry_after)]
    pub(crate) retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(processed_at)]
    pub(crate) processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl Entry {
    /// An entry of `account` that carries `payload`, at `priority`, due at `retry_after`.
    pub(crate) fn new(
        account: &str,
        priority: i16,
        retry_after: DateTime<Utc>,
        payload: &[u8],
    ) -> Self {
        Self {
            id: 0,
            account: account.to_owned(),
            priority,
            retry_after,
            attempt: 1,
            processed_at: None,
            payload: payload.to_vec(),
        }
    }

    /// The claim of the ledger as `dialect` builds it: the head of a group, while it is due
    /// and no other claim holds it.
    pub(crate) fn fifo_claim(dialect: &impl RowLock) -> Statement {
        dialect
            .lock_claim(&Self::SPEC, ClaimShape::Rows)
            .expect("the dialect claims the ledger")
    }
}

impl<DB> Publish<DB> for Entry
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), Error> {
        let entry = Self::new(
            message.name(),
            PUBLISHED_PRIORITY,
            Utc::now(),
            message.payload(),
        );
        entry.insert(conn).await
    }
}

/// The email queue: a group per name and every role of the form.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "email_jobs")]
pub(crate) struct SendEmail {
    #[field(id, generated)]
    pub(crate) job_id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(partition_key)]
    pub(crate) customer: Option<String>,
    #[field(retry_after)]
    pub(crate) retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(processed_at)]
    pub(crate) processed_at: Option<DateTime<Utc>>,
    #[field(headers)]
    pub(crate) meta: Option<Json<BTreeMap<String, String>>>,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl SendEmail {
    /// A job of the queue `name` that carries `payload`, due now, as a producer writes it.
    pub(crate) fn queued(name: &str, payload: Vec<u8>) -> Self {
        Self {
            job_id: 0,
            name: name.to_owned(),
            customer: None,
            retry_after: Utc::now(),
            attempt: 1,
            processed_at: None,
            meta: None,
            payload,
        }
    }
}

impl<DB> Publish<DB> for SendEmail
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
            customer: message.headers().get_str("customer").map(str::to_owned),
            retry_after: Utc::now(),
            attempt: 1,
            processed_at: None,
            meta: HeaderColumn::from_headers(message.headers()),
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

/// One queue per table: no group, no time; a finished row is deleted.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs")]
pub(crate) struct Plain {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl<DB> Publish<DB> for Plain
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

/// The plain queue read by a fetch of the service's own: the crate claims the ids, the
/// service's fetch reads their rows.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "plain_jobs", custom(fetch))]
pub(crate) struct Fetched {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

/// The columns `Fetched` decodes.
const FETCHED: &str = "id, attempt, payload";

impl<DB> Publish<DB> for Fetched
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

#[cfg(feature = "postgres")]
impl Fetch<Postgres> for Fetched {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
        own_fetch::postgres(conn, FETCHED, ids).await
    }
}

#[cfg(feature = "mysql")]
impl Fetch<MySql> for Fetched {
    async fn fetch(conn: &mut MySqlConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
        own_fetch::mysql(conn, FETCHED, ids).await
    }
}

/// A job whose payload column holds an integer, which the struct's bytes never read: no row
/// of it decodes.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "unreadable_jobs")]
pub(crate) struct Unreadable {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl<DB> Publish<DB> for Unreadable
where
    DB: QueueDatabase,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    async fn publish(conn: &mut DB::Connection, _: &OutgoingMessage<'_>) -> Result<(), Error> {
        super::unreadable::<DB>(conn).await
    }
}

/// A job another table may point at: its acknowledgement fails while a reference stands.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "fragile_jobs")]
pub(crate) struct Fragile {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl<DB> Publish<DB> for Fragile
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
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

/// The table of the routing contract's by-name subscriptions: a group per name, a native
/// delayed retry, headers and an attempt.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "conformance_jobs")]
pub(crate) struct ConformanceRow {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(retry_after)]
    pub(crate) retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(headers)]
    pub(crate) meta: Option<Json<BTreeMap<String, String>>>,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl<DB> Publish<DB> for ConformanceRow
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
            name: message.name().to_owned(),
            retry_after: Utc::now(),
            attempt: 1,
            meta: HeaderColumn::from_headers(message.headers()),
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

/// The lifecycle table: a group per name and a native delayed retry. It keeps no headers, so a
/// publish that carries some is refused.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "lifecycle_jobs")]
pub(crate) struct LifecycleRow {
    #[field(id, generated)]
    pub(crate) id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(retry_after)]
    pub(crate) retry_after: DateTime<Utc>,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(payload)]
    pub(crate) payload: Vec<u8>,
}

impl<DB> Publish<DB> for LifecycleRow
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
            name: message.name().to_owned(),
            retry_after: Utc::now(),
            attempt: 1,
            payload: message.payload().to_vec(),
        };
        job.insert(conn).await
    }
}

/// A mail to send, which its handler takes as the row itself: no payload field, so the table is in
/// row mode. A group per name, an attempt, headers, and the mail's own columns.
#[derive(Debug, Clone, PartialEq, Inbox, FromRow)]
#[inbox(table = "mail_jobs")]
pub(crate) struct Mail {
    #[field(id, generated)]
    pub(crate) job_id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(headers)]
    pub(crate) meta: Option<Json<BTreeMap<String, String>>>,
    pub(crate) recipient: String,
    pub(crate) subject: Option<String>,
}

impl Mail {
    /// A mail of the queue `name` to `recipient`, as a producer writes it.
    pub(crate) fn queued(name: &str, recipient: &str, subject: Option<&str>) -> Self {
        Self {
            job_id: 0,
            name: name.to_owned(),
            attempt: 1,
            meta: None,
            recipient: recipient.to_owned(),
            subject: subject.map(str::to_owned),
        }
    }

    /// `self` with the lease of the claim that lent `lent`: none in this form, whose claim writes
    /// nothing into the row, so the row is lent as the table holds it.
    pub(crate) fn leased_as(self, _lent: &Self) -> Self {
        self
    }
}

/// A published message becomes a mail of the group it names, to the recipient its bytes spell:
/// the service's own wire for its row-mode table, written through the generated insert.
impl<DB> Publish<DB> for Mail
where
    DB: QueueDatabase,
    Self: Insert<DB::Connection>,
{
    async fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), Error> {
        let recipient =
            str::from_utf8(message.payload()).map_err(|error| Error::Encode(Box::new(error)))?;
        Self::queued(message.name(), recipient, None)
            .insert(conn)
            .await
    }
}

/// The mail queue read as what its producer wrote: two mails are equal when they go to the same
/// recipient with the same subject, whatever id, attempt or lease the table gave them. The core's
/// carried suites compare what a delivery lends with what they published, and only these fields
/// are theirs.
#[derive(Debug, Clone, Inbox, FromRow)]
#[inbox(table = "mail_jobs")]
pub(crate) struct WrittenMail {
    #[field(id, generated)]
    pub(crate) job_id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(headers)]
    pub(crate) meta: Option<Json<BTreeMap<String, String>>>,
    pub(crate) recipient: String,
    pub(crate) subject: Option<String>,
}

impl WrittenMail {
    /// A mail of the queue `name` to `recipient`, as a producer writes it.
    pub(crate) fn queued(name: &str, recipient: &str, subject: Option<&str>) -> Self {
        Self {
            job_id: 0,
            name: name.to_owned(),
            attempt: 1,
            meta: None,
            recipient: recipient.to_owned(),
            subject: subject.map(str::to_owned),
        }
    }
}

impl PartialEq for WrittenMail {
    fn eq(&self, other: &Self) -> bool {
        (&self.recipient, &self.subject) == (&other.recipient, &other.subject)
    }
}

/// The mail queue read by a fetch of the service's own, which leaves out a mail whose subject is
/// `gone`: the crate claims the ids, the service's fetch reads their rows.
#[derive(Debug, Clone, PartialEq, Inbox, FromRow)]
#[inbox(table = "mail_jobs", custom(fetch))]
pub(crate) struct FetchedMail {
    #[field(id, generated)]
    pub(crate) job_id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    #[field(headers)]
    pub(crate) meta: Option<Json<BTreeMap<String, String>>>,
    pub(crate) recipient: String,
    pub(crate) subject: Option<String>,
}

impl FetchedMail {
    /// A mail of the queue `name` to `recipient`, as a producer writes it.
    pub(crate) fn queued(name: &str, recipient: &str, subject: Option<&str>) -> Self {
        Self {
            job_id: 0,
            name: name.to_owned(),
            attempt: 1,
            meta: None,
            recipient: recipient.to_owned(),
            subject: subject.map(str::to_owned),
        }
    }
}

/// The columns `FetchedMail` decodes.
const MAILED: &str = "job_id, name, attempt, meta, recipient, subject";

#[cfg(feature = "postgres")]
impl Fetch<Postgres> for FetchedMail {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
        mail_fetch::postgres(conn, MAILED, ids).await
    }
}

#[cfg(feature = "mysql")]
impl Fetch<MySql> for FetchedMail {
    async fn fetch(conn: &mut MySqlConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
        mail_fetch::mysql(conn, MAILED, ids).await
    }
}

/// The queue table of an order's job, described by its headers struct: a group per name and an attempt, and the service's
/// own headers. A field without a role is a header: `tenant`, `trace` where it is not `NULL`, and
/// `order_id`.
#[derive(Debug, Clone, PartialEq, InboxHeaders, FromRow)]
#[inbox(table = "headed_jobs")]
pub(crate) struct OrderHeaders {
    #[field(id, generated)]
    pub(crate) job_id: i64,
    #[field(group)]
    pub(crate) name: String,
    #[field(attempt, generated)]
    pub(crate) attempt: i16,
    pub(crate) tenant: String,
    pub(crate) trace: Option<String>,
    pub(crate) order_id: i64,
}

/// The message a handler takes from `headed_jobs`: the headers struct, and the job's note, which
/// the default fetch reads from the same row.
#[derive(Debug, Clone, PartialEq, Inbox, FromRow)]
pub(crate) struct OrderJob {
    #[field(headers)]
    #[sqlx(flatten)]
    pub(crate) headers: OrderHeaders,
    pub(crate) note: Option<String>,
}

impl OrderJob {
    /// A job of the queue `name` for the order `order_id` of `tenant`, as a producer writes it.
    pub(crate) fn queued(name: &str, tenant: &str, trace: Option<&str>, order_id: i64) -> Self {
        Self {
            headers: OrderHeaders {
                job_id: 0,
                name: name.to_owned(),
                attempt: 1,
                tenant: tenant.to_owned(),
                trace: trace.map(str::to_owned),
                order_id,
            },
            note: None,
        }
    }

    /// The same job with `note`.
    pub(crate) fn noted(self, note: &str) -> Self {
        Self {
            note: Some(note.to_owned()),
            ..self
        }
    }

    /// `self` with the lease of the claim that lent `lent`: none in this form, so the job is lent
    /// as the table holds it.
    pub(crate) fn leased_as(self, _lent: &Self) -> Self {
        self
    }
}

/// A message of `headed_jobs` that reads a column the table lacks: the default fetch names it, so
/// its subscription stops at startup.
#[derive(Debug, Clone, PartialEq, Inbox, FromRow)]
pub(crate) struct MissingJob {
    #[field(headers)]
    #[sqlx(flatten)]
    pub(crate) headers: OrderHeaders,
    pub(crate) missing: String,
}
