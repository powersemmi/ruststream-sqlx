//! The queue rows the suites read, a module per form of the queue table.
//!
//! The modules name their rows alike, over the same tables, so a test body reads the form its
//! module imports. Every row but `Unreadable` writes a published message through its generated
//! insert, on each database that insert serves; `Unreadable` writes a row that never decodes.

use sqlx::{Database, Error, Executor};

/// The priority a published ledger entry takes: a row written by hand with a smaller one goes
/// ahead of it.
pub(crate) const PUBLISHED_PRIORITY: i16 = 1;

/// Writes a job of `unreadable_jobs`, whatever the message: an integer where its struct reads
/// bytes, so the row never decodes.
pub(crate) async fn unreadable<DB>(conn: &mut DB::Connection) -> Result<(), Error>
where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    sqlx::raw_sql("INSERT INTO unreadable_jobs (payload) VALUES (7)")
        .execute(conn)
        .await?;
    Ok(())
}

/// A fetch of the service's own over `plain_jobs`, per database: the rows of `ids`, read with the
/// columns a row's struct names.
///
/// It refuses a list of no ids, as MySQL refuses an empty `IN ()`, so a claim that hands it none
/// fails.
pub(crate) mod own_fetch {
    use sqlx::{AssertSqlSafe, Error, FromRow};
    #[cfg(feature = "mysql")]
    use sqlx::{MySqlConnection, mysql::MySqlRow};
    #[cfg(feature = "postgres")]
    use sqlx::{PgConnection, postgres::PgRow};
    #[cfg(feature = "sqlite")]
    use sqlx::{SqliteConnection, sqlite::SqliteRow};

    fn refuse_none(ids: &[i64]) -> Result<(), Error> {
        if ids.is_empty() {
            return Err(Error::InvalidArgument(
                "the claim handed the service's fetch no ids".to_owned(),
            ));
        }
        Ok(())
    }

    /// The select of MySQL and SQLite, which bind no list as one parameter: one placeholder per id.
    #[cfg(any(feature = "mysql", feature = "sqlite"))]
    fn listed(columns: &str, ids: &[i64]) -> AssertSqlSafe<String> {
        let placeholders = vec!["?"; ids.len()].join(", ");
        AssertSqlSafe(format!(
            "SELECT {columns} FROM plain_jobs WHERE id IN ({placeholders})"
        ))
    }

    #[cfg(feature = "postgres")]
    pub(crate) async fn postgres<Row>(
        conn: &mut PgConnection,
        columns: &str,
        ids: &[i64],
    ) -> Result<Vec<Row>, Error>
    where
        Row: for<'r> FromRow<'r, PgRow> + Send + Unpin,
    {
        refuse_none(ids)?;
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT {columns} FROM plain_jobs WHERE id = ANY($1)"
        )))
        .bind(ids)
        .fetch_all(conn)
        .await
    }

    #[cfg(feature = "mysql")]
    pub(crate) async fn mysql<Row>(
        conn: &mut MySqlConnection,
        columns: &str,
        ids: &[i64],
    ) -> Result<Vec<Row>, Error>
    where
        Row: for<'r> FromRow<'r, MySqlRow> + Send + Unpin,
    {
        refuse_none(ids)?;
        let mut select = sqlx::query_as(listed(columns, ids));
        for id in ids {
            select = select.bind(id);
        }
        select.fetch_all(conn).await
    }

    #[cfg(feature = "sqlite")]
    pub(crate) async fn sqlite<Row>(
        conn: &mut SqliteConnection,
        columns: &str,
        ids: &[i64],
    ) -> Result<Vec<Row>, Error>
    where
        Row: for<'r> FromRow<'r, SqliteRow> + Send + Unpin,
    {
        refuse_none(ids)?;
        let mut select = sqlx::query_as(listed(columns, ids));
        for id in ids {
            select = select.bind(id);
        }
        select.fetch_all(conn).await
    }
}

/// The rows of the row lock form: a claim locks its rows in a transaction their settlements end.
pub(crate) mod row_lock {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use ruststream::OutgoingMessage;
    use ruststream_sqlx::dialect::{ClaimShape, RowLock, Statement};
    use ruststream_sqlx::{Fetch, HeaderColumn, Inbox, InboxRow, Insert, Publish, QueueDatabase};
    use sqlx::types::Json;
    use sqlx::{Error, Executor, FromRow};
    #[cfg(feature = "mysql")]
    use sqlx::{MySql, MySqlConnection};
    #[cfg(feature = "postgres")]
    use sqlx::{PgConnection, Postgres};

    use super::{PUBLISHED_PRIORITY, own_fetch};

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
}

/// The rows of the lease form: a claim writes a lease into `locked_until` and commits, and a
/// settlement passes only while the row still holds it.
pub(crate) mod lease {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use ruststream::OutgoingMessage;
    use ruststream_sqlx::dialect::{ClaimShape, Lease, Statement};
    use ruststream_sqlx::{Fetch, HeaderColumn, Inbox, InboxRow, Insert, Publish, QueueDatabase};
    use sqlx::types::Json;
    use sqlx::{Error, Executor, FromRow};
    #[cfg(feature = "mysql")]
    use sqlx::{MySql, MySqlConnection};
    #[cfg(feature = "postgres")]
    use sqlx::{PgConnection, Postgres};
    #[cfg(feature = "sqlite")]
    use sqlx::{Sqlite, SqliteConnection};

    use super::{PUBLISHED_PRIORITY, own_fetch};

    /// The `attempt` a row keeps after `delivered` deliveries, the last of which settled it.
    ///
    /// A row starts at one, every claim adds one and commits it, and a retry adds nothing, so the
    /// row holds one more than its deliveries.
    pub(crate) const fn attempts_after(delivered: i16) -> i16 {
        delivered + 1
    }

    /// Whether a lease holds the form's rows: yes, the one each claim writes.
    pub(crate) const LEASED: bool = true;

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
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
                locked_until: None,
                payload: payload.to_vec(),
            }
        }

        /// The claim of the ledger as `dialect` builds it: the head of a group, while it is due
        /// and no row of the group holds a lease.
        pub(crate) fn fifo_claim(dialect: &impl Lease) -> Statement {
            dialect
                .lease_claim(&Self::SPEC, ClaimShape::Rows)
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
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
                locked_until: None,
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
                locked_until: None,
                meta: HeaderColumn::from_headers(message.headers()),
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// One queue per table: no group, no time but the lease; a finished row is deleted.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs")]
    pub(crate) struct Plain {
        #[field(id, generated)]
        pub(crate) id: i64,
        #[field(attempt, generated)]
        pub(crate) attempt: i16,
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
                locked_until: None,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }

    /// The plain queue read by a fetch of the service's own: the crate claims and leases the ids,
    /// the service's fetch reads their rows.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "plain_jobs", custom(fetch))]
    pub(crate) struct Fetched {
        #[field(id, generated)]
        pub(crate) id: i64,
        #[field(attempt, generated)]
        pub(crate) attempt: i16,
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
        #[field(payload)]
        pub(crate) payload: Vec<u8>,
    }

    /// The columns `Fetched` decodes.
    const FETCHED: &str = "id, attempt, locked_until, payload";

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
                locked_until: None,
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

    #[cfg(feature = "sqlite")]
    impl Fetch<Sqlite> for Fetched {
        async fn fetch(conn: &mut SqliteConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
            own_fetch::sqlite(conn, FETCHED, ids).await
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
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
                locked_until: None,
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
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
                locked_until: None,
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
        #[field(locked_until)]
        pub(crate) locked_until: Option<DateTime<Utc>>,
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
                locked_until: None,
                payload: message.payload().to_vec(),
            };
            job.insert(conn).await
        }
    }
}

/// The rows of the advisory lock form: a claim locks each row's key in the delivery's session and
/// takes the row, counting its attempt, and a settlement releases the lock.
pub(crate) mod advisory;
