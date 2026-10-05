//! The queue rows the suites read, a module per form of the queue table.
//!
//! The modules name their rows alike, over the same tables, so a test body reads the form its
//! module imports. Every row writes a published message through its generated insert, on each
//! database that insert serves.

/// The rows of the row lock form: a claim locks its rows in a transaction their settlements end.
pub(crate) mod row_lock {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use ruststream::OutgoingMessage;
    use ruststream_sqlx::{HeaderColumn, Inbox, Insert, Publish, QueueDatabase};
    use sqlx::types::Json;
    use sqlx::{Error, FromRow};

    /// The `attempt` a row keeps after `delivered` deliveries, the last of which settled it.
    ///
    /// A claim counts nothing and a retry counts one, so the row keeps its count of deliveries.
    pub(crate) const fn attempts_after(delivered: i16) -> i16 {
        delivered
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
