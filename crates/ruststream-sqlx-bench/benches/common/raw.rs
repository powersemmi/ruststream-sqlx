//! The raw loop of every inbox scenario: a hand-written sqlx loop that runs the statements the broker
//! runs for the same table, in the order it runs them, on a pool built the same way.
//!
//! The loops take their statements from [`Statements`], which renders them once, at setup, with
//! the dialect the broker renders them with: a change in the crate's SQL changes every loop, and
//! a change in what a statement binds fails the setup instead of measuring something else. Each
//! loop counts a delivery down at the point the service's handler does, after the decode and
//! before the settlement, and runs until a claim comes back empty.

use std::hint::black_box;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use ruststream_sqlx::InboxTable;
use ruststream_sqlx::dialect::{
    Advisory, ClaimShape, Lease, OutboxDialect, Param, RowLock, Statement, TableSpec,
};
use sqlx::mysql::MySqlQueryResult;
use sqlx::postgres::PgQueryResult;
use sqlx::query::Query;
use sqlx::sqlite::SqliteQueryResult;
use sqlx::{
    Database, Decode, Encode, Executor, FromRow, IntoArguments, PgPool, Pool, Postgres, Row, Type,
};

use super::tables::{NAMED_INSERT, REPLY_INSERT, RowLockJob};
use super::{Confirmation, Latch, Order, OrderPlaced};

/// A statement rendered at setup, kept for the life of the process: sqlx takes a `'static` text
/// without copying it, and a raw loop that copied its text per message would charge itself a
/// cost the broker does not pay.
#[derive(Clone, Copy, Debug)]
pub struct Sql {
    pub text: &'static str,
    pub params: &'static [Param],
}

impl Sql {
    /// Keeps `statement` for the life of the process, after checking it binds what the loop
    /// that runs it binds.
    fn of(statement: &Statement, binds: &[&[Param]]) -> Self {
        let params = statement.params();
        assert!(
            binds.contains(&params),
            "the crate's statement `{}` binds {params:?}, and the raw loop binds one of {binds:?}: \
             rewrite the loop to run what the broker runs",
            statement.sql()
        );
        Self {
            text: Box::leak(statement.sql().to_owned().into_boxed_str()),
            params: Box::leak(params.to_vec().into_boxed_slice()),
        }
    }
}

/// The statements one form runs, rendered by the dialect from the table's description.
#[derive(Clone, Debug)]
pub struct Statements {
    pub claim: Sql,
    pub ack: Sql,
    /// The advisory lock form's extra statements: the lock on the key, the take, the unlock.
    pub advisory: Option<AdvisorySql>,
}

/// What the advisory lock form runs between its candidate claim and its acknowledgement.
#[derive(Clone, Debug)]
pub struct AdvisorySql {
    pub lock: Sql,
    pub take: Sql,
    pub unlock: Sql,
}

impl Statements {
    /// The row lock form: a claim that locks its rows in the open transaction, and the delete.
    pub fn row_lock(dialect: &impl RowLock, spec: &TableSpec<'_>, shape: ClaimShape) -> Self {
        Self {
            claim: Sql::of(
                &dialect.lock_claim(spec, shape).expect("the claim renders"),
                &[&[Param::Limit]],
            ),
            ack: Sql::of(
                &dialect.ack(spec).expect("the ack renders"),
                &[&[Param::Id]],
            ),
            advisory: None,
        }
    }

    /// The lease form: a claim that writes the lease and commits, and the delete that holds the
    /// lease it wrote.
    pub fn lease(dialect: &impl Lease, spec: &TableSpec<'_>) -> Self {
        Self {
            claim: Sql::of(
                &dialect
                    .lease_claim(spec, ClaimShape::Rows)
                    .expect("the claim renders"),
                &[
                    &[Param::LeaseNow, Param::Limit, Param::Lease],
                    &[Param::Lease, Param::LeaseNow, Param::Limit],
                ],
            ),
            ack: Sql::of(
                &dialect.ack(spec).expect("the ack renders"),
                &[&[Param::Id, Param::Held]],
            ),
            advisory: None,
        }
    }

    /// The advisory lock form: the candidates, the lock on the key, the take, the delete, the
    /// unlock.
    pub fn advisory(dialect: &impl Advisory, spec: &TableSpec<'_>) -> Self {
        let mut take = dialect
            .take(spec, ClaimShape::Rows)
            .expect("the take renders");
        assert_eq!(take.len(), 1, "the dialect takes a row in one statement");
        Self {
            claim: Sql::of(
                &dialect.advisory_claim(spec).expect("the claim renders"),
                &[&[Param::Limit]],
            ),
            ack: Sql::of(
                &dialect.ack(spec).expect("the ack renders"),
                &[&[Param::Id]],
            ),
            advisory: Some(AdvisorySql {
                lock: Sql::of(
                    &dialect.lock().expect("the dialect locks"),
                    &[&[Param::Key]],
                ),
                take: Sql::of(&take.remove(0), &[&[Param::Id]]),
                unlock: Sql::of(
                    &dialect.unlock().expect("the dialect unlocks"),
                    &[&[Param::Key]],
                ),
            }),
        }
    }
}

/// The outbox's fetch of a tracked record and its mark, rendered from the record's description.
pub fn outbox(dialect: &impl OutboxDialect, spec: &TableSpec<'_>) -> (Sql, Sql) {
    (
        Sql::of(
            &dialect.outbox_fetch(spec).expect("the fetch renders"),
            &[&[Param::Id]],
        ),
        Sql::of(
            &dialect.outbox_mark(spec).expect("the mark renders"),
            &[&[Param::Id]],
        ),
    )
}

/// The rows a statement changed, which every driver reports under a type of its own.
pub trait Affected {
    fn rows(&self) -> u64;
}

impl Affected for PgQueryResult {
    fn rows(&self) -> u64 {
        self.rows_affected()
    }
}

impl Affected for MySqlQueryResult {
    fn rows(&self) -> u64 {
        self.rows_affected()
    }
}

impl Affected for SqliteQueryResult {
    fn rows(&self) -> u64 {
        self.rows_affected()
    }
}

/// What a statement binds, by the parameters it names.
#[derive(Clone, Copy, Debug)]
struct Values<'v> {
    limit: i64,
    id: i64,
    key: &'v str,
    now: DateTime<Utc>,
    lease: DateTime<Utc>,
}

impl Values<'_> {
    fn claim(limit: i64) -> Self {
        let now = Utc::now();
        Self {
            limit,
            id: 0,
            key: "",
            now,
            lease: now,
        }
    }
}

/// Binds `values` in the order `sql` names them.
fn bind<'q, DB>(sql: Sql, values: Values<'q>) -> Query<'q, DB, <DB as Database>::Arguments>
where
    DB: Database,
    for<'t> i64: Encode<'t, DB> + Type<DB>,
    for<'t> &'t str: Encode<'t, DB> + Type<DB>,
    for<'t> DateTime<Utc>: Encode<'t, DB> + Type<DB>,
{
    let mut query = sqlx::query(sql.text);
    for param in sql.params {
        query = match param {
            Param::Limit => query.bind(values.limit),
            Param::Id => query.bind(values.id),
            Param::Key => query.bind(values.key),
            Param::LeaseNow => query.bind(values.now),
            Param::Lease | Param::Held => query.bind(values.lease),
            other => unreachable!("the statements were checked at setup, and none binds {other:?}"),
        };
    }
    query
}

/// What a payload-mode handler does with a delivery: decode the body and read both fields.
pub fn read_payload(payload: &[u8]) {
    let order: Order = serde_json::from_slice(payload).expect("the body decodes");
    black_box((order.id, order.quantity));
}

/// The row lock form by hand: open a transaction, claim up to `limit` rows in it, hand each to
/// `read`, delete each, commit. Until a claim finds nothing.
pub async fn row_lock<DB, Job>(
    pool: &Pool<DB>,
    statements: &Statements,
    limit: usize,
    latch: &Latch,
    read: fn(&Job),
) where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'t> i64: Encode<'t, DB> + Type<DB>,
    for<'t> &'t str: Encode<'t, DB> + Type<DB>,
    for<'t> DateTime<Utc>: Encode<'t, DB> + Type<DB>,
    Job: InboxTable<Id = i64> + for<'r> FromRow<'r, DB::Row>,
{
    let values = Values::claim(i64::try_from(limit).expect("a batch size fits a bigint"));
    let mut jobs: Vec<Job> = Vec::with_capacity(limit);
    loop {
        let mut tx = pool.begin().await.expect("the transaction begins");
        jobs.clear();
        {
            let mut claimed = bind::<DB>(statements.claim, values).fetch(&mut *tx);
            while let Some(row) = claimed.try_next().await.expect("the claim runs") {
                jobs.push(Job::from_row(&row).expect("the row decodes"));
            }
        }
        if jobs.is_empty() {
            tx.commit().await.expect("the empty claim commits");
            return;
        }
        for job in &jobs {
            read(job);
            latch.arrived();
        }
        for job in &jobs {
            let id = *job.id();
            bind::<DB>(statements.ack, Values { id, ..values })
                .execute(&mut *tx)
                .await
                .expect("the delete runs");
        }
        tx.commit().await.expect("the settlement commits");
    }
}

/// The reply scenario by hand on Postgres: the row lock form one row at a time, and between the
/// count and the delete, the answer encoded into a buffer the loop keeps and inserted into the
/// replies table on a connection of its own, as the default publisher inserts it.
pub async fn reply(pool: &PgPool, statements: &Statements, latch: &Latch) {
    let values = Values::claim(1);
    let mut buffer = Vec::new();
    loop {
        let mut tx = pool.begin().await.expect("the transaction begins");
        let job = {
            let mut claimed = bind::<Postgres>(statements.claim, values).fetch(&mut *tx);
            let mut found = None;
            while let Some(row) = claimed.try_next().await.expect("the claim runs") {
                found = Some(RowLockJob::from_row(&row).expect("the row decodes"));
            }
            found
        };
        let Some(job) = job else {
            tx.commit().await.expect("the empty claim commits");
            return;
        };
        let order: Order = serde_json::from_slice(&job.payload).expect("the body decodes");
        latch.arrived();
        buffer.clear();
        serde_json::to_writer(
            &mut buffer,
            &Confirmation {
                id: black_box(order.id),
            },
        )
        .expect("the reply encodes");
        {
            let mut conn = pool.acquire().await.expect("the pool lends a connection");
            sqlx::query(REPLY_INSERT)
                .bind(&buffer[..])
                .execute(&mut *conn)
                .await
                .expect("the reply is inserted");
        }
        bind::<Postgres>(
            statements.ack,
            Values {
                id: *job.id(),
                ..values
            },
        )
        .execute(&mut *tx)
        .await
        .expect("the delete runs");
        tx.commit().await.expect("the settlement commits");
    }
}

/// The lease form by hand: claim up to `limit` rows with a lease of `lease` on a connection of
/// the pool's, hand each to `read`, then delete each while it still holds the lease the claim
/// wrote, each on a connection of its own. Until a claim finds nothing.
pub async fn lease<DB, Job>(
    pool: &Pool<DB>,
    statements: &Statements,
    limit: usize,
    lease: Duration,
    latch: &Latch,
    read: fn(&Job),
) where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'t> i64: Encode<'t, DB> + Type<DB>,
    for<'t> &'t str: Encode<'t, DB> + Type<DB>,
    for<'t> DateTime<Utc>: Encode<'t, DB> + Type<DB>,
    Job: InboxTable<Id = i64> + for<'r> FromRow<'r, DB::Row>,
    DB::QueryResult: Affected,
{
    let lease = chrono::Duration::from_std(lease).expect("the lease fits");
    let mut jobs: Vec<Job> = Vec::with_capacity(limit);
    loop {
        let mut values = Values::claim(i64::try_from(limit).expect("a batch size fits"));
        values.lease = values.now + lease;
        jobs.clear();
        {
            let mut conn = pool.acquire().await.expect("the pool lends a connection");
            let mut claimed = bind::<DB>(statements.claim, values).fetch(&mut *conn);
            while let Some(row) = claimed.try_next().await.expect("the claim runs") {
                jobs.push(Job::from_row(&row).expect("the row decodes"));
            }
        }
        if jobs.is_empty() {
            return;
        }
        for job in &jobs {
            read(job);
            latch.arrived();
        }
        for job in &jobs {
            let id = *job.id();
            let mut conn = pool.acquire().await.expect("the pool lends a connection");
            let deleted = bind::<DB>(statements.ack, Values { id, ..values })
                .execute(&mut *conn)
                .await
                .expect("the delete runs");
            // A delete that missed would leave the row to come back once its lease ran out, and
            // the loop would measure the redelivery instead of the settlement.
            assert_eq!(
                deleted.rows(),
                1,
                "the delete holds the lease the claim wrote"
            );
        }
    }
}

/// The advisory lock form by hand, one row at a time on one connection: the candidate, the lock
/// on its key, the take, `read`, the delete, the unlock. Until no candidate is left.
pub async fn advisory<DB, Job>(
    pool: &Pool<DB>,
    statements: &Statements,
    latch: &Latch,
    read: fn(&Job),
) where
    DB: Database,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    <DB as Database>::Arguments: IntoArguments<DB>,
    for<'t> i64: Encode<'t, DB> + Decode<'t, DB> + Type<DB>,
    for<'t> &'t str: Encode<'t, DB> + Decode<'t, DB> + Type<DB>,
    for<'t> DateTime<Utc>: Encode<'t, DB> + Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    Job: InboxTable<Id = i64> + for<'r> FromRow<'r, DB::Row>,
{
    let advisory = statements
        .advisory
        .as_ref()
        .expect("the advisory form's statements");
    let mut key = String::new();
    loop {
        let mut conn = pool.acquire().await.expect("the pool lends a connection");
        let base = Values::claim(1);
        let mut candidate = None;
        {
            let mut found = bind::<DB>(statements.claim, base).fetch(&mut *conn);
            while let Some(row) = found.try_next().await.expect("the candidates run") {
                candidate = Some(row.try_get::<i64, _>(0).expect("the id"));
                key.clear();
                key.push_str(row.try_get::<&str, _>(1).expect("the key"));
            }
        }
        let Some(id) = candidate else {
            return;
        };
        let values = Values {
            id,
            key: &key,
            ..base
        };
        let locked = bind::<DB>(advisory.lock, values)
            .fetch_one(&mut *conn)
            .await
            .expect("the lock runs");
        assert_ne!(
            locked.try_get::<i64, _>(0).expect("the lock's answer"),
            0,
            "nothing else holds the key"
        );
        let job = bind::<DB>(advisory.take, values)
            .fetch_one(&mut *conn)
            .await
            .expect("the take runs");
        let job = Job::from_row(&job).expect("the row decodes");
        read(&job);
        latch.arrived();
        bind::<DB>(statements.ack, values)
            .execute(&mut *conn)
            .await
            .expect("the delete runs");
        bind::<DB>(advisory.unlock, values)
            .fetch_one(&mut *conn)
            .await
            .expect("the unlock runs");
    }
}

/// A publish by hand: encode the message into a buffer the loop keeps, take a connection, run
/// the insert the table's `Publish` runs. Once per delivery the latch expects.
pub async fn publish(pool: &PgPool, latch: &Latch) {
    let mut buffer = Vec::new();
    for _ in 0..latch.total() {
        buffer.clear();
        serde_json::to_writer(&mut buffer, &OrderPlaced::fixed()).expect("the message encodes");
        let mut conn = pool.acquire().await.expect("the pool lends a connection");
        sqlx::query(NAMED_INSERT)
            .bind(&buffer[..])
            .execute(&mut *conn)
            .await
            .expect("the insert runs");
        latch.arrived();
    }
}
