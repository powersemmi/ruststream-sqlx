//! The advisory lock form's events on their own, for a subscription whose statements the test
//! prepares: the lock, the unlock and the take on Postgres, and the take on SQLite in memory,
//! written as SQLite and as MySQL write it.
//!
//! A session takes a key another session cannot, and a take counts the attempt and reads the row
//! only while the row is still claimable.

#![cfg(any(feature = "postgres", all(feature = "sqlite", feature = "mysql")))]

use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream_sqlx::__private::{Claiming, IdAt, Now, Prepared, Queue, Stmt};
use ruststream_sqlx::Inbox;
use ruststream_sqlx::dialect::{Statement, TableSpec};
use sqlx::FromRow;

/// A job in the advisory lock form: the take counts its attempt, and a finished job is no
/// longer claimable.
#[derive(Debug, Inbox, FromRow)]
#[inbox(table = "jobs", advisory_lock = "jobs-{id}")]
struct Job {
    #[field(id)]
    id: i64,
    #[field(attempt)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

fn interned(statement: &Statement) -> Stmt {
    Stmt {
        sql: Box::leak(statement.sql().into()),
        params: Box::leak(statement.params().into()),
    }
}

/// A subscription to the jobs `spec` describes, which prepared `prepared`.
fn queue(spec: &TableSpec<'static>, prepared: &Prepared) -> &'static Queue {
    Box::leak(Box::new(Queue {
        name: "jobs",
        table: "jobs",
        row: "Job",
        spec: *spec,
        id_at: IdAt::First,
        native_retry_after: false,
        kinds: None,
        prepared: *prepared,
        begin_claim: None,
        counted_attempt: false,
        poll_interval: Duration::from_secs(1),
        lease: None,
        cap: None,
    }))
}

/// The claim in progress of `queue`.
fn claiming(queue: &'static Queue) -> Claiming {
    Claiming {
        queue,
        limit: 1,
        now: Now::default(),
    }
}

#[cfg(feature = "postgres")]
mod on_postgres {
    use ruststream_sqlx::__private::{Claimed, Events, Prepared, Settling};
    use ruststream_sqlx::InboxRow;
    use ruststream_sqlx::dialect::{Advisory, ClaimShape};
    use sqlx::{
        AssertSqlSafe, Column, Error, Executor, PgConnection, Postgres, SqlSafeStr, Statement,
        TypeInfo,
    };

    use super::{Job, claiming, interned, queue};
    use crate::live::postgres::{DIALECT, database};
    use crate::live::rows::advisory::Plain;

    /// The type of the `attempt` column the statement `sql` returns, as Postgres describes it.
    async fn attempt_type(conn: &mut PgConnection, sql: &str) -> Result<String, Error> {
        let statement = conn
            .prepare(AssertSqlSafe(sql.to_owned()).into_sql_str())
            .await?;
        let attempt = statement
            .columns()
            .iter()
            .find(|column| column.name() == "attempt")
            .expect("the take returns the attempt");
        Ok(attempt.type_info().name().to_owned())
    }

    // `plain_jobs` keeps its attempt as `smallint`, which the struct reads as `i16`: the
    // take's read of the attempt as it was before its count keeps that type, for the struct
    // and for a reader by role alike.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_take_reads_the_attempt_in_its_columns_own_type() -> Result<(), Error> {
        let Some(db) = database().await else {
            return Ok(());
        };
        db.plain(&[b"x".as_slice()]).await;
        let id: i64 = sqlx::query_scalar("SELECT id FROM plain_jobs")
            .fetch_one(&db.pool)
            .await?;
        let take = DIALECT
            .take(&Plain::SPEC, ClaimShape::Rows)
            .expect("Postgres takes");
        let cx = claiming(queue(
            &Plain::SPEC,
            &Prepared {
                take: take.first().map(interned),
                ..Prepared::default()
            },
        ));
        let mut conn = db.pool.acquire().await?;
        let mut out = Vec::new();
        assert!(<Plain as Events<Postgres>>::take(&mut conn, &cx, &id, &mut out).await?);
        assert!(
            matches!(out.as_slice(), [Claimed::Row(Plain { attempt: 1, .. })]),
            "{out:?}"
        );
        assert_eq!(attempt_type(&mut conn, take[0].sql()).await?, "INT2");
        let roles = DIALECT
            .take(&Plain::SPEC, ClaimShape::Roles)
            .expect("Postgres takes");
        assert_eq!(attempt_type(&mut conn, roles[0].sql()).await?, "INT2");
        drop(conn);
        db.finish().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_session_takes_a_key_another_one_does_not_and_releases_it() -> Result<(), Error> {
        let Some(db) = database().await else {
            return Ok(());
        };
        let queue = queue(
            &Job::SPEC,
            &Prepared {
                lock: DIALECT.lock().as_ref().map(interned),
                unlock: DIALECT.unlock().as_ref().map(interned),
                ..Prepared::default()
            },
        );
        let cx = claiming(queue);
        let settling = Settling { queue, now: cx.now };
        let mut first = db.pool.acquire().await?;
        let mut second = db.pool.acquire().await?;
        assert!(<Job as Events<Postgres>>::lock(&mut first, &cx, "jobs-1").await?);
        assert!(!<Job as Events<Postgres>>::lock(&mut second, &cx, "jobs-1").await?);
        assert!(<Job as Events<Postgres>>::lock(&mut second, &cx, "jobs-2").await?);
        assert!(<Job as Events<Postgres>>::unlock(&mut first, &settling, "jobs-1").await?);
        // A key the session no longer holds is not released twice.
        assert!(!<Job as Events<Postgres>>::unlock(&mut first, &settling, "jobs-1").await?);
        assert!(<Job as Events<Postgres>>::lock(&mut second, &cx, "jobs-1").await?);
        for key in ["jobs-1", "jobs-2"] {
            assert!(<Job as Events<Postgres>>::unlock(&mut second, &settling, key).await?);
        }
        drop((first, second));
        db.finish().await;
        Ok(())
    }
}

#[cfg(all(feature = "sqlite", feature = "mysql"))]
mod on_sqlite {
    use chrono::{DateTime, Utc};
    use ruststream_sqlx::__private::{Claimed, Events, Prepared, Queue, Settling};
    use ruststream_sqlx::dialect::{self, Advisory, ClaimShape, Statement, TableSpec};
    use ruststream_sqlx::{Fetch, Inbox, InboxRow};
    use sqlx::{Connection, Error, FromRow, Sqlite, SqliteConnection};

    use super::{Job, claiming, interned, queue};

    /// The same jobs, each read by the service's own fetch after its take.
    #[derive(Debug, Inbox, FromRow)]
    #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(fetch))]
    struct Fetched {
        #[field(id)]
        id: i64,
        #[field(attempt)]
        attempt: i16,
        #[field(processed_at)]
        processed_at: Option<DateTime<Utc>>,
        #[field(payload)]
        payload: Vec<u8>,
    }

    /// Jobs 1 and 2 waiting at their first attempt, job 2 with no payload, and job 3
    /// finished.
    async fn jobs() -> Result<SqliteConnection, Error> {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        sqlx::raw_sql(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY, attempt INTEGER NOT NULL, \
             processed_at TEXT, payload BLOB NOT NULL); \
             INSERT INTO jobs VALUES (1, 1, NULL, x'01'), (2, 1, NULL, x''), \
             (3, 1, '2026-10-06T00:00:00Z', x'03')",
        )
        .execute(&mut conn)
        .await?;
        Ok(conn)
    }

    async fn attempt(conn: &mut SqliteConnection, id: i64) -> Result<i16, Error> {
        sqlx::query_scalar("SELECT attempt FROM jobs WHERE id = ?")
            .bind(id)
            .fetch_one(conn)
            .await
    }

    /// The subscription to `spec` whose take is `take`, in one statement or two.
    fn taking(spec: &TableSpec<'static>, take: &[Statement]) -> &'static Queue {
        queue(
            spec,
            &Prepared {
                take: take.first().map(interned),
                take_then: take.get(1).map(interned),
                ..Prepared::default()
            },
        )
    }

    /// The take of `spec` in `shape`, as SQLite writes it in one statement and as MySQL
    /// writes it in two, which SQLite runs too.
    fn takes(spec: &TableSpec<'static>, shape: ClaimShape) -> [Vec<Statement>; 2] {
        let one = dialect::Sqlite.take(spec, shape).expect("SQLite takes");
        let two = dialect::MySql.take(spec, shape).expect("MySQL takes");
        assert_eq!((one.len(), two.len()), (1, 2));
        [one, two]
    }

    impl Fetch<Sqlite> for Fetched {
        async fn fetch(conn: &mut SqliteConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
            // A job without a payload is one the service's fetch does not find.
            let mut rows = Vec::new();
            for id in ids {
                rows.extend(
                    sqlx::query_as::<_, Self>(
                        "SELECT id, attempt, processed_at, payload FROM jobs \
                         WHERE id = ? AND length(payload) > 0",
                    )
                    .bind(id)
                    .fetch_optional(&mut *conn)
                    .await?,
                );
            }
            Ok(rows)
        }
    }

    #[tokio::test]
    async fn a_take_counts_the_attempt_and_reads_the_row_while_it_is_claimable() -> Result<(), Error>
    {
        for take in takes(&Job::SPEC, ClaimShape::Rows) {
            let cx = claiming(taking(&Job::SPEC, &take));
            let mut conn = jobs().await?;
            let mut out = Vec::new();
            assert!(<Job as Events<Sqlite>>::take(&mut conn, &cx, &1, &mut out).await?);
            // The row comes as it was before the count, which the table now holds.
            assert!(
                matches!(
                    out.as_slice(),
                    [Claimed::Row(Job {
                        id: 1,
                        attempt: 1,
                        ..
                    })]
                ),
                "{out:?}"
            );
            assert_eq!(attempt(&mut conn, 1).await?, 2);
            // A finished job is no longer claimable: its take counts and reads nothing.
            assert!(!<Job as Events<Sqlite>>::take(&mut conn, &cx, &3, &mut out).await?);
            assert_eq!(out.len(), 1, "{out:?}");
            assert_eq!(attempt(&mut conn, 3).await?, 1);
        }
        let _ = |job: Job| (job.processed_at, job.payload);
        Ok(())
    }

    #[tokio::test]
    async fn a_row_the_service_fetches_is_read_by_its_fetch_after_the_take() -> Result<(), Error> {
        for take in takes(&Fetched::SPEC, ClaimShape::Ids) {
            let cx = claiming(taking(&Fetched::SPEC, &take));
            let mut conn = jobs().await?;
            let mut out = Vec::new();
            for id in [1, 2] {
                assert!(<Fetched as Events<Sqlite>>::take(&mut conn, &cx, &id, &mut out).await?);
            }
            assert!(!<Fetched as Events<Sqlite>>::take(&mut conn, &cx, &3, &mut out).await?);
            // The fetch reads the counted row; a taken row it does not find is missing.
            assert!(
                matches!(
                    out.as_slice(),
                    [
                        Claimed::Row(Fetched {
                            id: 1,
                            attempt: 2,
                            ..
                        }),
                        Claimed::Missing(2)
                    ]
                ),
                "{out:?}"
            );
            assert_eq!(
                (attempt(&mut conn, 2).await?, attempt(&mut conn, 3).await?),
                (2, 1)
            );
        }
        let _ = |row: Fetched| (row.processed_at, row.payload);
        Ok(())
    }

    #[tokio::test]
    async fn a_dialect_that_keeps_no_locks_prepares_none_to_run() -> Result<(), Error> {
        let take = dialect::Sqlite
            .take(&Job::SPEC, ClaimShape::Rows)
            .expect("SQLite takes");
        assert_eq!(dialect::Sqlite.lock(), None);
        let queue = taking(&Job::SPEC, &take);
        let cx = claiming(queue);
        let mut conn = jobs().await?;
        let locked = <Job as Events<Sqlite>>::lock(&mut conn, &cx, "jobs-1").await;
        assert!(
            matches!(&locked, Err(Error::Configuration(message))
                if message.to_string() == "the subscription prepared no lock statement"),
            "{locked:?}"
        );
        let settling = Settling { queue, now: cx.now };
        let unlocked = <Job as Events<Sqlite>>::unlock(&mut conn, &settling, "jobs-1").await;
        assert!(
            matches!(&unlocked, Err(Error::Configuration(message))
                if message.to_string() == "the subscription prepared no unlock statement"),
            "{unlocked:?}"
        );
        Ok(())
    }
}
