//! One trait per event of the queue: what a service implements when it lists the event in
//! `#[inbox(custom(..))]`, and `Publish`, which has no default.

use std::future::Future;
use std::time::Duration;

use ruststream::OutgoingMessage;
use sqlx::{Database, Error};

use super::InboxRow;
use super::time::LeaseRow;

/// Claims up to `limit` rows of the queue `queue` and returns their ids, inside the claim's
/// transaction.
///
/// The derive builds it from the roles; a service lists `claim` in `custom(..)` to take it over,
/// for a database without a built-in dialect or a claim of its own. The crate then reads the
/// rows with the service's [`Fetch`], or with its own fetch by a list of ids, which Postgres
/// alone runs: on MySQL and SQLite a claim of the service's own comes with a `Fetch` of its own,
/// and a subscription without one stops at startup. In the lease form the crate also leases each
/// id the claim returns, in the same transaction, and passes over an id whose row another lease
/// holds.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::{Claim, Inbox};
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs", custom(claim))]
/// pub struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Claim<Postgres> for Job {
///     async fn claim(
///         conn: &mut PgConnection,
///         _queue: &str,
///         limit: i64,
///     ) -> Result<Vec<i64>, sqlx::Error> {
///         // The oldest rows first, skipping the ones another worker holds.
///         sqlx::query_scalar("SELECT id FROM jobs ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED")
///             .bind(limit)
///             .fetch_all(conn)
///             .await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `claim` in `#[inbox(custom(..))]` and does not implement `Claim<{DB}>`",
    label = "the service's own claim is missing",
    note = "implement `Claim<{DB}>` for `{Self}`, or drop `claim` from `custom(..)`"
)]
pub trait Claim<DB: Database>: InboxRow {
    /// Claims up to `limit` rows and returns their ids.
    ///
    /// # Errors
    ///
    /// The database's error; the subscription waits one second and claims again.
    fn claim(
        conn: &mut DB::Connection,
        queue: &str,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<Self::Id>, Error>> + Send;
}

/// Reads the rows of claimed ids, on the claim's connection.
///
/// The derive builds it for a flat table; a service lists `fetch` in `custom(..)` to assemble
/// messages itself, from other tables. Rows are matched to the claimed ids by their `id` field; a
/// claimed id with no row settles by the decode-failure policy before its handler runs, and the
/// log names the id. It runs inside the claim's transaction, or right after a lease claim that
/// leased the rows and committed in one statement, as on Postgres and SQLite. In the advisory lock
/// form it runs once per row, with that row's id alone, after the take that counted its attempt.
/// Either way a delivery reports the attempt its row held before the claim. It runs only after a
/// claim that took ids, so `ids` is never empty.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::{Fetch, Inbox};
/// use sqlx::{PgConnection, Postgres};
///
/// /// A job whose message is the body of the order it refers to.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs", custom(fetch))]
/// pub struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Fetch<Postgres> for Job {
///     async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
///         sqlx::query_as(
///             "SELECT j.id, o.body AS payload FROM jobs j JOIN orders o ON o.id = j.order_id \
///              WHERE j.id = ANY($1)",
///         )
///         .bind(ids)
///         .fetch_all(conn)
///         .await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `fetch` in `#[inbox(custom(..))]` and does not implement `Fetch<{DB}>`",
    label = "the service's own fetch is missing",
    note = "implement `Fetch<{DB}>` for `{Self}`, or drop `fetch` from `custom(..)`"
)]
pub trait Fetch<DB: Database>: InboxRow {
    /// The rows of `ids`, in any order.
    ///
    /// # Errors
    ///
    /// The database's error; the claim fails and its rows return to the queue.
    fn fetch(
        conn: &mut DB::Connection,
        ids: &[Self::Id],
    ) -> impl Future<Output = Result<Vec<Self>, Error>> + Send;
}

macro_rules! settle_event {
    (
        $(#[$doc:meta])*
        $trait:ident, $method:ident, $event:literal,
        gate = $gate:literal, $(uses = $uses:literal,)? $(fields = $fields:literal,)? body = $body:literal,
        message = $message:literal, note = $note:literal
        $(, $extra:ident: $extra_ty:ty = $extra_value:literal)?
    ) => {
        $(#[$doc])*
        #[doc = ""]
        #[doc = "# Examples"]
        #[doc = ""]
        #[doc = "```"]
        #[doc = concat!("# #[cfg(", $gate, ")]")]
        #[doc = "# mod demo {"]
        #[doc = "use std::time::Duration;"]
        #[doc = ""]
        #[doc = concat!("use ruststream_sqlx::{", stringify!($trait), ", Inbox};")]
        #[doc = "use sqlx::{PgConnection, Postgres};"]
        $(#[doc = $uses])?
        #[doc = ""]
        #[doc = "#[derive(Inbox, sqlx::FromRow)]"]
        #[doc = concat!("#[inbox(table = \"jobs\", custom(", $event, "))]")]
        #[doc = "pub struct Job {"]
        #[doc = "    #[field(id)]"]
        #[doc = "    id: i64,"]
        $(#[doc = $fields])?
        #[doc = "    #[field(payload)]"]
        #[doc = "    payload: Vec<u8>,"]
        #[doc = "}"]
        #[doc = ""]
        #[doc = concat!("impl ", stringify!($trait), "<Postgres> for Job {")]
        #[doc = concat!("    async fn ", stringify!($method), "(")]
        #[doc = "        conn: &mut PgConnection,"]
        #[doc = "        id: &i64,"]
        $(#[doc = concat!("        ", stringify!($extra), ": ", stringify!($extra_ty), ",")])?
        #[doc = "    ) -> Result<(), sqlx::Error> {"]
        #[doc = $body]
        #[doc = "        Ok(())"]
        #[doc = "    }"]
        #[doc = "}"]
        #[doc = "# }"]
        #[doc = "# fn main() {}"]
        #[doc = "```"]
        #[diagnostic::on_unimplemented(
            message = $message,
            label = "the service's own event is missing",
            note = $note
        )]
        pub trait $trait<DB: Database>: InboxRow {
            #[doc = concat!("Runs the `", $event, "` event for the row `id` inside a transaction the crate commits: the claim's, or in the lease form one that first confirms the row still holds the delivery's lease.")]
            #[doc = ""]
            #[doc = "# Errors"]
            #[doc = ""]
            #[doc = "The database's error; the transaction rolls back, and the row returns to the queue at once, or in the lease form once its lease runs out."]
            fn $method(
                conn: &mut DB::Connection,
                id: &Self::Id,
                $($extra: $extra_ty,)?
            ) -> impl Future<Output = Result<(), Error>> + Send;
        }
    };
}

settle_event!(
    /// Acknowledges a row: the derive deletes it, or sets `processed_at`.
    ///
    /// The service's own acknowledgement takes the row out of what the claim selects: it deletes
    /// the row, moves it, or marks it in a column the claim passes over. A row left claimable is
    /// delivered again.
    Ack, ack, "ack",
    gate = "feature = \"postgres\"",
    body = "        // The finished job moves to an archive in one statement.
        sqlx::query(
            \"WITH done AS (DELETE FROM jobs WHERE id = $1 RETURNING *) \\
             INSERT INTO jobs_done SELECT * FROM done\",
        )
        .bind(id)
        .execute(conn)
        .await?;",
    message = "`{Self}` lists `ack` in `#[inbox(custom(..))]` and does not implement `Ack<{DB}>`",
    note = "implement `Ack<{DB}>` for `{Self}`, or drop `ack` from `custom(..)`"
);

settle_event!(
    /// Releases a row for another attempt at once: the derive counts the attempt, or leaves the
    /// row for the rollback to release; in the lease form it clears the lease.
    ///
    /// In the lease form the row comes back once `locked_until` no longer holds it: the service's
    /// own retry clears the column to release the row at once, and otherwise the row waits for its
    /// lease to run out.
    Retry, retry, "retry",
    gate = "feature = \"postgres\"",
    body = "        sqlx::query(\"UPDATE jobs SET tries = tries + 1 WHERE id = $1\")
            .bind(id)
            .execute(conn)
            .await?;",
    message = "`{Self}` lists `retry` in `#[inbox(custom(..))]` and does not implement `Retry<{DB}>`",
    note = "implement `Retry<{DB}>` for `{Self}`, or drop `retry` from `custom(..)`"
);

settle_event!(
    /// Releases a row for another attempt after `delay`: the derive sets `retry_after`.
    ///
    /// A row with this event, or a `retry_after` field, is redelivered by the database's own
    /// clock. The service's own event delays a row only where the claim passes over it until
    /// then: the default claim reads the `retry_after` field.
    RetryAfter, retry_after, "retry_after",
    gate = "all(feature = \"postgres\", feature = \"chrono\")",
    uses = "use chrono::{DateTime, Utc};",
    fields = "    #[field(retry_after)]
    retry_after: DateTime<Utc>,",
    body = "        // The delay counts from the database's clock, whatever the host's says.
        sqlx::query(
            \"UPDATE jobs SET retry_after = now() + make_interval(secs => $2) WHERE id = $1\",
        )
        .bind(id)
        .bind(delay.as_secs_f64())
        .execute(conn)
        .await?;",
    message = "`{Self}` lists `retry_after` in `#[inbox(custom(..))]` and does not implement `RetryAfter<{DB}>`",
    note = "implement `RetryAfter<{DB}>` for `{Self}`, or drop `retry_after` from `custom(..)`",
    delay: Duration = "Duration::from_secs(30)"
);

settle_event!(
    /// Drops a row: the derive deletes it, or sets `processed_at`.
    ///
    /// The service's own drop takes the row out of what the claim selects, as an acknowledgement
    /// does.
    Discard, discard, "discard",
    gate = "feature = \"postgres\"",
    body = "        sqlx::query(\"DELETE FROM jobs WHERE id = $1\")
            .bind(id)
            .execute(conn)
            .await?;",
    message = "`{Self}` lists `discard` in `#[inbox(custom(..))]` and does not implement `Discard<{DB}>`",
    note = "implement `Discard<{DB}>` for `{Self}`, or drop `discard` from `custom(..)`"
);

settle_event!(
    /// Moves a row whose attempts are spent to `destination`: the derive moves it to that group
    /// (with a `group` field) or into that table.
    ///
    /// The service's own move takes the row out of what the claim selects; a row copied and left
    /// behind is moved again at every delivery.
    DeadLetter, dead_letter, "dead_letter",
    gate = "feature = \"postgres\"",
    body = "        // One dead-letter table serves the whole service, whatever the destination names.
        sqlx::query(
            \"WITH dead AS (DELETE FROM jobs WHERE id = $1 RETURNING *) \\
             INSERT INTO dead_jobs SELECT * FROM dead\",
        )
        .bind(id)
        .execute(conn)
        .await?;
        tracing::warn!(id, destination, \"a job's attempts are spent\");",
    message = "`{Self}` lists `dead_letter` in `#[inbox(custom(..))]` and does not implement `DeadLetter<{DB}>`",
    note = "implement `DeadLetter<{DB}>` for `{Self}`, or drop `dead_letter` from `custom(..)`",
    destination: &str = "\"jobs.dead\""
);

/// Extends the lease on a row: writes `until` into `locked_until` while the row still holds
/// `held`, and says whether it did.
///
/// The derive builds it for every table with a `#[field(locked_until)]` field; a service lists
/// `extend` in `custom(..)` to take it over, for a database without a built-in dialect. A
/// subscription runs it each half lease for every delivery in work, on one connection. The crate
/// also runs it with `until` equal to `held` to confirm a delivery's lease, inside the transaction
/// where an event of the service's own then runs.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::{Extend, Inbox};
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs", custom(extend))]
/// pub struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(locked_until)]
///     locked_until: Option<DateTime<Utc>>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Extend<Postgres> for Job {
///     async fn extend(
///         conn: &mut PgConnection,
///         id: &i64,
///         held: &DateTime<Utc>,
///         until: &DateTime<Utc>,
///     ) -> Result<bool, sqlx::Error> {
///         // The row keeps its lease only while it holds the expiry the delivery knows.
///         let extended =
///             sqlx::query("UPDATE jobs SET locked_until = $1 WHERE id = $2 AND locked_until = $3")
///                 .bind(until)
///                 .bind(id)
///                 .bind(held)
///                 .execute(conn)
///                 .await?;
///         Ok(extended.rows_affected() == 1)
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `extend` in `#[inbox(custom(..))]` and does not implement `Extend<{DB}>`",
    label = "the service's own extension is missing",
    note = "implement `Extend<{DB}>` for `{Self}`, or drop `extend` from `custom(..)`"
)]
pub trait Extend<DB: Database>: LeaseRow {
    /// Writes `until` into the lease of the row `id` while it still holds `held`; `true` when it
    /// did, `false` when the row no longer holds `held`.
    ///
    /// # Errors
    ///
    /// The database's error.
    fn extend(
        conn: &mut DB::Connection,
        id: &Self::Id,
        held: &Self::Lease,
        until: &Self::Lease,
    ) -> impl Future<Output = Result<bool, Error>> + Send;
}

/// Takes the advisory lock on a row's key for the session that will hold its delivery, without
/// waiting: the `lock` event of the advisory lock form.
///
/// The dialect takes the lock itself on Postgres and MySQL, and the process keeps the keys in work
/// on SQLite. A table lists `lock` and `unlock` in `custom(..)` together to run the service's own
/// SQL for them instead, as a database without a built-in dialect does, MSSQL with
/// `sp_getapplock` and `sp_releaseapplock`; the dialect still selects the candidates and takes
/// each row. A claim calls it once per candidate with the key the template renders, on the
/// connection that holds the delivery; [`Unlock`] releases the lock when the delivery settles,
/// and where the delivery is dropped unsettled the connection closes after that unlock. With a
/// lock of the service's own the process keeps no registry of keys, on SQLite too.
///
/// The dialect's candidate select cannot see a lock of the service's own, so a claim reads as many
/// candidates past its limit as the subscription has deliveries in work: a key its deliveries hold
/// does not hold back the rows behind it. A key the service's lock holds elsewhere, in another
/// process or system, is passed over within that margin only: while such keys head the claim
/// order, the claim may end empty and wait for its next poll.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::{Inbox, Lock, Unlock};
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(lock, unlock))]
/// pub struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// // The pair MSSQL writes with `sp_getapplock` and `sp_releaseapplock`, written here with
/// // Postgres's session locks on a 32-bit hash of the key.
/// impl Lock<Postgres> for Job {
///     async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
///         sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
///             .bind(key)
///             .fetch_one(conn)
///             .await
///     }
/// }
///
/// impl Unlock<Postgres> for Job {
///     async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
///         sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext($1))")
///             .bind(key)
///             .fetch_one(conn)
///             .await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `lock` in `#[inbox(custom(..))]` and does not implement `Lock<{DB}>`",
    label = "the service's own lock is missing",
    note = "implement `Lock<{DB}>` for `{Self}`, or drop `lock` from `custom(..)`"
)]
pub trait Lock<DB: Database>: InboxRow {
    /// Tries the advisory lock on `key` for the session of `conn`, without waiting: `true` when it
    /// took the lock. The session holds it until `Unlock` releases it or the session ends.
    ///
    /// # Errors
    ///
    /// The database's error; the claim fails, its connection closes, and the next claim waits one
    /// second.
    fn lock(
        conn: &mut DB::Connection,
        key: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;
}

/// Releases the advisory lock on a delivery's key: the `unlock` event of the advisory lock form,
/// which a table lists in `custom(..)` beside [`Lock`].
///
/// A settlement calls it after its statement, on the connection that holds the delivery, and the
/// connection goes back to the pool once it answered `true`. An unlock that answers `false` or
/// fails closes the connection instead, which ends the session and its locks. A delivery dropped
/// unsettled runs it before its connection closes, and `shutdown` runs it for every delivery in
/// work.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::{Inbox, Lock, Unlock};
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs", advisory_lock = "jobs-{id}", custom(lock, unlock))]
/// pub struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// // MSSQL releases with `sp_releaseapplock`; Postgres's session lock stands in for it here.
/// impl Unlock<Postgres> for Job {
///     async fn unlock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
///         sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext($1))")
///             .bind(key)
///             .fetch_one(conn)
///             .await
///     }
/// }
///
/// impl Lock<Postgres> for Job {
///     async fn lock(conn: &mut PgConnection, key: &str) -> Result<bool, sqlx::Error> {
///         sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
///             .bind(key)
///             .fetch_one(conn)
///             .await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `unlock` in `#[inbox(custom(..))]` and does not implement `Unlock<{DB}>`",
    label = "the service's own unlock is missing",
    note = "implement `Unlock<{DB}>` for `{Self}`, or drop `unlock` from `custom(..)`"
)]
pub trait Unlock<DB: Database>: InboxRow {
    /// Releases the advisory lock on `key` the session of `conn` holds: `true` when the session
    /// held it.
    ///
    /// # Errors
    ///
    /// The database's error; the connection closes, which ends the session and its locks.
    fn unlock(
        conn: &mut DB::Connection,
        key: &str,
    ) -> impl Future<Output = Result<bool, Error>> + Send;
}

/// Writes a published message into the table: the name, the bytes and the headers reach the
/// service's SQL, which lays them out in its columns.
///
/// It has no default. With it, the struct's [`Repository`](crate::Repository) is a publisher and
/// a route can lead names to it; without it, a mount that needs one does not compile, rather than
/// publish into nothing. A message carrying a header the struct cannot hold byte for byte (any
/// header, without a `#[field(headers)]` field; see [`HeaderColumn`](crate::HeaderColumn)) is
/// refused before this runs.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::{Inbox, Publish};
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for SendEmail {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
///             .bind(message.name())
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `Publish<{DB}>`, so nothing writes a published message into its table",
    label = "no `Publish` for this row",
    note = "implement `Publish<{DB}>` for `{Self}`: its SQL lays the name, the bytes and the headers out in the table"
)]
pub trait Publish<DB: Database>: InboxRow {
    /// Writes `message` into the table.
    ///
    /// # Errors
    ///
    /// The database's error, which the publish returns.
    fn publish(
        conn: &mut DB::Connection,
        message: &OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Inserts the row: every field except the `generated` ones, the queue's mechanics included, so
/// a delayed task is a row whose `retry_after` lies ahead.
///
/// The derive implements it for the connection of each built-in dialect, `PgConnection` (feature
/// `postgres`), `MySqlConnection` (feature `mysql`) and `SqliteConnection` (feature `sqlite`), with
/// the statements built at compile time. A headers struct gets it from
/// [`InboxHeaders`](derive@crate::InboxHeaders): the queue table's columns.
/// A struct with a `#[sqlx(flatten)]` field gets none: the derive cannot see the nested struct's
/// columns, so the service writes that insert itself.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::{Inbox, Insert, Publish};
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for SendEmail {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         let job = SendEmail {
///             job_id: 0,
///             name: message.name().to_owned(),
///             payload: message.payload().to_vec(),
///         };
///         // `job_id` is generated: the database fills it in.
///         job.insert(conn).await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no generated insert for `{Connection}`",
    label = "no `Insert` for this connection",
    note = "the derive generates it for the connections of the built-in dialects (features \
            `postgres`, `mysql` and `sqlite`), unless a `#[sqlx(flatten)]` field hides columns \
            from it: write that insert in the service"
)]
pub trait Insert<Connection>: Sync {
    /// Inserts the row on `conn`.
    ///
    /// # Errors
    ///
    /// The database's error.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// use ruststream_sqlx::prelude::*;
    /// use serde::Deserialize;
    /// use sqlx::{PgPool, Postgres};
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "signup_jobs")]
    /// pub struct Signup {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// /// A welcome mail to send, in row mode: the handler of `welcome_jobs` takes the row itself.
    /// #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
    /// #[inbox(table = "welcome_jobs")]
    /// pub struct Welcome {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     email: String,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Account {
    ///     email: String,
    /// }
    ///
    /// // The welcome row commits with the signup's acknowledgement, or neither does.
    /// #[subscriber(InboxQueue::<Signup>::new("signups"))]
    /// async fn open_account(account: &Account, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
    ///     let welcome = Welcome { id: 0, email: account.email.clone() };
    ///     match welcome.insert(&mut *tx).await {
    ///         Ok(()) => HandlerOutcome::ack(),
    ///         Err(_) => HandlerOutcome::retry(),
    ///     }
    /// }
    ///
    /// pub fn app(pool: PgPool) -> RustStream {
    ///     RustStream::new(AppInfo::new("accounts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
    ///         b.include(open_account.transactional());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn insert<'c>(
        &'c self,
        conn: &'c mut Connection,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'c;
}
