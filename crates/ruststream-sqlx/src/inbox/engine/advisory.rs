//! The advisory lock form's default events: the candidates of a claim with their keys, the lock and
//! the unlock of a key, and the take of a candidate whose key the delivery's session holds.

use sqlx::{Decode, Error, Type};

use super::{Claimed, Claiming, Event, Events, Settling, Stmt, Values, arguments, unprepared};
use crate::inbox::database::QueueDatabase;

/// The candidates of one claim: ids with their keys, kept between claims so a key's buffer is
/// written over, not allocated, once the claim has seen as many. Machinery.
#[doc(hidden)]
#[derive(Debug)]
pub struct Candidates<Id> {
    /// The entries the claims so far filled: the first `len` are this claim's, and each of the
    /// rest keeps its key's buffer for a later claim.
    entries: Vec<(Option<Id>, String)>,
    /// How many entries this claim filled.
    len: usize,
}

impl<Id> Default for Candidates<Id> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            len: 0,
        }
    }
}

impl<Id> Candidates<Id> {
    /// Empties it for the next claim: each id goes, each key's buffer stays.
    pub fn clear(&mut self) {
        for (id, _) in &mut self.entries[..self.len] {
            *id = None;
        }
        self.len = 0;
    }

    /// Adds the candidate `id`, its lock key copied into the buffer an earlier claim left in its
    /// place, or into a new one past the most candidates a claim has seen.
    pub fn push(&mut self, id: Id, key: &str) {
        match self.entries.get_mut(self.len) {
            Some((kept, buffer)) => {
                *kept = Some(id);
                buffer.clear();
                buffer.push_str(key);
            }
            None => self.entries.push((Some(id), key.to_owned())),
        }
        self.len += 1;
    }

    /// How many candidates the claim found.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the claim found none.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The candidates in claim order, each id with its lock key.
    pub fn iter(&self) -> impl Iterator<Item = (&Id, &str)> {
        self.entries[..self.len]
            .iter()
            .filter_map(|(id, key)| Some((id.as_ref()?, key.as_str())))
    }
}

/// The candidates of an advisory claim: up to `cx.limit` claimable rows in claim order, each id
/// with its lock key, written over the last claim's.
///
/// # Errors
///
/// The database's error.
#[expect(
    dead_code,
    reason = "the subscriber's claim in the advisory lock form runs it"
)]
pub(crate) async fn candidates<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    out: &mut Candidates<Row::Id>,
) -> Result<(), Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB>,
{
    let statement = cx
        .queue
        .prepared
        .claim
        .ok_or_else(|| unprepared(Event::Claim))?;
    let arguments = arguments::<DB, Row>(statement, &Values::claiming(*cx, Event::Claim, None))?;
    out.clear();
    DB::fetch_candidates(conn, statement.sql, arguments, out).await
}

/// The default `lock`: tries the lock on `key` for the session of `conn`, without waiting;
/// `true` when it took the lock.
///
/// # Errors
///
/// The database's error, or [`Error::Configuration`] where the subscription prepared no lock: its
/// dialect leaves the locks to the process.
pub async fn lock<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    key: &str,
) -> Result<bool, Error> {
    let statement = cx
        .queue
        .prepared
        .lock
        .ok_or_else(|| unprepared(Event::Lock))?;
    let values = Values {
        key: Some(key),
        ..Values::claiming(*cx, Event::Lock, None)
    };
    flag::<DB, Row>(conn, statement, &values).await
}

/// The default `unlock`: releases the lock on `key` the session of `conn` holds; `true` when it
/// held it.
///
/// # Errors
///
/// As [`lock`].
pub async fn unlock<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    key: &str,
) -> Result<bool, Error> {
    let statement = cx
        .queue
        .prepared
        .unlock
        .ok_or_else(|| unprepared(Event::Unlock))?;
    flag::<DB, Row>(conn, statement, &Values::unlocking(*cx, key)).await
}

/// Runs `statement`, whose one row starts with a 64-bit integer, and says whether it is nonzero.
async fn flag<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    statement: Stmt,
    values: &Values<'_, DB, Row>,
) -> Result<bool, Error> {
    let arguments = arguments::<DB, Row>(statement, values)?;
    DB::fetch_flag(conn, statement.sql, arguments).await
}

/// The default take of the candidate `id`, whose lock the session of `conn` holds.
///
/// The dialect's take counts the attempt and reads the row while it is still claimable, and the
/// row goes onto `out`; `false` when the row is gone or no longer claimable. A take of two
/// statements reads the row only once the first changed it; a take of one statement is the read.
///
/// # Errors
///
/// The database's error, or the decode error of a row whose id does not decode either.
pub async fn take<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    id: &Row::Id,
    out: &mut Vec<Claimed<Row>>,
) -> Result<bool, Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB>,
{
    let take = cx
        .queue
        .prepared
        .take
        .ok_or_else(|| unprepared(Event::Take))?;
    let values = Values {
        id: Some(id),
        ..Values::claiming(*cx, Event::Take, None)
    };
    let read = match cx.queue.prepared.take_then {
        Some(then) => {
            let arguments = arguments::<DB, Row>(take, &values)?;
            // A count that changed no row found the row gone or no longer claimable.
            if DB::execute(conn, take.sql, arguments).await? == 0 {
                return Ok(false);
            }
            then
        }
        None => take,
    };
    let taken = out.len();
    let arguments = arguments::<DB, Row>(read, &values)?;
    DB::fetch_rows(conn, read.sql, arguments, cx.queue, out).await?;
    Ok(out.len() > taken)
}

/// The take of the candidate `id` of a row the service's own fetch reads: it counts the attempt
/// as [`take`] does and says whether the row is still claimable, reading none of its columns.
///
/// # Errors
///
/// The database's error.
pub async fn take_id<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    id: &Row::Id,
) -> Result<bool, Error> {
    let take = cx
        .queue
        .prepared
        .take
        .ok_or_else(|| unprepared(Event::Take))?;
    let values = Values {
        id: Some(id),
        ..Values::claiming(*cx, Event::Take, None)
    };
    let arguments = arguments::<DB, Row>(take, &values)?;
    if cx.queue.prepared.take_then.is_some() {
        // The count changed the row only while it was claimable, and the read after it would
        // return the id the claim holds already.
        return Ok(DB::execute(conn, take.sql, arguments).await? > 0);
    }
    DB::fetch_found(conn, take.sql, arguments).await
}

/// Pairs the taken `id` with the rows the service's fetch returned for it: its row, or
/// [`Claimed::Missing`] where the fetch found none.
pub fn match_taken<DB, Row>(id: &Row::Id, rows: Vec<Row>, out: &mut Vec<Claimed<Row>>)
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: PartialEq,
{
    let row = rows.into_iter().find(|row| Row::id(row) == id);
    out.push(row.map_or_else(|| Claimed::Missing(id.clone()), Claimed::Row));
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use sqlx::sqlite::SqliteArguments;
    use sqlx::{Connection, Error, Sqlite, SqliteConnection};

    use super::Candidates;
    use crate::inbox::database::QueueDatabase;

    /// The candidates' ids and keys, in claim order.
    fn listed(candidates: &Candidates<i64>) -> Vec<(i64, String)> {
        candidates
            .iter()
            .map(|(id, key)| (*id, key.to_owned()))
            .collect()
    }

    #[tokio::test]
    async fn the_candidates_write_each_key_over_the_buffer_a_claim_before_left() -> Result<(), Error>
    {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        let mut candidates = Candidates::default();
        <Sqlite as QueueDatabase>::fetch_candidates(
            &mut conn,
            "SELECT 1, 'jobs-1' UNION ALL SELECT 2, 'jobs-22'",
            SqliteArguments::default(),
            &mut candidates,
        )
        .await?;
        assert_eq!(
            listed(&candidates),
            [(1, "jobs-1".to_owned()), (2, "jobs-22".to_owned())]
        );
        let buffers: Vec<*const u8> = candidates.iter().map(|(_, key)| key.as_ptr()).collect();
        candidates.clear();
        assert!(candidates.is_empty());
        // A claim of fewer candidates, and keys no longer than the buffers it finds.
        <Sqlite as QueueDatabase>::fetch_candidates(
            &mut conn,
            "SELECT 3, 'jobs-3'",
            SqliteArguments::default(),
            &mut candidates,
        )
        .await?;
        assert_eq!(listed(&candidates), [(3, "jobs-3".to_owned())]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates.iter().map(|(_, key)| key.as_ptr()).next(),
            Some(buffers[0]),
            "the key is written over the buffer the first claim left"
        );
        // A claim of more candidates than any before keeps the buffers it has, and adds one.
        candidates.clear();
        <Sqlite as QueueDatabase>::fetch_candidates(
            &mut conn,
            "SELECT 4, 'jobs-4' UNION ALL SELECT 5, 'jobs-5' UNION ALL SELECT 6, 'jobs-6'",
            SqliteArguments::default(),
            &mut candidates,
        )
        .await?;
        assert_eq!(candidates.len(), 3);
        let reused: Vec<*const u8> = candidates.iter().map(|(_, key)| key.as_ptr()).collect();
        assert_eq!(reused[..2], buffers[..]);
        conn.close().await
    }

    #[tokio::test]
    async fn a_statement_is_found_when_it_returns_a_row() -> Result<(), Error> {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        let returned = <Sqlite as QueueDatabase>::fetch_found(
            &mut conn,
            "SELECT 1",
            SqliteArguments::default(),
        )
        .await?;
        assert!(returned);
        let empty = <Sqlite as QueueDatabase>::fetch_found(
            &mut conn,
            "SELECT 1 WHERE 0",
            SqliteArguments::default(),
        )
        .await?;
        assert!(!empty);
        conn.close().await
    }
}
