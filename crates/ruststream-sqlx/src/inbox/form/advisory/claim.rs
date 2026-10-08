//! The advisory lock form's claim: each candidate's key locked on a session of its own, and its row
//! taken while it is still claimable.

use sqlx::{Database, Pool, SqlStr};
use sqlx_core::transaction::TransactionManager;

use super::events::Candidates;
use super::session::{Closing, Session};
use super::{LockBook, LockHold};
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{self, Claimed, Claiming, Events, Prepared, Settling};
use crate::inbox::subscriber::Failed;

/// What an advisory claim keeps beside its rows.
pub(crate) struct Advised<DB: QueueDatabase, Row: Events<DB>> {
    /// The place of each row in the subscription's book, beside the row.
    pub(crate) holds: Vec<LockHold<DB>>,
    /// The candidates of the last claim, whose keys' buffers the next one writes over.
    pub(crate) candidates: Candidates<Row::Id>,
    /// The candidates the last claim took, by their place among the candidates.
    pub(crate) taken: Vec<usize>,
}

impl<DB: QueueDatabase, Row: Events<DB>> Default for Advised<DB, Row> {
    fn default() -> Self {
        Self {
            holds: Vec::new(),
            candidates: Candidates::default(),
            taken: Vec::new(),
        }
    }
}

/// Claims up to `limit` rows of an advisory subscription: each candidate's key locked on a session
/// of its own, and its row taken while it is still claimable. The rows go into `rows`, and each
/// row's place in `book` beside it into `advised`.
///
/// The first session waits for the pool; each next one is an idle connection, or a new one while
/// the pool is below its size, and the claim ends where the pool is full. A key this claim took
/// already is passed over, and so is a key another session holds. The candidate select reads as
/// many rows past `limit` as there are keys in work it cannot leave out. A session left over holds
/// nothing and goes back to the pool. A claim that fails or is dropped midway leaves no lock: what
/// it took drops, so each session ends, one that may hold a lock closed after an unlock of its key,
/// and each row returns. The select reads one more candidate for each of the `beside` claims of the
/// subscription in flight, which may lock the first ones meanwhile.
pub(crate) async fn claim_advised<DB, Row>(
    pool: &Pool<DB>,
    book: &'static LockBook<DB>,
    cx: &Claiming,
    limit: usize,
    beside: usize,
    rows: &mut Vec<Claimed<Row>>,
    advised: &mut Advised<DB, Row>,
) -> Result<(), Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let prepared = &cx.queue.prepared;
    let named = |statement: Option<engine::Stmt>, event: &'static str| {
        move |source| -> Failed { (statement.map_or(event, |statement| statement.sql), source) }
    };
    let settling = Settling {
        queue: cx.queue,
        now: cx.now,
    };
    let Advised {
        holds,
        candidates,
        taken,
    } = advised;
    let mut taking = Taking {
        rows,
        holds,
        done: false,
    };
    let mut session = Session::acquire(pool, book.closing())
        .await
        .map_err(|source| ("acquire", source))?;
    select_candidates::<DB, Row>(&mut session, book, cx, beside, candidates)
        .await
        .map_err(named(prepared.claim, "claim"))?;
    let mut spare = Some(session);
    for index in 0..candidates.len() {
        let Some((id, key)) = candidates.get(index) else {
            continue;
        };
        let held_already = taken.iter().any(|&earlier| {
            candidates
                .get(earlier)
                .is_some_and(|(_, other)| other == key)
        });
        if held_already {
            continue;
        }
        if spare.is_none() {
            spare = next_session(pool, book.closing()).await;
        }
        let Some(session) = spare.take() else {
            break;
        };
        // From its lock until the book holds it, the session goes with the candidate's key: a
        // claim dropped or failed midway closes it after an unlock of the key, so the lock is gone
        // once the close ends.
        let mut locked = book.locked(session, key);
        let took = if book.process() {
            locked.take_in_process()
        } else {
            // Marked before the statement leaves: a lock statement dropped midway may have taken
            // the lock, and a session that may hold one closes instead of going back to the pool.
            locked.session().set_locked(true);
            let took = Row::lock(locked.session().conn(), cx, key)
                .await
                .map_err(named(prepared.lock, "lock"))?;
            if !took {
                locked.session().set_locked(false);
            }
            took
        };
        if !took {
            spare = Some(locked.into_session());
            continue;
        }
        let before = taking.rows.len();
        let found = Row::take(locked.session().conn(), cx, id, taking.rows)
            .await
            .map_err(named(prepared.take, "take"))?;
        if !found {
            // Another holder settled the row between the select and the lock: its key goes, and
            // the session tries the next candidate.
            let freed = free_key::<DB, Row>(locked.session(), book, &settling, key).await?;
            let session = locked.into_session();
            if freed {
                spare = Some(session);
            } else {
                // The database did not confirm the release: the session closes, and the claim
                // goes on with another one.
                drop(session);
                spare = None;
            }
            continue;
        }
        let read = taking.rows.len() - before;
        if read != 1 {
            return Err((
                prepared.take.map_or("take", |take| take.sql),
                sqlx::Error::Protocol(format!("the take of one candidate read {read} rows")),
            ));
        }
        if prepared.transactional {
            begin_on(locked.session(), prepared).await?;
        }
        taking.holds.push(book.enter(key, locked.into_session()));
        taken.push(index);
        if taking.holds.len() >= limit {
            break;
        }
    }
    // A session left over holds nothing, and goes back to the pool.
    drop(spare);
    taking.done = true;
    Ok(())
}

/// Selects the candidates of an advisory claim on `session` into `candidates`: up to `cx.limit`
/// rows, and as many more as there are keys in work the select cannot leave out and `beside`
/// claims in flight. The select leaves
/// out the keys in work only where it probes the locks that hold them, so past the keys it cannot
/// see, it reads further, and a key in work at the head of the claim order does not hold back the
/// rows behind it.
async fn select_candidates<DB, Row>(
    session: &mut Session<DB>,
    book: &LockBook<DB>,
    cx: &Claiming,
    beside: usize,
    candidates: &mut Candidates<Row::Id>,
) -> Result<(), sqlx::Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let unseen = i64::try_from(book.unseen_in_work().saturating_add(beside)).unwrap_or(i64::MAX);
    let reach = Claiming {
        limit: cx.limit.saturating_add(unseen),
        ..*cx
    };
    Row::candidates(session.conn(), &reach, candidates).await
}

/// The session an advisory claim locks its next candidate on: an idle connection of `pool`, or a
/// new one while the pool is below its size. `None` where the pool is full, so a batch larger than
/// the pool shrinks instead of waiting for a delivery in work to settle.
async fn next_session<DB: Database>(
    pool: &Pool<DB>,
    closing: &'static Closing,
) -> Option<Session<DB>> {
    if let Some(session) = Session::try_acquire(pool, closing) {
        return Some(session);
    }
    if pool.size() >= pool.options().get_max_connections() {
        return None;
    }
    // Why a wait remains: the pool has no call that opens a connection only while it has room.
    // Another task may take the last place between the count above and this acquire, which then
    // waits for a connection to come back, at most the pool's acquire timeout; a claim whose
    // acquire fails ends with what it took.
    Session::acquire(pool, closing).await.ok()
}

/// The rows and the holds of an advisory claim in progress. Dropped before the claim is done, by
/// an error or a cancellation, it drops what the claim took.
struct Taking<'a, DB: QueueDatabase, Row: Events<DB>> {
    rows: &'a mut Vec<Claimed<Row>>,
    holds: &'a mut Vec<LockHold<DB>>,
    done: bool,
}

impl<DB: QueueDatabase, Row: Events<DB>> Drop for Taking<'_, DB, Row> {
    fn drop(&mut self) {
        if !self.done {
            self.holds.clear();
            self.rows.clear();
        }
    }
}

/// Frees `key`, which `session` took for a candidate the take found gone: from the process, or
/// with the row's unlock. `false` when the database did not confirm the release, and the session
/// then ends closed.
async fn free_key<DB, Row>(
    session: &mut Session<DB>,
    book: &LockBook<DB>,
    cx: &Settling,
    key: &str,
) -> Result<bool, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    if book.process() {
        session.free_in_process();
        return Ok(true);
    }
    let unlocked = Row::unlock(session.conn(), cx, key)
        .await
        .map_err(|source| {
            let unlock = cx.queue.prepared.unlock;
            (unlock.map_or("unlock", |unlock| unlock.sql), source)
        })?;
    if unlocked {
        session.set_locked(false);
    } else {
        tracing::warn!(
            target: "ruststream_sqlx",
            subscription = cx.queue.name,
            table = cx.queue.table,
            row = cx.queue.row,
            key,
            "the unlock of a candidate's key found the session not holding it; the session \
             closes",
        );
    }
    Ok(unlocked)
}

/// Opens the handler's transaction on `session`, whose lock holds a taken row's key, at the
/// table's isolation level or SQLite mode, or with `BEGIN` where the table names neither:
/// transactional mode in the advisory lock form.
async fn begin_on<DB: QueueDatabase>(
    session: &mut Session<DB>,
    prepared: &Prepared,
) -> Result<(), Failed> {
    // Open before the statement leaves: a begin dropped midway may have started the transaction,
    // and a session that may hold one closes instead of going back to the pool.
    session.set_open(true);
    let begin = prepared.begin_work;
    DB::TransactionManager::begin(session.conn(), begin.map(SqlStr::from_static))
        .await
        .map_err(|source| (begin.unwrap_or("BEGIN"), source))
}
