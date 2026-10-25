//! The lease form's claim: the rows it takes hold the lease it writes and commits, so no
//! transaction holds them; in transactional mode each row's delivery opens a transaction of its
//! own.

use std::ops::{Deref, DerefMut};

use sqlx::Pool;
use sqlx::pool::PoolConnection;

use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{self, Claimed, Claiming, Events, Leasing, Now};
use crate::inbox::form::advisory::session::Closing;
use crate::inbox::queue::Queue;
use crate::inbox::subscriber::{Failed, begin, take_group};
use crate::inbox::tx::PoolTx;

/// Claims up to `cx.limit` rows of a lease subscription into `rows` (a claim of ids reads them
/// into `ids`), and returns the lease they
/// hold: committed before it returns, so the rows hold their leases and no transaction does.
pub(crate) async fn claim_leased<DB, Row>(
    pool: &Pool<DB>,
    closing: &'static Closing,
    cx: &Claiming,
    now: Now,
    ids: &mut Row::Ids,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<Row::Token, Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let queue = cx.queue;
    let claim_failed = |source| -> Failed {
        let statement = queue.prepared.claim.map_or("claim", |claim| claim.sql);
        (statement, source)
    };
    // The lease is read once the connection is in hand, so a wait for the pool does not shorten
    // it. The claim reads "now" there once: the rows it finds due, the leases it finds ended and
    // the expiry it writes start from that instant.
    if queue.prepared.stamps || queue.prepared.fifo_guard.is_some() {
        // The claim only selects, and its transaction stamps each row it took; or it takes its
        // group first, and the group stays taken until the transaction commits its lease. Either
        // way the transaction commits before the handlers run, so the rows hold their leases, not
        // the transaction.
        let mut tx = begin(pool, queue, closing).await?;
        let claimed = async {
            let lease = Row::lease(queue, now).map_err(claim_failed)?;
            let taken = take_group::<DB, Row>(&mut tx, cx, Some(&lease)).await?;
            if taken {
                Row::claim(&mut tx, cx, Some(&lease), ids, rows)
                    .await
                    .map_err(claim_failed)?;
                if queue.prepared.stamps {
                    stamp_rows::<DB, Row>(&mut tx, cx, &lease, rows).await?;
                }
            }
            Ok::<_, Failed>((lease.expiry, taken))
        }
        .await;
        return match claimed {
            Ok((lease, true)) => {
                tx.commit().await.map_err(|source| ("COMMIT", source))?;
                Ok(lease)
            }
            // Another transaction holds the group: the claim took nothing, and the rollback lets
            // go of what the guard read.
            Ok((lease, false)) => {
                let _ = tx.rollback().await;
                Ok(lease)
            }
            Err(failed) => {
                let _ = tx.rollback().await;
                Err(failed)
            }
        };
    }
    // The claim writes the lease itself, in one statement that commits on its own.
    let conn = pool.acquire().await.map_err(|source| ("acquire", source))?;
    let mut conn = ClaimConn {
        conn: Some(conn),
        closing,
    };
    let lease = Row::lease(queue, now).map_err(claim_failed)?;
    let claimed = Row::claim(&mut conn, cx, Some(&lease), ids, rows).await;
    conn.claimed();
    claimed.map_err(claim_failed)?;
    Ok(lease.expiry)
}

/// The connection a claim's one statement runs on. Dropped before the statement ended, as a
/// subscription's stream drops its claim in flight at shutdown, it drains through the broker's
/// closes, so `shutdown` returns once the server ended the statement and released the rows it
/// locked; otherwise it goes back to the pool.
struct ClaimConn<DB: QueueDatabase> {
    conn: Option<PoolConnection<DB>>,
    closing: &'static Closing,
}

impl<DB: QueueDatabase> ClaimConn<DB> {
    /// The statement ended: the connection goes back to the pool.
    fn claimed(&mut self) {
        drop(self.conn.take());
    }
}

impl<DB: QueueDatabase> Deref for ClaimConn<DB> {
    type Target = DB::Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("a claim holds its connection until its statement ends")
    }
}

impl<DB: QueueDatabase> DerefMut for ClaimConn<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("a claim holds its connection until its statement ends")
    }
}

impl<DB: QueueDatabase> Drop for ClaimConn<DB> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.closing.drain(conn, false);
        }
    }
}

/// Opens a transactional delivery's own transaction on a connection of `pool`, at the table's
/// isolation level or SQLite mode, or with `BEGIN` where the table names neither.
pub(crate) async fn begin_work<DB: QueueDatabase>(
    pool: &Pool<DB>,
    queue: &Queue,
) -> Result<PoolTx<DB>, Failed> {
    let begin = queue.prepared.begin_work;
    PoolTx::begin(pool, begin)
        .await
        .map_err(|source| (begin.unwrap_or("BEGIN"), source))
}

/// Leases each of `rows` with `lease` inside the claim's transaction, and drops from the claim
/// each row another lease holds.
async fn stamp_rows<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    lease: &Leasing<Row::Token>,
    rows: &mut Vec<Claimed<Row>>,
) -> Result<(), Failed>
where
    DB: QueueDatabase,
    Row: Events<DB>,
{
    let mut index = 0;
    while let Some(claimed) = rows.get(index) {
        let stamped = engine::stamp::<DB, Row>(conn, cx, claimed.id::<DB>(), lease)
            .await
            .map_err(|source| {
                let statement = cx.queue.prepared.stamp.map_or("stamp", |stamp| stamp.sql);
                (statement, source)
            })?;
        if stamped {
            index += 1;
        } else {
            rows.remove(index);
        }
    }
    Ok(())
}
