//! The default settlements: the statement each outcome runs on a delivery's row, and the extension
//! of a lease.

use std::time::Duration;

use sqlx::Error;

use super::{Event, Events, Settled, Settling, Values, run};
use crate::inbox::database::QueueDatabase;
use crate::inbox::queue::Queue;

/// What a settlement statement of `queue` that changed `changed` rows did.
const fn settled(queue: &Queue, changed: u64) -> Settled {
    // Why the form decides: a statement names the row and its token only in the lease form, where
    // a row that changed nothing holds another lease; a row lock settlement commits whatever ran.
    if queue.lease.is_some() && changed == 0 {
        Settled::Lost
    } else {
        Settled::Written
    }
}

/// The default `ack`.
///
/// # Errors
///
/// The database's error.
pub async fn ack<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: Option<&Row::Token>,
) -> Result<Settled, Error> {
    let values = Values::settling(*cx, Event::Ack, id, held.copied());
    let changed = run::<DB, Row>(conn, cx.queue.prepared.ack, values).await?;
    Ok(settled(cx.queue, changed))
}

/// The default `retry()`.
///
/// # Errors
///
/// The database's error.
pub async fn retry<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: Option<&Row::Token>,
) -> Result<Settled, Error> {
    let Some(statement) = cx.queue.prepared.retry else {
        return Ok(Settled::Untouched);
    };
    let values = Values::settling(*cx, Event::Retry, id, held.copied());
    let changed = run::<DB, Row>(conn, Some(statement), values).await?;
    Ok(settled(cx.queue, changed))
}

/// The default `retry_after(d)`.
///
/// # Errors
///
/// The database's error.
pub async fn retry_after<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: Option<&Row::Token>,
    delay: Duration,
) -> Result<Settled, Error> {
    let values = Values {
        delay,
        ..Values::settling(*cx, Event::RetryAfter, id, held.copied())
    };
    let changed = run::<DB, Row>(conn, cx.queue.prepared.retry_after, values).await?;
    Ok(settled(cx.queue, changed))
}

/// The default `drop`.
///
/// # Errors
///
/// The database's error.
pub async fn discard<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: Option<&Row::Token>,
) -> Result<Settled, Error> {
    let values = Values::settling(*cx, Event::Discard, id, held.copied());
    let changed = run::<DB, Row>(conn, cx.queue.prepared.discard, values).await?;
    Ok(settled(cx.queue, changed))
}

/// The default dead-letter move: one statement, or two where the dialect splits the move, the
/// second run only once the first moved the row.
///
/// In the lease form every statement of the move must change the row: one that changes nothing
/// found the row under another lease, and the move is [`Settled::Lost`], which rolls back the
/// transaction both statements run in.
///
/// # Errors
///
/// The database's error.
pub async fn dead_letter<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: Option<&Row::Token>,
    destination: &str,
) -> Result<Settled, Error> {
    let values = Values {
        destination,
        ..Values::settling(*cx, Event::DeadLetter, id, held.copied())
    };
    let changed = run::<DB, Row>(conn, cx.queue.prepared.dead_letter, values).await?;
    let moved = settled(cx.queue, changed);
    let (Settled::Written, Some(then)) = (moved, cx.queue.prepared.dead_letter_then) else {
        return Ok(moved);
    };
    // Why the second count matters: the copy may read the row without locking it (it does at READ
    // COMMITTED), so another claim may take the row before the delete runs, and committing then
    // would leave the row in the queue and in the destination at once.
    let values = Values {
        destination,
        ..Values::settling(*cx, Event::DeadLetter, id, held.copied())
    };
    let changed = run::<DB, Row>(conn, Some(then), values).await?;
    Ok(settled(cx.queue, changed))
}

/// The default extension: writes `until` into the lease while the row holds `held`.
///
/// # Errors
///
/// The database's error.
pub async fn extend<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Settling,
    id: &Row::Id,
    held: &Row::Token,
    until: &Row::Token,
) -> Result<Settled, Error> {
    let values = Values {
        lease: Some(*until),
        ..Values::settling(*cx, Event::Extend, id, Some(*held))
    };
    let changed = run::<DB, Row>(conn, cx.queue.prepared.extend, values).await?;
    Ok(settled(cx.queue, changed))
}
