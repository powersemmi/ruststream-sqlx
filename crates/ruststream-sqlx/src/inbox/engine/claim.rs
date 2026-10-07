//! The default claims: whole rows or ids in one statement, the crate's fetch of claimed ids, the
//! pairing of ids with fetched rows, the guard a FIFO claim takes its group with, and the stamp of
//! a row a lease claim only selected.

use std::convert::identity;

use sqlx::{Decode, Error, Type};

use super::{Claimed, Claiming, Event, Events, Leasing, Stmt, Values, arguments, run, unprepared};
use crate::inbox::QueueRow;
use crate::inbox::database::QueueDatabase;

/// The default claim: whole rows, in one statement.
///
/// # Errors
///
/// The database's error.
pub async fn claim_rows<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    lease: Option<&Leasing<Row::Token>>,
    out: &mut Vec<Claimed<Row>>,
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
    let arguments = arguments::<DB, Row>(statement, &Values::claiming(*cx, Event::Claim, lease))?;
    DB::fetch_rows(conn, statement.sql, arguments, cx.queue, out).await
}

/// The default claim of ids, for a fetch of the service's own, into `ids`.
///
/// # Errors
///
/// The database's error.
pub async fn claim_ids<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    lease: Option<&Leasing<Row::Token>>,
    ids: &mut Vec<Row::Id>,
) -> Result<(), Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB> + Unpin,
{
    let statement = cx
        .queue
        .prepared
        .claim
        .ok_or_else(|| unprepared(Event::Claim))?;
    let arguments = arguments::<DB, Row>(statement, &Values::claiming(*cx, Event::Claim, lease))?;
    DB::fetch_ids(conn, statement.sql, arguments, ids).await
}

/// The default fetch of the rows of `ids`, after a claim of the service's own.
///
/// Each row comes as the claim reads it: whole, or [`Claimed::Undecodable`].
///
/// # Errors
///
/// The database's error, or the decode error of a row whose id does not decode either.
pub async fn fetch_by_ids<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    lease: Option<&Leasing<Row::Token>>,
    ids: &[Row::Id],
) -> Result<Vec<Claimed<Row>>, Error>
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB>,
{
    let statement = cx
        .queue
        .prepared
        .fetch
        .ok_or_else(|| unprepared(Event::Fetch))?;
    let values = Values {
        ids,
        ..Values::claiming(*cx, Event::Fetch, lease)
    };
    let arguments = arguments::<DB, Row>(statement, &values)?;
    let mut fetched = Vec::with_capacity(ids.len());
    DB::fetch_rows(conn, statement.sql, arguments, cx.queue, &mut fetched).await?;
    Ok(fetched)
}

/// Pairs claimed ids with what the crate's fetch returned, in claim order, and empties `ids`.
///
/// A row or an [`Claimed::Undecodable`] entry goes with its id, an id with neither is
/// [`Claimed::Missing`], and an entry no id claimed is left alone.
pub fn match_claimed<DB, Row>(
    ids: &mut Vec<Row::Id>,
    fetched: Vec<Claimed<Row>>,
    out: &mut Vec<Claimed<Row>>,
) where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: PartialEq,
{
    pair(ids, fetched, Claimed::id::<DB>, identity, out);
}

/// Pairs claimed ids with the rows a fetch of the service's own returned, as [`match_claimed`]
/// does.
pub fn match_rows<DB, Row>(ids: &mut Vec<Row::Id>, rows: Vec<Row>, out: &mut Vec<Claimed<Row>>)
where
    DB: QueueDatabase,
    Row: Events<DB>,
    Row::Id: PartialEq,
{
    pair(ids, rows, Row::id, Claimed::Row, out);
}

/// Pairs each of `ids` with the entry `id_of` names it in, turned into its row by `claimed`.
fn pair<Row, Entry>(
    ids: &mut Vec<Row::Id>,
    mut fetched: Vec<Entry>,
    id_of: impl Fn(&Entry) -> &Row::Id,
    claimed: impl Fn(Entry) -> Claimed<Row>,
    out: &mut Vec<Claimed<Row>>,
) where
    Row: QueueRow,
    Row::Id: PartialEq,
{
    // Drained, not consumed: the buffer goes back to its subscription for the next claim.
    for id in ids.drain(..) {
        match fetched.iter().position(|entry| id_of(entry) == &id) {
            Some(position) => out.push(claimed(fetched.swap_remove(position))),
            None => out.push(Claimed::Missing(id)),
        }
    }
}

/// Takes the subscription's group for the claim's transaction with `guard`, the queue's guard;
/// `false` when another transaction holds the group, and the claim then takes nothing.
///
/// The guard binds as the claim binds, with the claim's `lease`, so the guard of a dialect of the
/// service's own may compare the times the claim compares.
///
/// # Errors
///
/// The database's error.
pub(crate) async fn take_group<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    guard: Stmt,
    lease: Option<&Leasing<Row::Token>>,
) -> Result<bool, Error> {
    let arguments = arguments::<DB, Row>(guard, &Values::claiming(*cx, Event::Claim, lease))?;
    DB::fetch_flag(conn, guard.sql, arguments).await
}

/// Leases the claimed row `id` with `lease`, inside the claim's transaction; `false` when another
/// lease holds the row, which the claim then passes over.
///
/// # Errors
///
/// The database's error.
pub(crate) async fn stamp<DB: QueueDatabase, Row: Events<DB>>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    id: &Row::Id,
    lease: &Leasing<Row::Token>,
) -> Result<bool, Error> {
    let values = Values {
        id: Some(id),
        ..Values::claiming(*cx, Event::Stamp, Some(lease))
    };
    Ok(run::<DB, Row>(conn, cx.queue.prepared.stamp, values).await? > 0)
}
