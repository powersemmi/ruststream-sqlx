//! Which code runs each event of a table described by hand: the crate's default, or the
//! service's own where its description sets the event with `own`. Each choice is a type, so the
//! call is static.

use std::future::Future;
use std::slice;
use std::time::Duration;

use sqlx::{Decode, Encode, Error, Type};

use crate::inbox::QueueRow;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{self, Claimed, Claiming, Events, Leasing, Settled, Settling, Values};
use crate::inbox::events::{
    Ack, Claim, DeadLetter, Discard, Extend, Fetch, Lock, Retry, RetryAfter, Unlock,
};
use crate::inbox::form::advisory::events as advisory;
use crate::inbox::spec::{Set, Unset, own};
use crate::inbox::time::LeaseRow;

/// The claim, the fetch behind it and the take of an advisory candidate, chosen by the pair
/// (own claim, own fetch).
pub trait ClaimAxes<DB: QueueDatabase, Row: QueueRow> {
    /// What a claim keeps of its ids until its fetch.
    type Ids: Default + Send + Sync + 'static;

    /// Claims rows into `out`.
    fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<<Row as Events<DB>>::Token>>,
        ids: &'a mut Self::Ids,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a
    where
        Row: Events<DB>;

    /// Takes an advisory candidate.
    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a Row::Id,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>;

    /// Binds the ids of the crate's fetch after the service's claim.
    ///
    /// # Errors
    ///
    /// The driver's encoding error.
    fn bind_ids(arguments: &mut DB::Arguments, values: &Values<'_, DB, Row>) -> Result<bool, Error>
    where
        Row: Events<DB>;
}

impl<DB, Row> ClaimAxes<DB, Row> for (Unset, Unset)
where
    DB: QueueDatabase,
    Row: QueueRow,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB>,
{
    type Ids = ();

    fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<<Row as Events<DB>>::Token>>,
        (): &'a mut (),
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        engine::claim_rows::<DB, Row>(conn, cx, lease, out)
    }

    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a Row::Id,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        advisory::take::<DB, Row>(conn, cx, id, out)
    }

    fn bind_ids(_: &mut DB::Arguments, _: &Values<'_, DB, Row>) -> Result<bool, Error>
    where
        Row: Events<DB>,
    {
        Ok(false)
    }
}

impl<DB, Row> ClaimAxes<DB, Row> for (Set<own::Claim>, Unset)
where
    DB: QueueDatabase,
    Row: Claim<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB> + PartialEq,
    for<'q, 'x> &'x [Row::Id]: Encode<'q, DB> + Type<DB>,
{
    type Ids = ();

    async fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<<Row as Events<DB>>::Token>>,
        (): &'a mut (),
        out: &'a mut Vec<Claimed<Row>>,
    ) -> Result<(), Error>
    where
        Row: Events<DB>,
    {
        // The service's claim returns a vector of its own, which the crate's fetch reads.
        let mut ids = <Row as Claim<DB>>::claim(&mut *conn, cx.queue.name, cx.limit).await?;
        if ids.is_empty() {
            return Ok(());
        }
        let fetched = engine::fetch_by_ids::<DB, Row>(&mut *conn, cx, lease, &ids).await?;
        engine::match_claimed::<DB, Row>(&mut ids, fetched, out);
        Ok(())
    }

    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a Row::Id,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        advisory::take::<DB, Row>(conn, cx, id, out)
    }

    fn bind_ids(arguments: &mut DB::Arguments, values: &Values<'_, DB, Row>) -> Result<bool, Error>
    where
        Row: Events<DB>,
    {
        engine::put::<DB, _>(arguments, values.ids)?;
        Ok(true)
    }
}

impl<DB, Row> ClaimAxes<DB, Row> for (Unset, Set<own::Fetch>)
where
    DB: QueueDatabase,
    Row: Fetch<DB>,
    Row::Id: for<'r> Decode<'r, DB> + Type<DB> + PartialEq + Unpin,
{
    type Ids = Vec<Row::Id>;

    async fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<<Row as Events<DB>>::Token>>,
        ids: &'a mut Vec<Row::Id>,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> Result<(), Error>
    where
        Row: Events<DB>,
    {
        ids.clear();
        engine::claim_ids::<DB, Row>(&mut *conn, cx, lease, ids).await?;
        if ids.is_empty() {
            return Ok(());
        }
        let rows = <Row as Fetch<DB>>::fetch(&mut *conn, ids).await?;
        engine::match_rows::<DB, Row>(ids, rows, out);
        Ok(())
    }

    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a Row::Id,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        own_take::<DB, Row>(conn, cx, id, out)
    }

    fn bind_ids(_: &mut DB::Arguments, _: &Values<'_, DB, Row>) -> Result<bool, Error>
    where
        Row: Events<DB>,
    {
        Ok(false)
    }
}

impl<DB, Row> ClaimAxes<DB, Row> for (Set<own::Claim>, Set<own::Fetch>)
where
    DB: QueueDatabase,
    Row: Claim<DB> + Fetch<DB>,
    Row::Id: PartialEq,
{
    type Ids = ();

    async fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        _: Option<&'a Leasing<<Row as Events<DB>>::Token>>,
        (): &'a mut (),
        out: &'a mut Vec<Claimed<Row>>,
    ) -> Result<(), Error>
    where
        Row: Events<DB>,
    {
        // A claim and a fetch of the service's own bind no lease: the crate stamps the rows.
        let mut ids = <Row as Claim<DB>>::claim(&mut *conn, cx.queue.name, cx.limit).await?;
        if ids.is_empty() {
            return Ok(());
        }
        let rows = <Row as Fetch<DB>>::fetch(&mut *conn, &ids).await?;
        engine::match_rows::<DB, Row>(&mut ids, rows, out);
        Ok(())
    }

    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a Row::Id,
        out: &'a mut Vec<Claimed<Row>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        own_take::<DB, Row>(conn, cx, id, out)
    }

    fn bind_ids(_: &mut DB::Arguments, _: &Values<'_, DB, Row>) -> Result<bool, Error>
    where
        Row: Events<DB>,
    {
        Ok(false)
    }
}

/// The take of an advisory candidate for a fetch of the service's own.
async fn own_take<DB, Row>(
    conn: &mut DB::Connection,
    cx: &Claiming,
    id: &Row::Id,
    out: &mut Vec<Claimed<Row>>,
) -> Result<bool, Error>
where
    DB: QueueDatabase,
    Row: Events<DB> + Fetch<DB>,
    Row::Id: PartialEq,
{
    if !advisory::take_id::<DB, Row>(&mut *conn, cx, id).await? {
        return Ok(false);
    }
    let rows = <Row as Fetch<DB>>::fetch(&mut *conn, slice::from_ref(id)).await?;
    advisory::match_taken::<DB, Row>(id, rows, out);
    Ok(true)
}

/// One settlement event's choice: the crate's default or the service's own, which settles without
/// the lease after the crate confirmed it.
macro_rules! settlement {
    ($axis:ident, $trait:ident, $method:ident $(, $extra:ident: $extra_ty:ty)?) => {
        #[doc = concat!("Who runs `", stringify!($method), "`.")]
        pub trait $axis<DB: QueueDatabase, Row: QueueRow> {
            #[doc = concat!("Runs `", stringify!($method), "`.")]
            fn $method<'a>(
                conn: &'a mut DB::Connection,
                cx: &'a Settling,
                id: &'a Row::Id,
                held: Option<&'a <Row as Events<DB>>::Token>,
                $($extra: $extra_ty,)?
            ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a
            where
                Row: Events<DB>;
        }

        impl<DB: QueueDatabase, Row: QueueRow> $axis<DB, Row> for Unset {
            fn $method<'a>(
                conn: &'a mut DB::Connection,
                cx: &'a Settling,
                id: &'a Row::Id,
                held: Option<&'a <Row as Events<DB>>::Token>,
                $($extra: $extra_ty,)?
            ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a
            where
                Row: Events<DB>,
            {
                engine::$method::<DB, Row>(conn, cx, id, held $(, $extra)?)
            }
        }

        impl<DB: QueueDatabase, Row: $trait<DB>> $axis<DB, Row> for Set<own::$trait> {
            fn $method<'a>(
                conn: &'a mut DB::Connection,
                _: &'a Settling,
                id: &'a Row::Id,
                _: Option<&'a <Row as Events<DB>>::Token>,
                $($extra: $extra_ty,)?
            ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a
            where
                Row: Events<DB>,
            {
                async move {
                    <Row as $trait<DB>>::$method(conn, id $(, $extra)?).await?;
                    Ok(Settled::Written)
                }
            }
        }
    };
}

settlement!(AckAxis, Ack, ack);
settlement!(RetryAxis, Retry, retry);
settlement!(RetryAfterAxis, RetryAfter, retry_after, delay: Duration);
settlement!(DiscardAxis, Discard, discard);
settlement!(DeadLetterAxis, DeadLetter, dead_letter, destination: &'a str);

/// Who runs `extend`, for a table whose lease is held in `Token`.
pub trait ExtendAxis<DB: QueueDatabase, Row: QueueRow, Token: Copy + Send + Sync + 'static> {
    /// Runs `extend`.
    fn extend<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Row::Id,
        held: &'a Token,
        until: &'a Token,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a
    where
        Row: Events<DB, Token = Token>;
}

impl<DB: QueueDatabase, Row: QueueRow, Token: Copy + Send + Sync + 'static>
    ExtendAxis<DB, Row, Token> for Unset
{
    fn extend<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a Row::Id,
        held: &'a Token,
        until: &'a Token,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a
    where
        Row: Events<DB, Token = Token>,
    {
        engine::extend::<DB, Row>(conn, cx, id, held, until)
    }
}

impl<DB, Row> ExtendAxis<DB, Row, <Row as LeaseRow>::Lease> for Set<own::Extend>
where
    DB: QueueDatabase,
    Row: Extend<DB>,
{
    async fn extend<'a>(
        conn: &'a mut DB::Connection,
        _: &'a Settling,
        id: &'a Row::Id,
        held: &'a Row::Lease,
        until: &'a Row::Lease,
    ) -> Result<Settled, Error>
    where
        Row: Events<DB, Token = Row::Lease>,
    {
        let extended = <Row as Extend<DB>>::extend(conn, id, held, until).await?;
        Ok(if extended {
            Settled::Written
        } else {
            Settled::Lost
        })
    }
}

/// Who runs `lock`.
pub trait LockAxis<DB: QueueDatabase, Row: QueueRow> {
    /// Runs `lock`.
    fn lock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>;
}

impl<DB: QueueDatabase, Row: QueueRow> LockAxis<DB, Row> for Unset {
    fn lock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        advisory::lock::<DB, Row>(conn, cx, key)
    }
}

impl<DB: QueueDatabase, Row: Lock<DB>> LockAxis<DB, Row> for Set<own::Lock> {
    fn lock<'a>(
        conn: &'a mut DB::Connection,
        _: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        <Row as Lock<DB>>::lock(conn, key)
    }
}

/// Who runs `unlock`.
pub trait UnlockAxis<DB: QueueDatabase, Row: QueueRow> {
    /// Runs `unlock`.
    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>;
}

impl<DB: QueueDatabase, Row: QueueRow> UnlockAxis<DB, Row> for Unset {
    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        advisory::unlock::<DB, Row>(conn, cx, key)
    }
}

impl<DB: QueueDatabase, Row: Unlock<DB>> UnlockAxis<DB, Row> for Set<own::Unlock> {
    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        _: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a
    where
        Row: Events<DB>,
    {
        <Row as Unlock<DB>>::unlock(conn, key)
    }
}
