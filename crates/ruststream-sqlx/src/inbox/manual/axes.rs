//! What each setting of a table described by hand means to the crate: one trait per setting,
//! implemented for [`Unset`] and for the [`Set`] markers of that setting. The blanket contract
//! reads a setting through its trait, so every choice is a type the compiler resolves.

use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;

use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Param;
use sqlx::{Decode, Encode, Error, Type};

use super::{AttemptRow, HeaderFields, KeyRow};
use crate::HeaderColumn;
use crate::HeaderRow;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{self, Events, Leasing, Now, TimeFor, Values};
use crate::inbox::form::{AdvisoryForm, LeaseForm, RowLockForm};
use crate::inbox::headers::{self, HeaderCell, LazyHeaders};
use crate::inbox::named::kinds::KindsOf;
use crate::inbox::queue::Queue;
use crate::inbox::spec::{
    self, Advisory, Attempt, Clock, Headers, Key, Lease, OpeningLevel, Opens, Payload, ProcessedAt,
    RetryAfter, Set, Unset,
};
use crate::inbox::time::{QueueTime, SystemClock, TimeSource};
use crate::inbox::{Lane, PayloadLane, PayloadRow, RowLane};

/// Whether a table sets a setting.
pub trait Named {
    /// `true` where the table sets it.
    const NAMED: bool;
}

impl Named for Unset {
    const NAMED: bool = false;
}

impl<Value> Named for Set<Value> {
    const NAMED: bool = true;
}

/// The form setting: the form's type, the lease a delivery holds and how a claim takes it.
pub trait FormAxis {
    /// The form as the type a subscription checks against its dialect.
    type Form;
    /// The lease a delivery holds; `()` outside the lease form.
    type Token: Copy + Debug + Send + Sync + 'static;

    /// The lease a claim takes now.
    ///
    /// # Errors
    ///
    /// As `Events::lease`.
    fn lease<Source: TimeSource>(
        queue: &'static Queue,
        now: Now,
    ) -> Result<Leasing<Self::Token>, Error>;

    /// The lease's time among the column kinds a by-name subscription reads.
    fn kinds(kinds: KindsOf) -> KindsOf;
}

impl FormAxis for Unset {
    type Form = RowLockForm;
    type Token = ();

    fn lease<Source: TimeSource>(_: &'static Queue, _: Now) -> Result<Leasing<()>, Error> {
        engine::no_lease()
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

impl FormAxis for Set<Advisory> {
    type Form = AdvisoryForm;
    type Token = ();

    fn lease<Source: TimeSource>(_: &'static Queue, _: Now) -> Result<Leasing<()>, Error> {
        engine::no_lease()
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

impl<Time: QueueTime> FormAxis for Set<Lease<Time>> {
    type Form = LeaseForm;
    type Token = Time;

    fn lease<Source: TimeSource>(queue: &'static Queue, now: Now) -> Result<Leasing<Time>, Error> {
        engine::lease::<Source, Time>(queue, now)
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds.locked_until::<Time>()
    }
}

/// The form setting's statement parameters on `DB`.
pub trait FormBind<DB: QueueDatabase>: FormAxis {
    /// Binds a lease parameter; `false` where the form has none.
    ///
    /// # Errors
    ///
    /// The driver's encoding error.
    fn bind<Row: Events<DB, Token = Self::Token>>(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Row>,
    ) -> Result<bool, Error>;
}

macro_rules! no_lease_bind {
    ($($axis:ty),*) => {
        $(impl<DB: QueueDatabase> FormBind<DB> for $axis {
            fn bind<Row: Events<DB, Token = ()>>(
                _: Param,
                _: &mut DB::Arguments,
                _: &Values<'_, DB, Row>,
            ) -> Result<bool, Error> {
                Ok(false)
            }
        })*
    };
}

no_lease_bind!(Unset, Set<Advisory>);

impl<DB, Time> FormBind<DB> for Set<Lease<Time>>
where
    DB: QueueDatabase,
    Time: QueueTime + for<'q> Encode<'q, DB> + Type<DB>,
{
    fn bind<Row: Events<DB, Token = Time>>(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Row>,
    ) -> Result<bool, Error> {
        let value = match param {
            Param::LeaseNow => values.leasing.map(|leasing| leasing.now),
            Param::Lease => values.lease,
            Param::Held => values.held,
            _ => return Ok(false),
        };
        let Some(value) = value else {
            return Ok(false);
        };
        engine::put::<DB, _>(arguments, value)?;
        Ok(true)
    }
}

/// The lease form's time, for `LeaseRow`.
pub trait LeaseAxis {
    /// The time `locked_until` holds.
    type Time: QueueTime;
}

impl<Time: QueueTime> LeaseAxis for Set<Lease<Time>> {
    type Time = Time;
}

/// The message mode setting.
pub trait MessageAxis<Row> {
    /// The lane a delivery hands the message on.
    type Lane: Lane<Row>;

    /// The payload's column among the column kinds a by-name subscription reads.
    fn kinds(kinds: KindsOf) -> KindsOf;
}

impl<Row> MessageAxis<Row> for Unset {
    type Lane = RowLane;

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

impl<Row: PayloadRow> MessageAxis<Row> for Set<Payload> {
    type Lane = PayloadLane;

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds.payload::<Row::Column>()
    }
}

/// The `partition_key` setting.
pub trait KeyAxis<Row> {
    /// The row's key.
    fn key(row: &Row) -> Option<&[u8]>;

    /// The key's column among the column kinds a by-name subscription reads.
    fn kinds(kinds: KindsOf) -> KindsOf;
}

impl<Row> KeyAxis<Row> for Unset {
    fn key(_: &Row) -> Option<&[u8]> {
        None
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

impl<Row: KeyRow> KeyAxis<Row> for Set<Key> {
    fn key(row: &Row) -> Option<&[u8]> {
        crate::KeyColumn::key(row.partition_key())
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds.partition_key::<Row::Key>()
    }
}

/// The `attempt` setting on `DB`.
pub trait AttemptAxis<DB: QueueDatabase, Row> {
    /// The row's attempt.
    fn attempt(row: &Row) -> Option<u64>;

    /// The attempt of a row that did not decode, read alone from `column`.
    fn read(row: &DB::Row, column: Option<&str>) -> Option<u64>;

    /// The attempt's column among the column kinds a by-name subscription reads.
    fn kinds(kinds: KindsOf) -> KindsOf;
}

impl<DB: QueueDatabase, Row> AttemptAxis<DB, Row> for Unset {
    fn attempt(_: &Row) -> Option<u64> {
        None
    }

    fn read(_: &DB::Row, _: Option<&str>) -> Option<u64> {
        None
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

impl<DB, Row> AttemptAxis<DB, Row> for Set<Attempt>
where
    DB: QueueDatabase,
    Row: AttemptRow,
    Row::Attempt: for<'r> Decode<'r, DB> + Type<DB>,
{
    fn attempt(row: &Row) -> Option<u64> {
        Some(crate::AttemptColumn::attempt(row.attempt()))
    }

    fn read(row: &DB::Row, column: Option<&str>) -> Option<u64> {
        engine::attempt_in::<DB, Row::Attempt, Row::Attempt>(row, column?)
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds.attempt::<Row::Attempt>()
    }
}

/// The headers setting.
pub trait HeadersAxis<Row> {
    /// Where a delivery keeps its header map.
    type Cell: Default + Send + Sync + 'static;

    /// The header map moved out of the row.
    fn take(row: &mut Row) -> HeaderMap;

    /// The first header the row cannot hold.
    fn unfit(headers: &HeaderMap) -> Option<&str>;

    /// The cell a delivery keeps, as `HeaderCell::take`.
    fn cell<DB: QueueDatabase>(row: &mut Row) -> Self::Cell
    where
        Row: Events<DB>;

    /// As `HeaderCell::is_unset`.
    fn is_unset<DB: QueueDatabase>(cell: &Self::Cell) -> bool
    where
        Row: Events<DB>;

    /// As `HeaderCell::read`.
    fn read<'c, DB: QueueDatabase>(cell: &'c Self::Cell, row: Option<&Row>) -> &'c HeaderMap
    where
        Row: Events<DB>;

    /// The header column among the column kinds a by-name subscription reads.
    fn kinds(kinds: KindsOf) -> KindsOf;
}

/// The header cell of a table described by hand, with the layout of the cell its headers setting
/// names.
///
/// The blanket contract names this type instead of the setting's cell, which would restate the
/// row's own bounds and make the trait solver loop.
pub struct ManualHeaders<Axis: HeadersAxis<Row>, Row>(Axis::Cell, PhantomData<fn() -> Row>);

impl<Axis: HeadersAxis<Row>, Row> Debug for ManualHeaders<Axis, Row> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("ManualHeaders")
    }
}

impl<Axis: HeadersAxis<Row>, Row> Default for ManualHeaders<Axis, Row> {
    fn default() -> Self {
        Self(Axis::Cell::default(), PhantomData)
    }
}

impl<DB, Row, Axis> HeaderCell<DB, Row> for ManualHeaders<Axis, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + 'static,
    Axis: HeadersAxis<Row> + 'static,
{
    fn take(row: &mut Row) -> Self {
        Self(Axis::cell::<DB>(row), PhantomData)
    }

    fn is_unset(&self) -> bool {
        Axis::is_unset::<DB>(&self.0)
    }

    fn read<'c>(&'c self, row: Option<&Row>) -> &'c HeaderMap {
        Axis::read::<DB>(&self.0, row)
    }
}

/// The cell of a table whose header map the row hands over: the map itself.
macro_rules! map_cell {
    () => {
        fn cell<DB: QueueDatabase>(row: &mut Row) -> HeaderMap
        where
            Row: Events<DB>,
        {
            <HeaderMap as HeaderCell<DB, Row>>::take(row)
        }

        fn is_unset<DB: QueueDatabase>(cell: &HeaderMap) -> bool
        where
            Row: Events<DB>,
        {
            <HeaderMap as HeaderCell<DB, Row>>::is_unset(cell)
        }

        fn read<'c, DB: QueueDatabase>(cell: &'c HeaderMap, row: Option<&Row>) -> &'c HeaderMap
        where
            Row: Events<DB>,
        {
            <HeaderMap as HeaderCell<DB, Row>>::read(cell, row)
        }
    };
}

impl<Row> HeadersAxis<Row> for Unset {
    type Cell = HeaderMap;

    fn take(_: &mut Row) -> HeaderMap {
        HeaderMap::new()
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        engine::first_header(headers)
    }

    map_cell!();

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

impl<Row: HeaderRow> HeadersAxis<Row> for Set<Headers> {
    type Cell = HeaderMap;

    fn take(row: &mut Row) -> HeaderMap {
        row.headers_mut().take_headers()
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        <Row::Column as HeaderColumn>::unfit(headers)
    }

    map_cell!();

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds.headers::<Row::Column>()
    }
}

impl<Row: HeaderFields> HeadersAxis<Row> for Set<spec::HeaderFields> {
    type Cell = LazyHeaders;

    fn take(_: &mut Row) -> HeaderMap {
        HeaderMap::new()
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        headers::unnamed_header(headers, Row::NAMES)
    }

    fn cell<DB: QueueDatabase>(row: &mut Row) -> LazyHeaders
    where
        Row: Events<DB>,
    {
        <LazyHeaders as HeaderCell<DB, Row>>::take(row)
    }

    fn is_unset<DB: QueueDatabase>(cell: &LazyHeaders) -> bool
    where
        Row: Events<DB>,
    {
        <LazyHeaders as HeaderCell<DB, Row>>::is_unset(cell)
    }

    fn read<'c, DB: QueueDatabase>(cell: &'c LazyHeaders, row: Option<&Row>) -> &'c HeaderMap
    where
        Row: Events<DB>,
    {
        <LazyHeaders as HeaderCell<DB, Row>>::read(cell, row)
    }

    fn kinds(kinds: KindsOf) -> KindsOf {
        kinds
    }
}

/// A time role on `DB`: `retry_after` or `processed_at`.
pub trait TimeAxis<DB: QueueDatabase> {
    /// Binds now; `false` without the column.
    ///
    /// # Errors
    ///
    /// The driver's encoding error, or the clock's.
    fn now<Source: TimeSource, Row: Events<DB>>(
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Row>,
    ) -> Result<bool, Error>;

    /// Binds now plus the delay; `false` without the column.
    ///
    /// # Errors
    ///
    /// As [`now`](Self::now).
    fn later<Source: TimeSource, Row: Events<DB>>(
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Row>,
    ) -> Result<bool, Error>;

    /// The column among the column kinds a by-name subscription reads, as `retry_after` when
    /// `due` and as `processed_at` otherwise.
    fn kinds(kinds: KindsOf, due: bool) -> KindsOf;
}

impl<DB: QueueDatabase> TimeAxis<DB> for Unset {
    fn now<Source: TimeSource, Row: Events<DB>>(
        _: &mut DB::Arguments,
        _: &Values<'_, DB, Row>,
    ) -> Result<bool, Error> {
        Ok(false)
    }

    fn later<Source: TimeSource, Row: Events<DB>>(
        _: &mut DB::Arguments,
        _: &Values<'_, DB, Row>,
    ) -> Result<bool, Error> {
        Ok(false)
    }

    fn kinds(kinds: KindsOf, _: bool) -> KindsOf {
        kinds
    }
}

macro_rules! time_axis {
    ($($marker:ident),*) => {$(
        impl<DB, Time> TimeAxis<DB> for Set<$marker<Time>>
        where
            DB: QueueDatabase,
            Time: TimeFor<DB>,
        {
            fn now<Source: TimeSource, Row: Events<DB>>(
                arguments: &mut DB::Arguments,
                values: &Values<'_, DB, Row>,
            ) -> Result<bool, Error> {
                let now = engine::now::<Source, Time::Time, DB, Row>(values)?;
                engine::put::<DB, _>(arguments, now)?;
                Ok(true)
            }

            fn later<Source: TimeSource, Row: Events<DB>>(
                arguments: &mut DB::Arguments,
                values: &Values<'_, DB, Row>,
            ) -> Result<bool, Error> {
                let later = engine::later::<Source, Time::Time, DB, Row>(values)?;
                engine::put::<DB, _>(arguments, later)?;
                Ok(true)
            }

            fn kinds(kinds: KindsOf, due: bool) -> KindsOf {
                if due {
                    kinds.retry_after::<Time::Time>()
                } else {
                    kinds.processed_at::<Time::Time>()
                }
            }
        }
    )*};
}

time_axis!(RetryAfter, ProcessedAt);

/// The clock setting.
pub trait ClockAxis {
    /// Where the table reads now.
    type Source: TimeSource;
}

impl ClockAxis for Unset {
    type Source = SystemClock;
}

impl<Source: TimeSource> ClockAxis for Set<Clock<Source>> {
    type Source = Source;
}

/// The opening setting.
pub trait OpeningAxis {
    /// The opening as the type a subscription checks against its dialect.
    type Opening;
}

impl OpeningAxis for Unset {
    type Opening = ();
}

impl<Level: OpeningLevel> OpeningAxis for Set<Opens<Level>> {
    type Opening = Level;
}
