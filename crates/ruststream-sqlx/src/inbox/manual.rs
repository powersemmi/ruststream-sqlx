//! A queue table described by hand: the roles a delivery reads off a row, one small trait each,
//! and the row's whole contract with the crate, implemented once for every
//! [`InboxTable`](crate::InboxTable) and generic over the database.
//!
//! The contract reads the table's settings from the type of its description: each setting is a
//! type, and each trait of [`axes`] and [`dispatch`] picks the code for it, so the derived table
//! and the table described by hand call the same helpers with the same type arguments.

mod axes;
mod check;
mod dispatch;

use std::future::Future;
use std::time::Duration;

use ruststream::HeaderMap;
use ruststream_sqlx_dialect::{Param, Role, TableSpec};
use sqlx::{Decode, Encode, Error, FromRow, Type};

pub use axes::{
    AttemptAxis, ClockAxis, FormAxis, FormBind, HeadersAxis, KeyAxis, LeaseAxis, ManualHeaders,
    MessageAxis, Named, OpeningAxis, TimeAxis,
};
pub use dispatch::{
    AckAxis, ClaimAxes, DeadLetterAxis, DiscardAxis, ExtendAxis, LockAxis, RetryAfterAxis,
    RetryAxis, UnlockAxis,
};

use super::columns::{AttemptColumn, KeyColumn};
use super::database::QueueDatabase;
use super::engine::{
    self, Claimed, Claiming, Event, Events, Leasing, Now, Settled, Settling, Shape, Values,
};
use super::form::advisory::events::{self as advisory, Candidates};
use super::headers::{Assembled, HeaderField};
use super::named::kinds::{Kinds, KindsOf};
use super::queue::Queue;
use super::spec::Declaration;
use super::time::LeaseRow;
use super::{InboxRow, InboxSpec, InboxTable, QueueRow};

/// The row's partition key, which a table described by hand reads off its row where its
/// description sets [`InboxSpec::partition_key`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "sqlite", feature = "chrono", feature = "json"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream::runtime::{Input, SoloCarried};
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::spec::{Key, Lease};
/// use ruststream_sqlx::{InboxSpec, InboxTable, KeyRow};
/// use sqlx::{Sqlite, SqlitePool};
///
/// #[derive(Debug, Clone, sqlx::FromRow)]
/// pub struct Invoice {
///     id: i64,
///     customer: String,
///     total: i64,
/// }
///
/// impl InboxTable for Invoice {
///     type Id = i64;
///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Key)>;
///     const TABLE: Self::Table = InboxSpec::new("invoices", Column::new("id").generated())
///         .lease(Column::new("locked_until"))
///         .partition_key(Column::new("customer"))
///         .data(&[Column::new("total")]);
///
///     fn id(&self) -> &i64 {
///         &self.id
///     }
/// }
///
/// // Row mode: the handler takes the row itself.
/// impl Input for Invoice {
///     type Axis = SoloCarried<Self>;
/// }
///
/// impl KeyRow for Invoice {
///     type Key = String;
///
///     fn partition_key(&self) -> &String {
///         &self.customer
///     }
/// }
///
/// // One customer's invoices settle in order, each customer on a lane of its own.
/// #[subscriber(InboxQueue::<Invoice>::new("invoices"))]
/// async fn bill(invoice: &Invoice) -> HandlerOutcome {
///     if invoice.total >= 0 {
///         HandlerOutcome::ack()
///     } else {
///         HandlerOutcome::drop()
///     }
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("billing", "1.0.0")).with_broker(
///         SqlxBroker::<Sqlite>::new(pool),
///         |b| {
///             b.include(bill);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` sets a `partition_key` column and does not read it off its row",
    label = "no `KeyRow` for this row",
    note = "implement `KeyRow` for `{Self}`, or drop `.partition_key(..)` and `Key` from its \
            description"
)]
pub trait KeyRow {
    /// The key column's type: text or bytes, optional or not.
    type Key: KeyColumn + 'static;

    /// The key field.
    fn partition_key(&self) -> &Self::Key;
}

/// The row's attempt, which a table described by hand reads off its row where its description
/// sets [`InboxSpec::attempt`].
///
/// The column's type is part of the trait, so a row that does not decode still reports its
/// attempt, read alone.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "sqlite", feature = "chrono", feature = "json"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::spec::{Attempt, Lease, Payload};
/// use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable, PayloadRow};
/// use serde::Deserialize;
/// use sqlx::{Sqlite, SqlitePool};
///
/// #[derive(sqlx::FromRow)]
/// pub struct EmailJob {
///     job_id: i64,
///     attempt: i16,
///     payload: Vec<u8>,
/// }
///
/// impl InboxTable for EmailJob {
///     type Id = i64;
///     type Table = InboxSpec<(Lease<DateTime<Utc>>, Attempt, Payload)>;
///     const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
///         .lease(Column::new("locked_until"))
///         .group(Column::new("name"))
///         .attempt(Column::new("attempt").generated())
///         .payload(Column::new("payload"));
///
///     fn id(&self) -> &i64 {
///         &self.job_id
///     }
/// }
///
/// impl PayloadRow for EmailJob {
///     type Column = Vec<u8>;
///
///     fn payload(&self) -> &[u8] {
///         &self.payload
///     }
/// }
///
/// impl AttemptRow for EmailJob {
///     type Attempt = i16;
///
///     fn attempt(&self) -> &i16 {
///         &self.attempt
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// // The third attempt gives up on an address the first two could not reach.
/// #[subscriber(InboxQueue::<EmailJob>::new("emails"))]
/// async fn send(email: &Email, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
///     match attempt {
///         Some(3..) => HandlerOutcome::drop(),
///         _ if email.to.contains('@') => HandlerOutcome::ack(),
///         _ => HandlerOutcome::retry(),
///     }
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "1.0.0")).with_broker(
///         SqlxBroker::<Sqlite>::new(pool),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` sets an `attempt` column and does not read it off its row",
    label = "no `AttemptRow` for this row",
    note = "implement `AttemptRow` for `{Self}`, or drop `.attempt(..)` and `Attempt` from its \
            description"
)]
pub trait AttemptRow {
    /// The attempt column's type: a signed integer.
    type Attempt: AttemptColumn + 'static;

    /// The attempt field.
    fn attempt(&self) -> &Self::Attempt;
}

/// Header fields of the row: where its description sets [`InboxSpec::header_fields`], the
/// delivery builds its header map from them on its first read.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "sqlite", feature = "chrono", feature = "json"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream::HeaderMap;
/// use ruststream::runtime::{Input, SoloCarried};
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::spec::{self, Lease};
/// use ruststream_sqlx::{HeaderFields, InboxSpec, InboxTable, put_header};
/// use sqlx::{Sqlite, SqlitePool};
///
/// #[derive(Debug, Clone, sqlx::FromRow)]
/// pub struct OrderJob {
///     job_id: i64,
///     tenant: String,
///     trace: Option<String>,
///     note: Option<String>,
/// }
///
/// impl InboxTable for OrderJob {
///     type Id = i64;
///     type Table = InboxSpec<(Lease<DateTime<Utc>>, spec::HeaderFields)>;
///     const TABLE: Self::Table = InboxSpec::new("order_jobs", Column::new("job_id").generated())
///         .lease(Column::new("locked_until"))
///         .group(Column::new("name"))
///         .data(&[Column::new("tenant"), Column::new("trace"), Column::new("note")])
///         .header_fields();
///
///     fn id(&self) -> &i64 {
///         &self.job_id
///     }
/// }
///
/// impl Input for OrderJob {
///     type Axis = SoloCarried<Self>;
/// }
///
/// impl HeaderFields for OrderJob {
///     const NAMES: &'static [&'static str] = &["tenant", "trace"];
///
///     fn header_map(&self) -> HeaderMap {
///         let mut headers = HeaderMap::with_capacity(Self::NAMES.len());
///         put_header(&mut headers, "tenant", &self.tenant);
///         put_header(&mut headers, "trace", &self.trace);
///         headers
///     }
/// }
///
/// // The tenant's header reads like any other; the row holds the note.
/// #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
/// async fn ship(job: &OrderJob, ctx: &mut Context<'_>) -> HandlerOutcome {
///     match (ctx.headers().get_str("tenant"), job.note.as_deref()) {
///         (Some(_), Some("fragile")) => HandlerOutcome::retry(),
///         _ => HandlerOutcome::ack(),
///     }
/// }
///
/// pub fn app(pool: SqlitePool) -> RustStream {
///     RustStream::new(AppInfo::new("shop", "1.0.0")).with_broker(
///         SqlxBroker::<Sqlite>::new(pool),
///         |b| {
///             b.include(ship);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` builds its headers from fields and does not say which",
    label = "no `HeaderFields` for this row",
    note = "implement `HeaderFields` for `{Self}`, or drop `.header_fields()` and \
            `spec::HeaderFields` from its description"
)]
pub trait HeaderFields {
    /// The headers the fields hold, by name; a publish that carries another is refused.
    const NAMES: &'static [&'static str];

    /// The header map: each field under its name, through [`put_header`].
    fn header_map(&self) -> HeaderMap;
}

/// Puts `field` into `headers` under `name`, the way a field of a struct deriving
/// [`InboxHeaders`](crate::InboxHeaders) becomes a header; a field without a value leaves the
/// header out.
///
/// # Examples
///
/// ```
/// use ruststream::HeaderMap;
/// use ruststream_sqlx::put_header;
///
/// let tenant = String::from("acme");
/// let trace: Option<String> = None;
/// let mut headers = HeaderMap::with_capacity(2);
/// put_header(&mut headers, "tenant", &tenant);
/// put_header(&mut headers, "trace", &trace);
/// assert_eq!(headers.get_str("tenant"), Some("acme"));
/// assert_eq!(headers.get_str("trace"), None);
/// ```
pub fn put_header(headers: &mut HeaderMap, name: &'static str, field: &impl HeaderField) {
    super::headers::put_header(headers, name, field);
}

impl<Row: HeaderFields> Assembled for Row {
    fn header_map(&self) -> HeaderMap {
        HeaderFields::header_map(self)
    }
}

impl<Row, Settings> QueueRow for Row
where
    Row: InboxTable<Table = InboxSpec<Settings>>,
    Settings: Declaration,
    Settings::Message: MessageAxis<Row>,
{
    type Id = <Row as InboxTable>::Id;
    type Lane = <Settings::Message as MessageAxis<Row>>::Lane;
}

impl<Row, Settings> InboxRow for Row
where
    Row: InboxTable<Table = InboxSpec<Settings>>,
    Settings: Declaration,
    Settings::Message: MessageAxis<Row>,
    Settings::Form: FormAxis,
    Settings::Opening: OpeningAxis,
{
    const SPEC: TableSpec<'static> = check::checked(&Row::TABLE.spec());
    type Form = <Settings::Form as FormAxis>::Form;
    type Opening = <Settings::Opening as OpeningAxis>::Opening;
}

impl<Row, Settings> LeaseRow for Row
where
    Row: InboxRow + InboxTable<Table = InboxSpec<Settings>>,
    Settings: Declaration,
    Settings::Form: LeaseAxis,
{
    type Lease = <Settings::Form as LeaseAxis>::Time;
}

/// The settings `Events` reads as constants.
pub trait Axes {
    /// Which events the service writes itself.
    const SHAPE: Shape;
    /// Whether the service writes any event itself, which a by-name subscription cannot run.
    const OWN: bool;
}

impl<Settings> Axes for InboxSpec<Settings>
where
    Settings: Declaration,
    Settings::OwnClaim: Named,
    Settings::OwnFetch: Named,
    Settings::OwnAck: Named,
    Settings::OwnRetry: Named,
    Settings::OwnRetryAfter: Named,
    Settings::OwnDiscard: Named,
    Settings::OwnDeadLetter: Named,
    Settings::OwnExtend: Named,
    Settings::OwnLock: Named,
    Settings::OwnUnlock: Named,
{
    const SHAPE: Shape = Shape {
        custom_claim: Settings::OwnClaim::NAMED,
        custom_fetch: Settings::OwnFetch::NAMED,
        custom_ack: Settings::OwnAck::NAMED,
        custom_retry: Settings::OwnRetry::NAMED,
        custom_retry_after: Settings::OwnRetryAfter::NAMED,
        custom_discard: Settings::OwnDiscard::NAMED,
        custom_dead_letter: Settings::OwnDeadLetter::NAMED,
        custom_extend: Settings::OwnExtend::NAMED,
        custom_lock: Settings::OwnLock::NAMED,
        custom_unlock: Settings::OwnUnlock::NAMED,
    };
    const OWN: bool = Settings::OwnClaim::NAMED
        || Settings::OwnFetch::NAMED
        || Settings::OwnAck::NAMED
        || Settings::OwnRetry::NAMED
        || Settings::OwnRetryAfter::NAMED
        || Settings::OwnDiscard::NAMED
        || Settings::OwnDeadLetter::NAMED
        || Settings::OwnExtend::NAMED
        || Settings::OwnLock::NAMED
        || Settings::OwnUnlock::NAMED;
}

type Own<Settings> = (
    <Settings as Declaration>::OwnClaim,
    <Settings as Declaration>::OwnFetch,
);
type Token<Settings> = <<Settings as Declaration>::Form as FormAxis>::Token;
type Source<Settings> = <<Settings as Declaration>::Clock as ClockAxis>::Source;

impl<DB, Row, Settings> Events<DB> for Row
where
    DB: QueueDatabase,
    Row: InboxTable<Table = InboxSpec<Settings>> + for<'r> FromRow<'r, DB::Row> + Unpin,
    <Row as InboxTable>::Id: for<'q> Encode<'q, DB> + for<'r> Decode<'r, DB> + Type<DB>,
    Settings: Declaration,
    InboxSpec<Settings>: Axes,
    Settings::Message: MessageAxis<Row>,
    Settings::Form: FormBind<DB>,
    Settings::Key: KeyAxis<Row>,
    Settings::Attempt: AttemptAxis<DB, Row>,
    Settings::Headers: HeadersAxis<Row> + 'static,
    Settings::RetryAfter: TimeAxis<DB>,
    Settings::ProcessedAt: TimeAxis<DB>,
    Settings::Clock: ClockAxis,
    Settings::Opening: OpeningAxis,
    Own<Settings>: ClaimAxes<DB, Row>,
    Settings::OwnAck: AckAxis<DB, Row>,
    Settings::OwnRetry: RetryAxis<DB, Row>,
    Settings::OwnRetryAfter: RetryAfterAxis<DB, Row>,
    Settings::OwnDiscard: DiscardAxis<DB, Row>,
    Settings::OwnDeadLetter: DeadLetterAxis<DB, Row>,
    Settings::OwnExtend: ExtendAxis<DB, Row, Token<Settings>>,
    Settings::OwnLock: LockAxis<DB, Row>,
    Settings::OwnUnlock: UnlockAxis<DB, Row>,
{
    const SHAPE: Shape = <InboxSpec<Settings> as Axes>::SHAPE;

    type Token = Token<Settings>;

    type Ids = <Own<Settings> as ClaimAxes<DB, Row>>::Ids;

    type Headers = ManualHeaders<Settings::Headers, Row>;

    fn kinds() -> Option<Kinds> {
        // A by-name subscription runs the crate's own events only.
        if <InboxSpec<Settings> as Axes>::OWN {
            return None;
        }
        let kinds = KindsOf::new::<<Row as InboxTable>::Id, Source<Settings>>();
        let kinds = <Settings::Message as MessageAxis<Row>>::kinds(kinds);
        let kinds = <Settings::Headers as HeadersAxis<Row>>::kinds(kinds);
        let kinds = <Settings::Key as KeyAxis<Row>>::kinds(kinds);
        let kinds = <Settings::Attempt as AttemptAxis<DB, Row>>::kinds(kinds);
        let kinds = <Settings::RetryAfter as TimeAxis<DB>>::kinds(kinds, true);
        let kinds = <Settings::ProcessedAt as TimeAxis<DB>>::kinds(kinds, false);
        <Settings::Form as FormAxis>::kinds(kinds).finish()
    }

    fn id(&self) -> &<Row as InboxTable>::Id {
        InboxTable::id(self)
    }

    fn take_headers(&mut self) -> HeaderMap {
        <Settings::Headers as HeadersAxis<Row>>::take(self)
    }

    fn unfit_header(headers: &HeaderMap) -> Option<&str> {
        <Settings::Headers as HeadersAxis<Row>>::unfit(headers)
    }

    fn partition_key(&self) -> Option<&[u8]> {
        <Settings::Key as KeyAxis<Row>>::key(self)
    }

    fn attempt(&self) -> Option<u64> {
        <Settings::Attempt as AttemptAxis<DB, Row>>::attempt(self)
    }

    fn read_attempt(row: &DB::Row, _: &'static Queue) -> Option<u64> {
        let column = Row::TABLE
            .spec()
            .column(Role::Attempt)
            .map(|column| column.name());
        <Settings::Attempt as AttemptAxis<DB, Row>>::read(row, column)
    }

    fn bind(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Self>,
    ) -> Result<bool, Error> {
        Ok(match (param, values.event) {
            (Param::Id, _) => match values.id {
                Some(id) => {
                    engine::put::<DB, _>(arguments, id)?;
                    true
                }
                None => false,
            },
            (Param::Group, _) => {
                DB::bind_str(arguments, values.queue.name)?;
                true
            }
            (Param::Limit, _) => {
                DB::bind_i64(arguments, values.limit)?;
                true
            }
            (Param::Destination, Event::DeadLetter) => {
                DB::bind_str(arguments, values.destination)?;
                true
            }
            (Param::Delay, Event::RetryAfter) => {
                DB::bind_i64(arguments, engine::micros(values.delay))?;
                true
            }
            (Param::Key, _) => match values.key {
                Some(key) => {
                    DB::bind_str(arguments, key)?;
                    true
                }
                None => false,
            },
            (Param::Now, Event::Claim | Event::Take) => {
                <Settings::RetryAfter as TimeAxis<DB>>::now::<Source<Settings>, Self>(
                    arguments, values,
                )?
            }
            (Param::RetryAfter, Event::RetryAfter) => {
                <Settings::RetryAfter as TimeAxis<DB>>::later::<Source<Settings>, Self>(
                    arguments, values,
                )?
            }
            (Param::Now, Event::Ack | Event::Discard) => {
                <Settings::ProcessedAt as TimeAxis<DB>>::now::<Source<Settings>, Self>(
                    arguments, values,
                )?
            }
            (Param::LeaseNow | Param::Lease | Param::Held, _) => {
                <Settings::Form as FormBind<DB>>::bind(param, arguments, values)?
            }
            (Param::Ids, Event::Fetch) => {
                <Own<Settings> as ClaimAxes<DB, Row>>::bind_ids(arguments, values)?
            }
            _ => false,
        })
    }

    fn lease(queue: &'static Queue, now: Now) -> Result<Leasing<Token<Settings>>, Error> {
        <Settings::Form as FormAxis>::lease::<Source<Settings>>(queue, now)
    }

    fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<Token<Settings>>>,
        ids: &'a mut Self::Ids,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        <Own<Settings> as ClaimAxes<DB, Row>>::claim(conn, cx, lease, ids, out)
    }

    fn ack<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a <Row as InboxTable>::Id,
        held: Option<&'a Token<Settings>>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        <Settings::OwnAck as AckAxis<DB, Row>>::ack(conn, cx, id, held)
    }

    fn retry<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a <Row as InboxTable>::Id,
        held: Option<&'a Token<Settings>>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        <Settings::OwnRetry as RetryAxis<DB, Row>>::retry(conn, cx, id, held)
    }

    fn retry_after<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a <Row as InboxTable>::Id,
        held: Option<&'a Token<Settings>>,
        delay: Duration,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        <Settings::OwnRetryAfter as RetryAfterAxis<DB, Row>>::retry_after(conn, cx, id, held, delay)
    }

    fn discard<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a <Row as InboxTable>::Id,
        held: Option<&'a Token<Settings>>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        <Settings::OwnDiscard as DiscardAxis<DB, Row>>::discard(conn, cx, id, held)
    }

    fn dead_letter<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a <Row as InboxTable>::Id,
        held: Option<&'a Token<Settings>>,
        destination: &'a str,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        <Settings::OwnDeadLetter as DeadLetterAxis<DB, Row>>::dead_letter(
            conn,
            cx,
            id,
            held,
            destination,
        )
    }

    fn extend<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a <Row as InboxTable>::Id,
        held: &'a Token<Settings>,
        until: &'a Token<Settings>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        <Settings::OwnExtend as ExtendAxis<DB, Row, Token<Settings>>>::extend(
            conn, cx, id, held, until,
        )
    }

    fn lock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        <Settings::OwnLock as LockAxis<DB, Row>>::lock(conn, cx, key)
    }

    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        <Settings::OwnUnlock as UnlockAxis<DB, Row>>::unlock(conn, cx, key)
    }

    fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a <Row as InboxTable>::Id,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        <Own<Settings> as ClaimAxes<DB, Row>>::take(conn, cx, id, out)
    }

    fn candidates<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        out: &'a mut Candidates<<Row as InboxTable>::Id>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        advisory::candidates::<DB, Self>(conn, cx, out)
    }
}
