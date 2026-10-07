//! How a by-name row binds its statements: its id, its times on the host's clock and its lease,
//! and the events it leaves to the crate.

use std::mem;
use std::time::Duration;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
use ruststream::HeaderMap;
use ruststream_sqlx_dialect::Param;
#[cfg(any(feature = "chrono", feature = "time"))]
use sqlx::Database;
use sqlx::{Column, Error, Row};
#[cfg(feature = "time")]
use time::OffsetDateTime;

use super::{NamedBytes, NamedId, NamedRow, NamedTime, attempt_fits, hold_to};
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{
    self, Claimed, Claiming, Event, Events, Leasing, Now, Settled, Settling, Shape, Values,
};
use crate::inbox::form::advisory::events::{self as advisory, Candidates};
use crate::inbox::named::by_name::ByName;
use crate::inbox::named::database::RoleColumns;
use crate::inbox::named::kinds::Kinds;
#[cfg(any(feature = "chrono", feature = "time"))]
use crate::inbox::named::kinds::{ClockKind, TimeKind};
use crate::inbox::queue::Queue;
#[cfg(any(feature = "chrono", feature = "time"))]
use crate::inbox::time::{QueueTime, SystemClock};

/// Binds now on the host's clock, or `delay` later, in the time `column` holds; `false` where
/// the table has no such column or reads the database's clock.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_now<DB, D>(
    arguments: &mut DB::Arguments,
    values: &Values<'_, DB, NamedRow<D>>,
    column: fn(Kinds) -> Option<TimeKind>,
    delay: Option<Duration>,
) -> Result<bool, Error>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    let Some(kinds) = values.queue.kinds else {
        return Ok(false);
    };
    let (Some(kind), ClockKind::System) = (column(kinds), kinds.clock) else {
        return Ok(false);
    };
    match kind {
        #[cfg(feature = "chrono")]
        TimeKind::Chrono => {
            bind_at::<DB, D, DateTime<Utc>>(arguments, values, delay, NamedTime::Chrono)
        }
        #[cfg(feature = "time")]
        TimeKind::Time => {
            bind_at::<DB, D, OffsetDateTime>(arguments, values, delay, NamedTime::Time)
        }
    }
}

/// Binds `lease`; `false` where the statement has no lease to bind.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_lease<DB, D>(arguments: &mut DB::Arguments, lease: Option<NamedTime>) -> Result<bool, Error>
where
    DB: Database,
    D: ByName<DB>,
{
    let Some(lease) = lease else {
        return Ok(false);
    };
    D::bind_time(arguments, lease)?;
    Ok(true)
}

/// Binds now, or `delay` later, as a `Time`.
#[cfg(any(feature = "chrono", feature = "time"))]
fn bind_at<DB, D, Time>(
    arguments: &mut DB::Arguments,
    values: &Values<'_, DB, NamedRow<D>>,
    delay: Option<Duration>,
    named: fn(Time) -> NamedTime,
) -> Result<bool, Error>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
    Time: QueueTime,
{
    let now = engine::now::<SystemClock, Time, DB, NamedRow<D>>(values)?;
    let at = delay.map_or(now, |delay| now.after(delay));
    D::bind_time(arguments, named(at))?;
    Ok(true)
}

impl<DB, D> Events<DB> for NamedRow<D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    const SHAPE: Shape = Shape {
        custom_claim: false,
        custom_fetch: false,
        custom_ack: false,
        custom_retry: false,
        custom_retry_after: false,
        custom_discard: false,
        custom_dead_letter: false,
        custom_extend: false,
        custom_lock: false,
        custom_unlock: false,
    };

    // The lease in the type of the route's `locked_until` column, which the queue's kinds name.
    type Token = NamedTime;

    type Ids = ();

    type Headers = HeaderMap;

    fn kinds() -> Option<Kinds> {
        // The route's own row answers when the subscription opens.
        None
    }

    fn id(&self) -> &NamedId {
        &self.id
    }

    fn take_headers(&mut self) -> HeaderMap {
        mem::take(&mut self.headers)
    }

    fn unfit_header(headers: &HeaderMap) -> Option<&str> {
        // Nothing publishes through a by-name row: the route's own row writes the table.
        engine::first_header(headers)
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.key.as_ref().map(NamedBytes::as_bytes)
    }

    fn attempt(&self) -> Option<u64> {
        self.attempt
    }

    fn read_attempt(row: &DB::Row, queue: &'static Queue) -> Option<u64> {
        // The alias `ClaimShape::Roles` gives the column, held to the type the struct reads it as.
        let kind = queue.kinds?.attempt?;
        let column = row
            .columns()
            .iter()
            .find(|column| column.name() == "attempt")?;
        let (attempt, held) = DB::attempt(DB::value(row, column.ordinal()).ok()?).ok()?;
        attempt_fits(kind, held).then_some(attempt)
    }

    fn bind(
        param: Param,
        arguments: &mut DB::Arguments,
        values: &Values<'_, DB, Self>,
    ) -> Result<bool, Error> {
        Ok(match (param, values.event) {
            (Param::Id, _) => match values.id {
                Some(id) => {
                    DB::bind_id(arguments, id)?;
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
            // A time binds only where a time type is enabled: without one no table has a time
            // column, and these fall through to `false`.
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Now, Event::Claim | Event::Take) => {
                bind_now::<DB, D>(arguments, values, |kinds| kinds.retry_after, None)?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Now, Event::Ack | Event::Discard) => {
                bind_now::<DB, D>(arguments, values, |kinds| kinds.processed_at, None)?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::RetryAfter, Event::RetryAfter) => bind_now::<DB, D>(
                arguments,
                values,
                |kinds| kinds.retry_after,
                Some(values.delay),
            )?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::LeaseNow, _) => {
                bind_lease::<DB, D>(arguments, values.leasing.map(|leasing| leasing.now))?
            }
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Lease, _) => bind_lease::<DB, D>(arguments, values.lease)?,
            #[cfg(any(feature = "chrono", feature = "time"))]
            (Param::Held, _) => bind_lease::<DB, D>(arguments, values.held)?,
            _ => false,
        })
    }

    fn lease(queue: &'static Queue, now: Now) -> Result<Leasing<NamedTime>, Error> {
        #[cfg(any(feature = "chrono", feature = "time"))]
        if let Some(Kinds {
            locked_until: Some(kind),
            clock: ClockKind::System,
            ..
        }) = queue.kinds
        {
            return Ok(match kind {
                #[cfg(feature = "chrono")]
                TimeKind::Chrono => {
                    engine::lease::<SystemClock, DateTime<Utc>>(queue, now)?.map(NamedTime::Chrono)
                }
                #[cfg(feature = "time")]
                TimeKind::Time => {
                    engine::lease::<SystemClock, OffsetDateTime>(queue, now)?.map(NamedTime::Time)
                }
            });
        }
        let _ = (queue, now);
        // A table whose kinds name no lease has none for the role columns to bind.
        Err(engine::unbound(Param::Lease, Event::Claim))
    }

    async fn claim<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        lease: Option<&'a Leasing<NamedTime>>,
        _ids: &'a mut (),
        out: &'a mut Vec<Claimed<Self>>,
    ) -> Result<(), Error> {
        let claimed = out.len();
        engine::claim_rows::<DB, Self>(conn, cx, lease, out).await?;
        if let Some(kinds) = cx.queue.kinds {
            for row in &mut out[claimed..] {
                hold_to(kinds, row)?;
            }
        }
        Ok(())
    }

    fn ack<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::ack::<DB, Self>(conn, cx, id, held)
    }

    fn retry<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::retry::<DB, Self>(conn, cx, id, held)
    }

    fn retry_after<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
        delay: Duration,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::retry_after::<DB, Self>(conn, cx, id, held, delay)
    }

    fn discard<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::discard::<DB, Self>(conn, cx, id, held)
    }

    fn dead_letter<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: Option<&'a NamedTime>,
        destination: &'a str,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::dead_letter::<DB, Self>(conn, cx, id, held, destination)
    }

    fn extend<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        id: &'a NamedId,
        held: &'a NamedTime,
        until: &'a NamedTime,
    ) -> impl Future<Output = Result<Settled, Error>> + Send + 'a {
        engine::extend::<DB, Self>(conn, cx, id, held, until)
    }

    fn lock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        advisory::lock::<DB, Self>(conn, cx, key)
    }

    fn unlock<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Settling,
        key: &'a str,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'a {
        advisory::unlock::<DB, Self>(conn, cx, key)
    }

    fn candidates<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        out: &'a mut Candidates<NamedId>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        advisory::candidates::<DB, Self>(conn, cx, out)
    }

    async fn take<'a>(
        conn: &'a mut DB::Connection,
        cx: &'a Claiming,
        id: &'a NamedId,
        out: &'a mut Vec<Claimed<Self>>,
    ) -> Result<bool, Error> {
        let taken = out.len();
        let found = advisory::take::<DB, Self>(conn, cx, id, out).await?;
        if let Some(kinds) = cx.queue.kinds {
            for row in &mut out[taken..] {
                hold_to(kinds, row)?;
            }
        }
        Ok(found)
    }
}

#[cfg(all(test, feature = "postgres", feature = "chrono"))]
mod tests {
    //! How a by-name row binds its statements on Postgres.

    use std::time::Duration;

    use chrono::{DateTime, TimeDelta, Utc};
    use ruststream::HeaderMap;
    use ruststream_sqlx_dialect::{Column, Form, Param, TableSpec};
    use sqlx::postgres::PgArguments;
    use sqlx::{Arguments, Postgres};

    use super::super::tests::{FITTING, KINDS, row};
    use super::super::{NamedId, NamedRow, NamedTime};
    use crate::inbox::engine::{Event, Events, IdAt, Leasing, Now, Prepared, Shape, Values};
    use crate::inbox::named::kinds::{ClockKind, Kinds, TimeKind};
    use crate::inbox::queue::Queue;
    use crate::inbox::time::QueueTime;
    use crate::inbox::{BuiltIn, PayloadRow};

    /// The row of a by-name subscription on Postgres, read and bound by its built-in dialect.
    type Named = NamedRow<BuiltIn<Postgres>>;

    const SPEC: TableSpec<'static> =
        TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
            .payload(Column::new("payload"));

    /// The subscription `emails` of a table read with `kinds`.
    fn queue(kinds: Option<Kinds>) -> &'static Queue {
        Box::leak(Box::new(Queue {
            name: "emails",
            table: "email_jobs",
            row: "SendEmail",
            spec: SPEC,
            id_at: IdAt::First,
            native_retry_after: true,
            kinds,
            prepared: Prepared::default(),
            begin_claim: None,
            counted_attempt: false,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        }))
    }

    /// The subscription `emails` of a lease table read with `kinds`, with a lease of thirty
    /// seconds.
    fn leased(kinds: Option<Kinds>) -> &'static Queue {
        Box::leak(Box::new(Queue {
            spec: TableSpec::new(
                "email_jobs",
                Column::new("job_id"),
                Form::Lease(Column::new("locked_until")),
            )
            .payload(Column::new("payload")),
            lease: Some(Duration::from_secs(30)),
            ..*queue(kinds)
        }))
    }

    /// Binds `param` for `event`, and says whether it bound and how many values are bound.
    fn bind(
        param: Param,
        event: Event,
        queue: &'static Queue,
        id: Option<&NamedId>,
    ) -> Result<(bool, usize), sqlx::Error> {
        bind_leased(param, event, queue, id, None)
    }

    /// Binds `param` for `event` where the statement takes `leasing`, and writes and matches
    /// its expiry.
    fn bind_leased(
        param: Param,
        event: Event,
        queue: &'static Queue,
        id: Option<&NamedId>,
        leasing: Option<&Leasing<NamedTime>>,
    ) -> Result<(bool, usize), sqlx::Error> {
        let lease = leasing.map(|leasing| leasing.expiry);
        let values = Values {
            event,
            queue,
            limit: 10,
            id,
            ids: &[],
            delay: Duration::from_secs(30),
            destination: "emails.dead",
            now: Now::default(),
            lease,
            leasing,
            held: lease,
            key: None,
        };
        let mut arguments = PgArguments::default();
        let bound = <Named as Events<Postgres>>::bind(param, &mut arguments, &values)?;
        Ok((bound, arguments.len()))
    }

    #[test]
    fn a_lock_key_binds_as_text_where_the_statement_names_one() -> Result<(), sqlx::Error> {
        let queue = queue(Some(KINDS));
        let bind_key = |event, key| {
            let values = Values {
                event,
                queue,
                limit: 10,
                id: None,
                ids: &[],
                delay: Duration::ZERO,
                destination: "",
                now: Now::default(),
                lease: None,
                leasing: None,
                held: None,
                key,
            };
            let mut arguments = PgArguments::default();
            let bound = <Named as Events<Postgres>>::bind(Param::Key, &mut arguments, &values)?;
            Ok::<_, sqlx::Error>((bound, arguments.len()))
        };
        assert_eq!(bind_key(Event::Lock, Some("email_jobs-7"))?, (true, 1));
        assert_eq!(bind_key(Event::Unlock, Some("email_jobs-7"))?, (true, 1));
        // A statement outside the lock and the unlock has no key to bind.
        assert_eq!(bind_key(Event::Ack, None)?, (false, 0));
        Ok(())
    }

    #[test]
    fn a_lease_is_taken_and_bound_in_the_type_of_its_column() -> Result<(), sqlx::Error> {
        let queue = leased(Some(Kinds {
            locked_until: Some(TimeKind::Chrono),
            ..KINDS
        }));
        let before = Utc::now();
        let lease = <Named as Events<Postgres>>::lease(queue, Now::default())?;
        assert!(
            matches!(
                lease,
                Leasing {
                    at,
                    now: NamedTime::Chrono(now),
                    expiry: NamedTime::Chrono(expiry),
                } if now == DateTime::<Utc>::from(at)
                    && expiry.timestamp_subsec_nanos() == 0
                    && expiry >= before + TimeDelta::seconds(30)
                    && expiry == now.after(Duration::from_secs(30)).rounded_up()
            ),
            "a whole second, a lease from the claim's now: {lease:?}"
        );
        for param in [Param::Lease, Param::Held, Param::LeaseNow] {
            assert_eq!(
                bind_leased(param, Event::Extend, queue, None, Some(&lease))?,
                (true, 1),
                "{param:?}"
            );
        }
        // A claim holds no lease yet, and a statement outside the lease form writes none.
        assert_eq!(
            bind_leased(Param::Held, Event::Claim, queue, None, None)?,
            (false, 0)
        );
        // A queue whose kinds name no lease cannot tell one.
        let refused = <Named as Events<Postgres>>::lease(leased(Some(KINDS)), Now::default());
        assert!(
            matches!(refused, Err(sqlx::Error::Configuration(_))),
            "{refused:?}"
        );
        assert_eq!(
            bind(Param::LeaseNow, Event::Claim, leased(Some(KINDS)), None)?,
            (false, 0)
        );
        Ok(())
    }

    fn timed(clock: ClockKind) -> Kinds {
        Kinds {
            clock,
            retry_after: Some(TimeKind::Chrono),
            processed_at: Some(TimeKind::Chrono),
            ..KINDS
        }
    }

    #[test]
    fn every_id_binds_as_its_column_holds_it() -> Result<(), sqlx::Error> {
        let queue = queue(Some(KINDS));
        for id in [
            NamedId::I16(1),
            NamedId::I32(2),
            NamedId::I64(3),
            NamedId::Text("job-4".to_owned()),
            NamedId::Bytes(vec![5]),
        ] {
            assert_eq!(bind(Param::Id, Event::Ack, queue, Some(&id))?, (true, 1));
        }
        assert_eq!(bind(Param::Id, Event::Ack, queue, None)?, (false, 0));
        Ok(())
    }

    #[test]
    fn the_queue_binds_its_name_limit_destination_and_delay() -> Result<(), sqlx::Error> {
        let queue = queue(Some(KINDS));
        assert_eq!(bind(Param::Group, Event::Claim, queue, None)?, (true, 1));
        assert_eq!(bind(Param::Limit, Event::Claim, queue, None)?, (true, 1));
        assert_eq!(
            bind(Param::Destination, Event::DeadLetter, queue, None)?,
            (true, 1)
        );
        assert_eq!(
            bind(Param::Destination, Event::Ack, queue, None)?,
            (false, 0)
        );
        assert_eq!(
            bind(Param::Delay, Event::RetryAfter, queue, None)?,
            (true, 1)
        );
        assert_eq!(bind(Param::Ids, Event::Fetch, queue, None)?, (false, 0));
        Ok(())
    }

    #[test]
    fn times_bind_on_the_hosts_clock_in_their_columns_type() -> Result<(), sqlx::Error> {
        let host = queue(Some(timed(ClockKind::System)));
        assert_eq!(bind(Param::Now, Event::Claim, host, None)?, (true, 1));
        // The take of an advisory claim checks the row is still due, as the claim does.
        assert_eq!(bind(Param::Now, Event::Take, host, None)?, (true, 1));
        assert_eq!(bind(Param::Now, Event::Ack, host, None)?, (true, 1));
        assert_eq!(bind(Param::Now, Event::Discard, host, None)?, (true, 1));
        assert_eq!(
            bind(Param::RetryAfter, Event::RetryAfter, host, None)?,
            (true, 1)
        );
        // The database's clock is read by the statement itself; a table without the column,
        // or a queue that never described its kinds, has no time to bind.
        let database = queue(Some(timed(ClockKind::Database)));
        assert_eq!(bind(Param::Now, Event::Claim, database, None)?, (false, 0));
        let untimed = queue(Some(KINDS));
        assert_eq!(bind(Param::Now, Event::Ack, untimed, None)?, (false, 0));
        assert_eq!(
            bind(Param::Now, Event::Claim, queue(None), None)?,
            (false, 0)
        );
        Ok(())
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_crate_columns_bind_too() -> Result<(), sqlx::Error> {
        let kinds = Some(Kinds {
            retry_after: Some(TimeKind::Time),
            ..KINDS
        });
        assert_eq!(
            bind(Param::RetryAfter, Event::RetryAfter, queue(kinds), None)?,
            (true, 1)
        );
        Ok(())
    }

    #[test]
    fn a_named_row_lends_its_parts_and_leaves_every_event_to_the_crate() {
        let mut row = row(FITTING);
        assert_eq!(row.payload(), b"{}");
        assert_eq!(<Named as Events<Postgres>>::id(&row), &NamedId::I64(7));
        assert_eq!(
            <Named as Events<Postgres>>::partition_key(&row),
            Some(b"acme".as_slice())
        );
        assert_eq!(<Named as Events<Postgres>>::attempt(&row), Some(2));
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        row.headers = headers.clone();
        // The delivery takes the headers once: they move out of the row.
        assert_eq!(<Named as Events<Postgres>>::take_headers(&mut row), headers);
        assert!(<Named as Events<Postgres>>::take_headers(&mut row).is_empty());
        assert_eq!(<Named as Events<Postgres>>::SHAPE, Shape::default());
        assert_eq!(<Named as Events<Postgres>>::kinds(), None);
        assert_eq!(
            <Named as Events<Postgres>>::unfit_header(&headers),
            Some("x-tenant")
        );
    }
}
