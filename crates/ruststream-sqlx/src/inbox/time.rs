//! Time on a queue: the types a time column holds, and where "now" comes from.

use std::fmt::Debug;
use std::time::{Duration, SystemTime};

use super::InboxRow;

/// A time a queue column holds: `retry_after`, `processed_at` or `locked_until`.
///
/// The crate reads "now" and moves it forward in the column's own type, so sqlx encodes it
/// exactly as the service writes times itself. `chrono::DateTime<Utc>` implements it under the
/// `chrono` feature and `time::OffsetDateTime` under `time`.
///
/// SQLite keeps times as text, and the claim compares them as text. The text sqlx writes for a
/// `chrono` time (RFC 3339 with `+00:00`) sorts as the times themselves. The text it writes for a
/// `time` value ends in `Z` and drops the fraction's trailing zeros, so it sorts two times right
/// only when they fall in different seconds: on SQLite a lease of such a table may end up to a
/// second late, and a delayed retry may come back up to a second early or late.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// // `retry_after` holds a `DateTime<Utc>`: the crate writes a delayed retry in that type, as the
/// // service's own inserts write the column.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "webhook_jobs")]
/// pub struct Webhook {
///     #[field(id, generated)]
///     id: i64,
///     #[field(retry_after, generated)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Call {
///     url: String,
/// }
///
/// # async fn post(_: &str) -> bool { true }
/// #[subscriber(InboxQueue::<Webhook>::new("webhooks"))]
/// async fn notify(call: &Call) -> HandlerOutcome {
///     if post(&call.url).await {
///         return HandlerOutcome::ack();
///     }
///     HandlerOutcome::retry_after(Duration::from_secs(30))
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("webhooks", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(notify);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait QueueTime: Copy + Debug + Send + Sync + 'static {
    /// The time `at`, in this type.
    fn from_system(at: SystemTime) -> Self;

    /// This time moved `delay` later; the latest time the type holds when that overflows.
    #[must_use]
    fn after(self, delay: Duration) -> Self;

    /// This time rounded up to the next whole second; a whole second stays, and so does the
    /// latest time the type holds.
    ///
    /// A lease ends on a whole second: every temporal column stores one exactly, so the expiry a
    /// claim writes reads back unchanged as the delivery's ownership token.
    #[must_use]
    fn rounded_up(self) -> Self;
}

#[cfg(feature = "chrono")]
impl QueueTime for chrono::DateTime<chrono::Utc> {
    fn from_system(at: SystemTime) -> Self {
        Self::from(at)
    }

    fn after(self, delay: Duration) -> Self {
        chrono::TimeDelta::from_std(delay)
            .ok()
            .and_then(|delta| self.checked_add_signed(delta))
            .unwrap_or(Self::MAX_UTC)
    }

    fn rounded_up(self) -> Self {
        if self.timestamp_subsec_nanos() == 0 {
            return self;
        }
        chrono::Timelike::with_nanosecond(&self, 0)
            .and_then(|whole| whole.checked_add_signed(chrono::TimeDelta::seconds(1)))
            .unwrap_or(self)
    }
}

#[cfg(feature = "time")]
impl QueueTime for time::OffsetDateTime {
    fn from_system(at: SystemTime) -> Self {
        Self::from(at)
    }

    fn after(self, delay: Duration) -> Self {
        self.checked_add(time::Duration::saturating_seconds_f64(delay.as_secs_f64()))
            .unwrap_or_else(|| time::PrimitiveDateTime::MAX.assume_utc())
    }

    fn rounded_up(self) -> Self {
        if self.nanosecond() == 0 {
            return self;
        }
        self.replace_nanosecond(0)
            .ok()
            .and_then(|whole| whole.checked_add(time::Duration::SECOND))
            .unwrap_or(self)
    }
}

/// The type of a field playing a time role: a [`QueueTime`] or an `Option` of one.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// // `processed_at` is empty until the row is finished; an acknowledgement writes a
/// // `DateTime<Utc>` into it and the row stays in the table.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "invoice_jobs")]
/// pub struct InvoiceJob {
///     #[field(id, generated)]
///     id: i64,
///     #[field(processed_at, generated)]
///     processed_at: Option<DateTime<Utc>>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Invoice {
///     number: u64,
/// }
///
/// #[subscriber(InboxQueue::<InvoiceJob>::new("invoices"))]
/// async fn issue(invoice: &Invoice) -> HandlerOutcome {
///     tracing::info!(invoice.number, "issued");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(issue);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait TimeColumn {
    /// The time the column holds.
    type Time: QueueTime;
}

impl<T: QueueTime> TimeColumn for T {
    type Time = T;
}

impl<T: QueueTime> TimeColumn for Option<T> {
    type Time = T;
}

/// A queue row taken by lease: its struct has a `#[field(locked_until)]` field.
///
/// A claim writes the lease's expiry into `locked_until` and commits at once, so the lease, not a
/// transaction, holds the row while the handler runs. The expiry it wrote is the delivery's
/// ownership token: a settlement takes effect only while the row still holds it. While the
/// handler runs, the subscription extends the lease each half lease, and each extension's expiry
/// becomes the token. The crate implements it for every table in the lease form, a struct with the
/// field or a description that sets [`InboxSpec::lease`](crate::InboxSpec::lease), and a
/// subscription of such a table can set its own lease
/// ([`InboxQueue::lease`](crate::InboxQueue::lease)).
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// // `locked_until` makes `Report` a lease row: a claim writes the lease's expiry there and commits.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "report_jobs")]
/// pub struct Report {
///     #[field(id, generated)]
///     id: i64,
///     #[field(locked_until)]
///     locked_until: Option<DateTime<Utc>>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Request {
///     month: u32,
/// }
///
/// // A lease row's subscription sets its own lease; the handler may run for minutes without a
/// // transaction open.
/// #[subscriber(InboxQueue::<Report>::new("reports").lease(Duration::from_secs(300)))]
/// async fn render(request: &Request) -> HandlerOutcome {
///     tracing::info!(month = request.month, "rendering");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(render);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no `locked_until` field, so its subscription holds no lease",
    note = "add `#[field(locked_until)] locked_until: Option<..>` to take rows by lease (by hand: \
            `.lease(..)` and `Lease<..>` in `type Table`)"
)]
pub trait LeaseRow: InboxRow {
    /// The time `locked_until` holds: the lease's expiry, and the delivery's ownership token.
    type Lease: QueueTime;
}

/// A clock on the host: what "now" is for the statements that bind it.
///
/// `#[inbox(clock = MyClock)]` makes a table read it. Hosts' clocks must agree to well within the
/// delays the queue works with; keeping them in step is the service's concern (NTP).
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use std::time::{Duration, SystemTime, UNIX_EPOCH};
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::Clock;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// /// The host's time in whole seconds, as the service's `DATETIME(0)` columns keep it.
/// pub struct WholeSeconds;
///
/// impl Clock for WholeSeconds {
///     fn now() -> SystemTime {
///         let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
///         UNIX_EPOCH + Duration::from_secs(since.as_secs())
///     }
/// }
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "reminder_jobs", clock = WholeSeconds)]
/// pub struct Reminder {
///     #[field(id, generated)]
///     id: i64,
///     #[field(retry_after, generated)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Note {
///     text: String,
/// }
///
/// #[subscriber(InboxQueue::<Reminder>::new("reminders"))]
/// async fn remind(note: &Note) -> HandlerOutcome {
///     tracing::info!(text = %note.text, "reminding");
///     // Back in an hour, counted from `WholeSeconds`.
///     HandlerOutcome::retry_after(Duration::from_secs(3600))
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("reminders", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(remind);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait Clock: Send + Sync + 'static {
    /// Now.
    fn now() -> SystemTime;
}

/// Where a table reads "now": a [`Clock`] on the host, or [`DatabaseClock`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::DatabaseClock;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// // Each table names where it reads "now": `Charge` the host's clock, the default, and
/// // `MonthlyStatement` the database's.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "charge_jobs")]
/// pub struct Charge {
///     #[field(id, generated)]
///     id: i64,
///     #[field(retry_after, generated)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "statement_jobs", clock = DatabaseClock)]
/// pub struct MonthlyStatement {
///     #[field(id, generated)]
///     id: i64,
///     #[field(retry_after, generated)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Account {
///     id: u64,
/// }
///
/// #[subscriber(InboxQueue::<Charge>::new("charges"))]
/// async fn charge(account: &Account) -> HandlerOutcome {
///     tracing::info!(account.id, "charging");
///     HandlerOutcome::retry_after(Duration::from_secs(60))
/// }
///
/// #[subscriber(InboxQueue::<MonthlyStatement>::new("statements"))]
/// async fn send_statement(account: &Account) -> HandlerOutcome {
///     tracing::info!(account.id, "sending the statement");
///     HandlerOutcome::retry_after(Duration::from_secs(60))
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(charge);
///         b.include(send_statement);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait TimeSource: Send + Sync + 'static {
    /// Whether the statements read the database's own clock.
    const DATABASE: bool;

    /// Now, for a statement that binds it; `None` where the database reads its own.
    ///
    /// The crate turns it into each column's own type.
    fn now() -> Option<SystemTime>;
}

impl<C: Clock> TimeSource for C {
    const DATABASE: bool = false;

    fn now() -> Option<SystemTime> {
        Some(C::now())
    }
}

/// The host's system clock: what a table reads unless it names another.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use std::time::{Duration, SystemTime};
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::{Clock, SystemClock};
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// /// The system clock five seconds ahead: a staging host replaying tomorrow's tasks a little early.
/// pub struct Ahead;
///
/// impl Clock for Ahead {
///     fn now() -> SystemTime {
///         SystemClock::now() + Duration::from_secs(5)
///     }
/// }
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "replay_jobs", clock = Ahead)]
/// pub struct Replay {
///     #[field(id, generated)]
///     id: i64,
///     #[field(retry_after, generated)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Task {
///     n: u32,
/// }
///
/// #[subscriber(InboxQueue::<Replay>::new("replays"))]
/// async fn replay(task: &Task) -> HandlerOutcome {
///     tracing::info!(task.n, "replaying");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("staging", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(replay);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now() -> SystemTime {
        SystemTime::now()
    }
}

/// The database's own clock: `#[inbox(clock = DatabaseClock)]` makes the statements read it
/// (`statement_timestamp()` on Postgres, `UTC_TIMESTAMP(6)` on MySQL) instead of binding the
/// host's time.
///
/// A table in the lease form reads the host's clock: every settlement names the expiry its claim
/// wrote, so the crate computes that expiry, and `#[field(locked_until)]` beside
/// `clock = DatabaseClock` does not compile.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "postgres", feature = "chrono"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::DatabaseClock;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// /// Tasks written by hosts whose clocks drift: every claim and every delay reads one clock.
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "sync_jobs", clock = DatabaseClock)]
/// pub struct SyncJob {
///     #[field(id, generated)]
///     id: i64,
///     #[field(retry_after, generated)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Change {
///     record: u64,
/// }
///
/// # async fn push(_: &Change) -> bool { true }
/// #[subscriber(InboxQueue::<SyncJob>::new("sync"))]
/// async fn sync(change: &Change) -> HandlerOutcome {
///     if push(change).await {
///         return HandlerOutcome::ack();
///     }
///     // Ten seconds by the database's clock, whichever host claimed the row.
///     HandlerOutcome::retry_after(Duration::from_secs(10))
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("sync", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(sync);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DatabaseClock;

impl TimeSource for DatabaseClock {
    const DATABASE: bool = true;

    fn now() -> Option<SystemTime> {
        None
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "chrono")]
    #[test]
    fn chrono_times_move_forward_and_saturate() {
        use std::time::{Duration, SystemTime};

        use chrono::{DateTime, Utc};

        use super::QueueTime;

        let start = DateTime::<Utc>::from_system(SystemTime::UNIX_EPOCH);
        assert_eq!(
            start.after(Duration::from_millis(1500)).timestamp_millis(),
            1500
        );
        assert_eq!(start.after(Duration::MAX), DateTime::<Utc>::MAX_UTC);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn chrono_times_round_up_to_the_next_whole_second() {
        use chrono::{DateTime, TimeZone, Utc};

        use super::QueueTime;

        let noon = Utc
            .with_ymd_and_hms(2026, 10, 5, 12, 0, 0)
            .single()
            .expect("a valid time");
        assert_eq!(noon.rounded_up(), noon, "a whole second stays");
        let later = noon + chrono::TimeDelta::microseconds(1);
        assert_eq!(later.rounded_up(), noon + chrono::TimeDelta::seconds(1));
        assert_eq!(
            DateTime::<Utc>::MAX_UTC.rounded_up(),
            DateTime::<Utc>::MAX_UTC,
            "the saturated maximum stays"
        );
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_values_round_up_to_the_next_whole_second() {
        use time::{Duration, OffsetDateTime, PrimitiveDateTime};

        use super::QueueTime;

        // 2026-10-05 12:00:00 UTC.
        let noon = OffsetDateTime::from_unix_timestamp(1_791_201_600).expect("a valid time");
        assert_eq!(noon.rounded_up(), noon, "a whole second stays");
        let later = noon + Duration::microseconds(1);
        assert_eq!(later.rounded_up(), noon + Duration::SECOND);
        let max = PrimitiveDateTime::MAX.assume_utc();
        assert_eq!(max.rounded_up(), max, "the saturated maximum stays");
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_values_move_forward_and_saturate() {
        use std::time::{Duration, SystemTime};

        use time::{OffsetDateTime, PrimitiveDateTime};

        use super::QueueTime;

        let start = OffsetDateTime::from_system(SystemTime::UNIX_EPOCH);
        assert_eq!(start.after(Duration::from_secs(90)).unix_timestamp(), 90);
        assert_eq!(
            start.after(Duration::MAX),
            PrimitiveDateTime::MAX.assume_utc()
        );
    }
}
