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
/// # #[cfg(feature = "chrono")] {
/// use std::time::{Duration, SystemTime};
///
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::QueueTime;
///
/// // A task scheduled a minute ahead, the way a service's insert writes it.
/// let at = DateTime::<Utc>::from_system(SystemTime::now()).after(Duration::from_secs(60));
/// assert!(at > Utc::now());
/// # }
/// ```
pub trait QueueTime: Copy + Debug + Send + Sync + 'static {
    /// The time `at`, in this type.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "chrono")] {
    /// use std::time::{Duration, SystemTime};
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::QueueTime;
    ///
    /// let epoch = DateTime::<Utc>::from_system(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
    /// assert_eq!(epoch.timestamp(), 60);
    /// # }
    /// ```
    fn from_system(at: SystemTime) -> Self;

    /// This time moved `delay` later; the latest time the type holds when that overflows.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "time")] {
    /// use std::time::Duration;
    ///
    /// use ruststream_sqlx::QueueTime;
    /// use time::OffsetDateTime;
    ///
    /// // When a retry thirty seconds out comes back.
    /// let back = OffsetDateTime::UNIX_EPOCH.after(Duration::from_secs(30));
    /// assert_eq!(back.unix_timestamp(), 30);
    /// # }
    /// ```
    #[must_use]
    fn after(self, delay: Duration) -> Self;

    /// This time rounded up to the next whole second; a whole second stays, and so does the
    /// latest time the type holds.
    ///
    /// A lease ends on a whole second: every temporal column stores one exactly, so the expiry a
    /// claim writes reads back unchanged as the delivery's ownership token.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "chrono")] {
    /// use std::time::{Duration, SystemTime};
    ///
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::QueueTime;
    ///
    /// // A lease of thirty seconds taken at 12:00:00.250 ends at 12:00:31.
    /// let taken =
    ///     DateTime::<Utc>::from_system(SystemTime::UNIX_EPOCH + Duration::from_millis(250));
    /// let expiry = taken.after(Duration::from_secs(30)).rounded_up();
    /// assert_eq!(expiry.timestamp_millis(), 31_000);
    /// # }
    /// ```
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
/// # #[cfg(feature = "chrono")] {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::TimeColumn;
///
/// // `processed_at: Option<DateTime<Utc>>` holds a `DateTime<Utc>` once the row is finished.
/// fn finished_at<Field: TimeColumn>(_: &Field) -> &'static str {
///     std::any::type_name::<Field::Time>()
/// }
/// let processed_at: Option<DateTime<Utc>> = None;
/// assert!(finished_at(&processed_at).contains("DateTime"));
/// # }
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
/// A claim writes the lease's expiry into `locked_until` and commits at once, so the handler runs
/// outside any transaction. The expiry it wrote is the delivery's ownership token: a settlement
/// takes effect only while the row still holds it. While the handler runs, the subscription
/// extends the lease each half lease, and each extension's expiry becomes the token. The derive
/// implements it for a struct with the field, and a subscription of such a struct can set its own
/// lease ([`InboxQueue::lease`](crate::InboxQueue::lease)).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "chrono")] {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::{Inbox, LeaseRow};
///
/// #[derive(Inbox)]
/// #[inbox(table = "report_jobs")]
/// struct Report {
///     #[field(id)]
///     id: i64,
///     #[field(locked_until)]
///     locked_until: Option<DateTime<Utc>>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// /// The type a lease table's expiry is written in, for the line a service logs when it starts.
/// fn expiry_type<Row: LeaseRow>() -> &'static str {
///     std::any::type_name::<Row::Lease>()
/// }
///
/// assert!(expiry_type::<Report>().contains("DateTime"));
/// # let _ = |report: Report| (report.id, report.locked_until, report.payload);
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no `locked_until` field, so its subscription holds no lease",
    note = "add `#[field(locked_until)] locked_until: Option<..>` to take rows by lease"
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
/// use std::time::{Duration, SystemTime};
///
/// use ruststream_sqlx::Clock;
///
/// /// The host's clock, five seconds ahead: a staging environment replaying tomorrow's tasks
/// /// a little early.
/// struct Ahead;
///
/// impl Clock for Ahead {
///     fn now() -> SystemTime {
///         SystemTime::now() + Duration::from_secs(5)
///     }
/// }
///
/// assert!(Ahead::now() > SystemTime::now());
/// ```
pub trait Clock: Send + Sync + 'static {
    /// Now.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::SystemTime;
    ///
    /// use ruststream_sqlx::{Clock, SystemClock};
    ///
    /// let before = SystemTime::now();
    /// assert!(SystemClock::now() >= before);
    /// ```
    fn now() -> SystemTime;
}

/// Where a table reads "now": a [`Clock`] on the host, or [`DatabaseClock`].
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::{DatabaseClock, SystemClock, TimeSource};
///
/// // A statement binds "now" from the host, or writes the database's own clock into its text.
/// fn binds_now<Source: TimeSource>() -> bool {
///     !Source::DATABASE
/// }
/// assert!(binds_now::<SystemClock>());
/// assert!(!binds_now::<DatabaseClock>());
/// ```
pub trait TimeSource: Send + Sync + 'static {
    /// Whether the statements read the database's own clock.
    const DATABASE: bool;

    /// Now, in the column's type, for a statement that binds it; `None` where the database reads
    /// its own.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "chrono")] {
    /// use chrono::{DateTime, Utc};
    /// use ruststream_sqlx::{DatabaseClock, SystemClock, TimeSource};
    ///
    /// assert!(<SystemClock as TimeSource>::now::<DateTime<Utc>>().is_some());
    /// assert!(<DatabaseClock as TimeSource>::now::<DateTime<Utc>>().is_none());
    /// # }
    /// ```
    fn now<T: QueueTime>() -> Option<T>;
}

impl<C: Clock> TimeSource for C {
    const DATABASE: bool = false;

    fn now<T: QueueTime>() -> Option<T> {
        Some(T::from_system(C::now()))
    }
}

/// The host's system clock: what a table reads unless it names another.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::{Clock, SystemClock};
///
/// let now = SystemClock::now();
/// assert!(now.elapsed().is_ok());
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
/// # Examples
///
/// ```
/// # #[cfg(feature = "chrono")] {
/// use chrono::{DateTime, Utc};
/// use ruststream_sqlx::{DatabaseClock, Inbox, InboxRow};
///
/// /// Tasks written by hosts whose clocks drift: every claim reads one clock.
/// #[derive(Inbox)]
/// #[inbox(table = "jobs", clock = DatabaseClock)]
/// struct Job {
///     #[field(id)]
///     id: i64,
///     #[field(retry_after)]
///     retry_after: DateTime<Utc>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// assert!(Job::SPEC.uses_database_clock());
/// # let _ = |job: Job| (job.id, job.retry_after, job.payload);
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DatabaseClock;

impl TimeSource for DatabaseClock {
    const DATABASE: bool = true;

    fn now<T: QueueTime>() -> Option<T> {
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
