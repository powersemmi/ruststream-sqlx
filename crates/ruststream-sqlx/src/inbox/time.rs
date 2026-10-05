//! Time on a queue: the types a time column holds, and where "now" comes from.

use std::time::{Duration, SystemTime};

/// A time a queue column holds: `retry_after` or `processed_at`.
///
/// The crate reads "now" and moves it forward in the column's own type, so sqlx encodes it
/// exactly as the service writes times itself. `chrono::DateTime<Utc>` implements it under the
/// `chrono` feature and `time::OffsetDateTime` under `time`.
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
pub trait QueueTime: Sized + Send + Sync + 'static {
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
/// (`statement_timestamp()` on Postgres) instead of binding the host's time.
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
