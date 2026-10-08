//! What a statement runs in: a claim or a settlement in progress, the lease a claim takes, where
//! "now" comes from, and the bounds the derive phrases its time columns with.

use std::time::SystemTime;

use sqlx::{Database, Encode, Type};

use crate::inbox::queue::Queue;
#[cfg(feature = "testing")]
use crate::inbox::testing::TestClock;
use crate::inbox::time::{QueueTime, TimeColumn, TimeSource};

/// A claim in progress.
#[derive(Debug, Clone, Copy)]
pub struct Claiming {
    /// The subscription: its name, its statements and what it knows of the table.
    pub queue: &'static Queue,
    /// The most rows to take.
    pub limit: i64,
    /// Where "now" comes from for a claim that takes no lease.
    pub now: Now,
}

/// The lease a claim takes, from one reading of the clock: that instant, and the instant and the
/// lease's end in the lease column's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Leasing<Token> {
    /// The instant the claim read: every time the claim binds starts from it.
    pub at: SystemTime,
    /// The same instant in the lease column's type: a row whose lease ended by then is free.
    pub now: Token,
    /// The lease's end, which the claim writes and its deliveries hold: the queue's lease after
    /// `now`, rounded up to a whole second.
    pub expiry: Token,
}

// A by-name lease turns its times into the time type its column names, and a column names one only
// with a time feature on.
#[cfg(any(feature = "chrono", feature = "time"))]
impl<Token> Leasing<Token> {
    /// The same lease, each of its times in the lease column's type turned by `into`.
    pub(crate) fn map<Other>(self, into: impl Fn(Token) -> Other) -> Leasing<Other> {
        Leasing {
            at: self.at,
            now: into(self.now),
            expiry: into(self.expiry),
        }
    }
}

/// A settlement in progress.
#[derive(Debug, Clone, Copy)]
pub struct Settling {
    /// The subscription: its name, its statements and what it knows of the table.
    pub queue: &'static Queue,
    /// Where "now" comes from.
    pub now: Now,
}

/// Where "now" comes from for one statement: the row's [`TimeSource`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Now {
    /// The clock of an in-process connection, which stands in for a host clock.
    #[cfg(feature = "testing")]
    test: Option<TestClock>,
    _private: (),
}

impl Now {
    /// Now as the host reads it, from `Source`; `None` where the database reads its own clock.
    pub(super) fn instant<Source: TimeSource>(self) -> Option<SystemTime> {
        #[cfg(feature = "testing")]
        if let Some(clock) = self.test
            && !Source::DATABASE
        {
            return Some(clock.now());
        }
        Source::now()
    }

    /// "Now" of an in-process connection: `clock` instead of a host clock, where it is set.
    #[cfg(feature = "testing")]
    pub(crate) const fn test(clock: Option<TestClock>) -> Self {
        Self {
            test: clock,
            _private: (),
        }
    }
}

/// Phrases a bound over a type that names no generic parameter so that it does.
///
/// The derive's bounds on a field's type then rule an impl out instead of failing the build where
/// the field's type does not fit. Machinery.
pub trait Via<Marker> {
    /// The type itself.
    type Is: ?Sized;
}

impl<Marker, T: ?Sized> Via<Marker> for T {
    type Is = T;
}

/// The time a time column binds in `DB`.
///
/// Machinery: a time role's type is bounded by it, which names the database, so a type the
/// database cannot bind rules the table out where it is mounted instead of failing the build.
pub trait TimeFor<DB: Database> {
    /// The time the column holds.
    type Time: QueueTime + for<'q> Encode<'q, DB> + Type<DB>;
}

impl<DB, C> TimeFor<DB> for C
where
    DB: Database,
    C: TimeColumn,
    C::Time: for<'q> Encode<'q, DB> + Type<DB>,
{
    type Time = C::Time;
}
