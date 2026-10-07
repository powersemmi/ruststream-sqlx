//! How the columns of a queue row reach a delivery: its headers, its key, its attempt.

#[cfg(feature = "json")]
use std::collections::BTreeMap;
#[cfg(feature = "json")]
use std::mem;

use ruststream::HeaderMap;
#[cfg(feature = "json")]
use sqlx::types::Json;

/// The type of a field playing `headers`: what the delivery's header map holds.
///
/// `sqlx::types::Json<BTreeMap<String, String>>` implements it under the `json` feature (a
/// `jsonb` or `json` column of string values), and so does an `Option` of any implementation.
/// A publish carrying a header the column cannot hold byte for byte is refused before the
/// service's [`Publish`](crate::Publish) runs: a header never reaches the table changed.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::HeaderMap;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::HeaderColumn;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// /// Headers stored one per line, `name: value`, in a `TEXT` column.
/// #[derive(sqlx::Type)]
/// #[sqlx(transparent)]
/// pub struct Lines(String);
///
/// impl HeaderColumn for Lines {
///     fn take_headers(&mut self) -> HeaderMap {
///         let headers = self
///             .0
///             .lines()
///             .filter_map(|line| line.split_once(": "))
///             .map(|(name, value)| (name.to_owned(), value.to_owned()))
///             .collect();
///         self.0.clear();
///         headers
///     }
///
///     fn from_headers(headers: &HeaderMap) -> Self {
///         let lines: Vec<String> = headers
///             .iter()
///             .map(|(name, value)| format!("{name}: {}", String::from_utf8_lossy(value)))
///             .collect();
///         Self(lines.join("\n"))
///     }
///
///     fn unfit(headers: &HeaderMap) -> Option<&str> {
///         // A value comes back as it went in when it is text on one line.
///         let fits = |value: &[u8]| str::from_utf8(value).is_ok_and(|text| !text.contains('\n'));
///         headers
///             .iter()
///             .find(|(_, value)| !fits(value))
///             .map(|(name, _)| name)
///     }
/// }
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs")]
/// pub struct Job {
///     #[field(id, generated)]
///     id: i64,
///     #[field(headers)]
///     headers: Lines,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Task {
///     n: u32,
/// }
///
/// // The delivery's headers come from `headers`, so the service's middleware reads `x-tenant`
/// // there as on any broker.
/// #[subscriber(InboxQueue::<Job>::new("tasks"))]
/// async fn run(task: &Task) -> HandlerOutcome {
///     tracing::info!(task.n, "running");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(run);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait HeaderColumn {
    /// Moves the headers the column holds into a header map, leaving the column empty.
    ///
    /// A delivery takes them once, when its row is claimed, so a column that owns its strings
    /// hands them over without a copy.
    fn take_headers(&mut self) -> HeaderMap;

    /// The column value that holds `headers`, for a service's `Publish`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "json"))]
    /// # mod demo {
    /// use std::collections::BTreeMap;
    ///
    /// use ruststream::OutgoingMessage;
    /// use ruststream_sqlx::{HeaderColumn, Inbox, Publish};
    /// use sqlx::types::Json;
    /// use sqlx::{PgConnection, Postgres};
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "jobs")]
    /// pub struct Job {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(headers)]
    ///     headers: Json<BTreeMap<String, String>>,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl Publish<Postgres> for Job {
    ///     async fn publish(
    ///         conn: &mut PgConnection,
    ///         message: &OutgoingMessage<'_>,
    ///     ) -> Result<(), sqlx::Error> {
    ///         // The `jsonb` column holds the message's headers as the delivery will read them back.
    ///         let headers = Json::<BTreeMap<String, String>>::from_headers(message.headers());
    ///         sqlx::query("INSERT INTO jobs (headers, payload) VALUES ($1, $2)")
    ///             .bind(headers)
    ///             .bind(message.payload())
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    fn from_headers(headers: &HeaderMap) -> Self;

    /// The first of `headers` the column cannot hold byte for byte, if any: the broker refuses a
    /// publish that carries it.
    fn unfit(headers: &HeaderMap) -> Option<&str>;
}

impl<T: HeaderColumn> HeaderColumn for Option<T> {
    fn take_headers(&mut self) -> HeaderMap {
        self.as_mut()
            .map_or_else(HeaderMap::new, HeaderColumn::take_headers)
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        (!headers.is_empty()).then(|| T::from_headers(headers))
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        T::unfit(headers)
    }
}

/// A JSON object of strings. A value that is not UTF-8 does not fit: the broker refuses a publish
/// that carries one, so the replacement `from_headers` would write never reaches the table.
#[cfg(feature = "json")]
impl HeaderColumn for Json<BTreeMap<String, String>> {
    fn take_headers(&mut self) -> HeaderMap {
        mem::take(&mut self.0).into_iter().collect()
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        Self(
            headers
                .iter()
                .map(|(name, value)| (name.to_owned(), String::from_utf8_lossy(value).into_owned()))
                .collect(),
        )
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        headers
            .iter()
            .find(|(_, value)| str::from_utf8(value).is_err())
            .map(|(name, _)| name)
    }
}

/// The type of a field playing `partition_key`: the bytes the delivery's key lends.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "order_jobs")]
/// pub struct OrderJob {
///     #[field(id, generated)]
///     id: i64,
///     // A `String` lends its bytes as the delivery's key; an empty `Option` gives none.
///     #[field(partition_key)]
///     customer: Option<String>,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
/// async fn fulfil(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "fulfilling");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // Four workers, and the orders of one customer keep their order on one of them.
///         b.include(fulfil.workers_by_key(nonzero!(4)));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait KeyColumn {
    /// The key's bytes, or `None` when the row carries no key.
    fn key(&self) -> Option<&[u8]>;
}

macro_rules! keys {
    ($($ty:ty => $bytes:ident),*) => {$(
        impl KeyColumn for $ty {
            fn key(&self) -> Option<&[u8]> {
                Some(self.$bytes())
            }
        }
    )*};
}

keys!(String => as_bytes, Box<str> => as_bytes, Vec<u8> => as_slice, Box<[u8]> => as_ref);

impl<T: KeyColumn> KeyColumn for Option<T> {
    fn key(&self) -> Option<&[u8]> {
        self.as_ref().and_then(KeyColumn::key)
    }
}

/// The type of a field playing `attempt`: the delivery's redelivery count.
///
/// The first delivery of a row reads 1, so a table's `attempt` column starts at 1. A negative
/// value reads as 0.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "charge_jobs")]
/// pub struct ChargeJob {
///     #[field(id, generated)]
///     id: i64,
///     // sqlx has no unsigned integers on Postgres, so the column is a `SMALLINT DEFAULT 1`.
///     #[field(attempt, generated)]
///     attempt: i16,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Charge {
///     order: u64,
/// }
///
/// # async fn try_charge(_: &Charge) -> bool { true }
/// #[subscriber(InboxQueue::<ChargeJob>::new("charges"))]
/// async fn charge(request: &Charge, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
///     tracing::info!(request.order, ?attempt, "charging");
///     if try_charge(request).await { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // The attempt counts the deliveries: the fifth failure moves the row into
///         // `failed_charge_jobs`, a table with the same columns.
///         b.include(charge).max_attempts(nonzero!(5u32)).dead_letter("failed_charge_jobs");
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait AttemptColumn {
    /// The attempt, counting the first delivery as 1.
    fn attempt(&self) -> u64;
}

macro_rules! attempts {
    ($($ty:ty),*) => {$(
        impl AttemptColumn for $ty {
            fn attempt(&self) -> u64 {
                u64::try_from(*self).unwrap_or(0)
            }
        }
    )*};
}

attempts!(i16, i32, i64, u16, u32, u64);

#[cfg(test)]
mod tests {
    use super::{AttemptColumn, KeyColumn};

    #[test]
    fn keys_lend_their_bytes() {
        assert_eq!(Box::<str>::from("k").key(), Some(b"k".as_slice()));
        assert_eq!(Some(vec![9_u8]).key(), Some([9].as_slice()));
    }

    #[test]
    fn attempts_never_read_below_zero() {
        assert_eq!(i64::MIN.attempt(), 0);
        assert_eq!(u64::MAX.attempt(), u64::MAX);
    }
}
