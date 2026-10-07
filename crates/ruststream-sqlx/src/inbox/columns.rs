//! How the columns of a queue row reach a delivery: its key, its attempt.

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
