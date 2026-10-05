//! How the columns of a queue row reach a delivery: its headers, its key, its attempt.

#[cfg(feature = "json")]
use std::collections::BTreeMap;

use ruststream::HeaderMap;
#[cfg(feature = "json")]
use sqlx::types::Json;

/// The type of a field playing `headers`: what the delivery's header map holds.
///
/// `sqlx::types::Json<BTreeMap<String, String>>` implements it under the `json` feature (a
/// `jsonb` or `json` column of string values), and so does an `Option` of any implementation.
///
/// # Examples
///
/// ```
/// use ruststream::HeaderMap;
/// use ruststream_sqlx::HeaderColumn;
///
/// /// Headers stored one per line, `name: value`.
/// struct Lines(String);
///
/// impl HeaderColumn for Lines {
///     fn to_headers(&self) -> HeaderMap {
///         self.0
///             .lines()
///             .filter_map(|line| line.split_once(": "))
///             .map(|(name, value)| (name.to_owned(), value.to_owned()))
///             .collect()
///     }
///
///     fn from_headers(headers: &HeaderMap) -> Self {
///         let lines: Vec<String> = headers
///             .iter()
///             .map(|(name, value)| format!("{name}: {}", String::from_utf8_lossy(value)))
///             .collect();
///         Self(lines.join("\n"))
///     }
/// }
///
/// let column = Lines("x-tenant: acme".to_owned());
/// assert_eq!(column.to_headers().get_str("x-tenant"), Some("acme"));
/// ```
pub trait HeaderColumn {
    /// The headers the column holds.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")] {
    /// use std::collections::BTreeMap;
    ///
    /// use ruststream_sqlx::HeaderColumn;
    /// use sqlx::types::Json;
    ///
    /// let column = Json(BTreeMap::from([("x-tenant".to_owned(), "acme".to_owned())]));
    /// assert_eq!(column.to_headers().get_str("x-tenant"), Some("acme"));
    /// # }
    /// ```
    fn to_headers(&self) -> HeaderMap;

    /// The column value that holds `headers`, for a service's `Publish`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")] {
    /// use std::collections::BTreeMap;
    ///
    /// use ruststream::HeaderMap;
    /// use ruststream_sqlx::HeaderColumn;
    /// use sqlx::types::Json;
    ///
    /// let mut headers = HeaderMap::new();
    /// headers.insert("x-tenant", "acme");
    /// // What a `Publish` writes into a `jsonb` column.
    /// let column = Json::<BTreeMap<String, String>>::from_headers(&headers);
    /// assert_eq!(column.0["x-tenant"], "acme");
    /// # }
    /// ```
    #[must_use]
    fn from_headers(headers: &HeaderMap) -> Self;
}

impl<T: HeaderColumn> HeaderColumn for Option<T> {
    fn to_headers(&self) -> HeaderMap {
        self.as_ref()
            .map_or_else(HeaderMap::new, HeaderColumn::to_headers)
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        (!headers.is_empty()).then(|| T::from_headers(headers))
    }
}

/// A JSON object of strings. A value that is not UTF-8 is written with its invalid bytes
/// replaced, as `String::from_utf8_lossy` does.
#[cfg(feature = "json")]
impl HeaderColumn for Json<BTreeMap<String, String>> {
    fn to_headers(&self) -> HeaderMap {
        self.0
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        Self(
            headers
                .iter()
                .map(|(name, value)| (name.to_owned(), String::from_utf8_lossy(value).into_owned()))
                .collect(),
        )
    }
}

/// The type of a field playing `partition_key`: the bytes the delivery's key lends.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::KeyColumn;
///
/// // Deliveries of one customer keep their order under `workers(n, by_key)`.
/// let customer = String::from("acme");
/// assert_eq!(customer.key(), Some(b"acme".as_slice()));
/// let none: Option<String> = None;
/// assert_eq!(none.key(), None);
/// ```
pub trait KeyColumn {
    /// The key's bytes, or `None` when the row carries no key.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx::KeyColumn;
    ///
    /// let account: Vec<u8> = vec![1, 2];
    /// assert_eq!(account.key(), Some([1, 2].as_slice()));
    /// ```
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
/// use ruststream_sqlx::AttemptColumn;
///
/// // Postgres has no unsigned integers in sqlx, so `attempt` is an `i16` or an `i32` there.
/// assert_eq!(3_i16.attempt(), 3);
/// assert_eq!((-1_i32).attempt(), 0);
/// ```
pub trait AttemptColumn {
    /// The attempt, counting the first delivery as 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx::AttemptColumn;
    ///
    /// assert_eq!(7_u32.attempt(), 7);
    /// ```
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
