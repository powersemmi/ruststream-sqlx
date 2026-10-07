//! The headers layout: a headers struct describes the queue table, and the message struct a
//! handler takes flattens it into its `#[field(headers)]` field beside data of its own. The
//! delivery's header map holds the headers struct's fields without a role, built on the first
//! `headers()` call.

use std::fmt::Debug;
use std::io::Write;
#[cfg(any(feature = "chrono", feature = "time"))]
use std::io::{self, Cursor};
use std::sync::OnceLock;

#[cfg(feature = "chrono")]
use chrono::format::{Fixed, Item};
#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
use ruststream::{HeaderMap, Str};
#[cfg(feature = "time")]
use time::OffsetDateTime;
#[cfg(feature = "time")]
use time::format_description::well_known::Rfc3339;

use super::InboxSpec;
use super::database::QueueDatabase;
use super::engine::Events;
use super::spec::Declaration;

/// A struct that describes a queue table whose message is assembled from it: the mechanics a
/// subscription runs the queue by, and the service's own headers. `#[derive(InboxHeaders)]`
/// implements it.
///
/// The headers struct takes the table's attributes (`#[inbox(table, schema, advisory_lock,
/// clock, isolation, mode)]`) and the fields that play a role. Every field without a role is a
/// header: the delivery's header map holds it under its column's name, built the first time
/// something reads the delivery's headers, and a field that holds `None` is left out. Its type
/// implements [`HeaderField`]. The derive also writes the generated
/// [`insert`](crate::Insert::insert) of the table's columns.
///
/// The message struct derives [`Inbox`](crate::Inbox) and holds the headers struct in a
/// `#[field(headers)] #[sqlx(flatten)]` field beside its own data. A handler takes the message
/// struct itself, `&Message`, or `&[Message]` in a batch, as in row mode. Its rows are read
/// by the default fetch, which names the headers struct's columns and the message's own, or by
/// the message's own [`Fetch`](crate::Fetch) with `#[inbox(custom(fetch))]`, which may read the
/// data from other tables.
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
/// /// The queue table: the mechanics, and the service's own headers `tenant` and `trace`.
/// #[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
/// #[inbox(table = "order_jobs")]
/// pub struct OrderHeaders {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(attempt, generated)]
///     attempt: i16,
///     tenant: String,
///     trace: Option<String>,
/// }
///
/// /// The message a handler takes: the headers and the order's note, read from the same row.
/// #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
/// pub struct OrderJob {
///     #[field(headers)]
///     #[sqlx(flatten)]
///     headers: OrderHeaders,
///     note: Option<String>,
/// }
///
/// #[derive(Deserialize)]
/// struct Tenant {
///     tenant: String,
/// }
///
/// // The header map is built from `OrderHeaders` on the first read: here, by `Headers<Tenant>`.
/// #[subscriber(InboxQueue::<OrderJob>::new("orders"))]
/// async fn ship(job: &OrderJob, Headers(tenant): Headers<Tenant>) -> HandlerOutcome {
///     tracing::info!(tenant = %tenant.tenant, note = ?job.note, "shipping");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(ship);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a headers struct: a flattened `#[field(headers)]` field holds one",
    label = "this type does not derive `InboxHeaders`",
    note = "derive `InboxHeaders` for `{Self}` and describe the queue table on it, or drop \
            `#[sqlx(flatten)]` to read the field as a header column"
)]
pub trait InboxHeaders: Sized + Send + Sync + 'static {
    /// The type of the field that plays `id`. Machinery; the derive sets it.
    #[doc(hidden)]
    type Id: Clone + Debug + Send + Sync + 'static;

    /// The queue table's settings, as the markers of [`spec`](crate::spec): the mechanics and
    /// the header fields. Machinery; the derive sets it.
    #[doc(hidden)]
    type Settings: Declaration;

    /// The queue table: the mechanics and the headers, without the message's own columns, which
    /// the message's own description adds. Machinery; the derive sets it.
    #[doc(hidden)]
    const TABLE: InboxSpec<Self::Settings>;

    /// The headers the fields without a role hold, by their columns' names. Machinery; the
    /// derive sets it.
    #[doc(hidden)]
    const NAMES: &'static [&'static str];

    /// The row's id. Machinery; the derive writes it.
    #[doc(hidden)]
    fn id(&self) -> &Self::Id;

    /// The header map of the fields without a role. Machinery; the derive writes it.
    #[doc(hidden)]
    fn header_map(&self) -> HeaderMap;
}

/// A row whose delivery builds its header map from its fields: where the map comes from.
/// Machinery; every [`HeaderFields`](crate::HeaderFields) row implements it.
#[doc(hidden)]
pub trait Assembled {
    /// The header map of the headers struct's fields without a role.
    fn header_map(&self) -> HeaderMap;
}

/// A value a field of a headers struct holds, as the header the delivery's header map carries.
///
/// A field of a struct deriving [`InboxHeaders`] without a role becomes the header of its
/// column's name. Strings and byte vectors are their bytes, integers and `bool` their decimal
/// text, and `chrono::DateTime<Utc>` (feature `chrono`) and `time::OffsetDateTime` (feature
/// `time`) their RFC 3339 text. `None` leaves the header out. A service implements it for a type
/// of its own.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::HeaderField;
/// use ruststream_sqlx::prelude::*;
///
/// /// A region code the table keeps as text.
/// #[derive(Debug, Clone, sqlx::Type)]
/// #[sqlx(transparent)]
/// pub struct Region(String);
///
/// // The header carries the code in capitals, as the service's consumers read it.
/// impl HeaderField for Region {
///     fn header(&self) -> Option<Vec<u8>> {
///         Some(self.0.to_uppercase().into_bytes())
///     }
/// }
///
/// #[derive(Debug, Clone, InboxHeaders, sqlx::FromRow)]
/// #[inbox(table = "shipments")]
/// pub struct ShipmentHeaders {
///     #[field(id, generated)]
///     id: i64,
///     region: Region,
/// }
///
/// #[derive(Debug, Clone, Inbox, sqlx::FromRow)]
/// pub struct Shipment {
///     #[field(headers)]
///     #[sqlx(flatten)]
///     headers: ShipmentHeaders,
///     address: String,
/// }
///
/// #[subscriber(InboxQueue::<Shipment>::new("shipments"))]
/// async fn ship(shipment: &Shipment, ctx: &mut Context<'_>) -> HandlerOutcome {
///     let region = ctx.headers().get_str("region").unwrap_or("none");
///     tracing::info!(region, to = %shipment.address, "shipping");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: sqlx::PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(ship);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a header value: a field of a headers struct without a role becomes \
               a header",
    label = "a header field of this type",
    note = "implement `HeaderField` for `{Self}`, or give the field a role with `#[field(..)]`"
)]
pub trait HeaderField {
    /// The header's value; `None` leaves the header out.
    fn header(&self) -> Option<Vec<u8>>;
}

impl HeaderField for String {
    fn header(&self) -> Option<Vec<u8>> {
        Some(self.clone().into_bytes())
    }
}

impl HeaderField for Vec<u8> {
    fn header(&self) -> Option<Vec<u8>> {
        Some(self.clone())
    }
}

impl HeaderField for bool {
    fn header(&self) -> Option<Vec<u8>> {
        Some(if *self {
            b"true".to_vec()
        } else {
            b"false".to_vec()
        })
    }
}

/// The longest decimal text of a 64-bit integer: `i64::MIN` and `u64::MAX` take 20 bytes.
const DECIMAL_TEXT: usize = 20;

/// `HeaderField` for integers: the decimal text.
macro_rules! decimal {
    ($($integer:ty),*) => {
        $(
            impl HeaderField for $integer {
                fn header(&self) -> Option<Vec<u8>> {
                    // The text goes into a buffer of its own length: a `Vec` with spare capacity
                    // costs the map one more allocation as it turns into `Bytes`.
                    let mut text = [0_u8; DECIMAL_TEXT];
                    let mut rest = &mut text[..];
                    // The buffer holds the longest decimal of a 64-bit integer, so the write
                    // cannot run out of room.
                    write!(rest, "{self}").ok()?;
                    let written = DECIMAL_TEXT - rest.len();
                    Some(text[..written].to_vec())
                }
            }
        )*
    };
}

decimal!(i8, i16, i32, i64, u8, u16, u32, u64);

impl<T: HeaderField> HeaderField for Option<T> {
    fn header(&self) -> Option<Vec<u8>> {
        self.as_ref().and_then(HeaderField::header)
    }
}

/// The longest RFC 3339 text of a time: a six-digit year with its sign, nanoseconds and an offset.
#[cfg(any(feature = "chrono", feature = "time"))]
const TIME_TEXT: usize = 48;

/// The text `write` puts into a buffer on the stack, copied into a `Vec` of its own length: a
/// formatter's `String` keeps spare capacity, which costs the map one more allocation as the
/// value turns into `Bytes`. `None` where the text does not fit or the formatter fails.
#[cfg(any(feature = "chrono", feature = "time"))]
fn exact_text(
    write: impl FnOnce(&mut Cursor<[u8; TIME_TEXT]>) -> io::Result<()>,
) -> Option<Vec<u8>> {
    let mut text = Cursor::new([0_u8; TIME_TEXT]);
    write(&mut text).ok()?;
    let written = usize::try_from(text.position()).ok()?;
    Some(text.get_ref()[..written].to_vec())
}

#[cfg(feature = "chrono")]
impl HeaderField for DateTime<Utc> {
    fn header(&self) -> Option<Vec<u8>> {
        // The items `to_rfc3339` writes, into a buffer of the text's own length.
        let rfc3339 = [Item::Fixed(Fixed::RFC3339)];
        exact_text(|text| write!(text, "{}", self.format_with_items(rfc3339.iter())))
    }
}

#[cfg(feature = "time")]
impl HeaderField for OffsetDateTime {
    fn header(&self) -> Option<Vec<u8>> {
        exact_text(|text| {
            self.format_into(text, &Rfc3339)
                .map(drop)
                .map_err(io::Error::other)
        })
    }
}

/// Puts the header `name` holding `field` into `headers`, unless `field` holds none. Machinery;
/// the derive's header map calls it for each field without a role.
#[doc(hidden)]
pub(crate) fn put_header(headers: &mut HeaderMap, name: &'static str, field: &impl HeaderField) {
    if let Some(value) = field.header() {
        // A column's name is a constant: the map holds it as it is, where a `&str` copies.
        headers.insert(Str::from_static(name), value);
    }
}

/// The first of `headers` whose name is not among `names`: the headers a headers struct's fields
/// hold. Machinery.
#[must_use]
#[doc(hidden)]
pub(crate) fn unnamed_header<'h>(headers: &'h HeaderMap, names: &[&str]) -> Option<&'h str> {
    headers
        .iter()
        .map(|(name, _)| name)
        .find(|name| !names.iter().any(|known| known.eq_ignore_ascii_case(name)))
}

/// Where a delivery keeps its header map. Machinery: a flat struct's delivery holds the map it
/// moved out of the row's headers column at the claim; a message assembled from a headers struct
/// holds a cell the map is built into on the first read.
#[doc(hidden)]
pub trait HeaderCell<DB: QueueDatabase, Row: Events<DB>>: Default + Send + Sync + 'static {
    /// The cell of `row`, as the claim hands the row over.
    fn take(row: &mut Row) -> Self;

    /// Whether the cell holds nothing yet: a batch keeps no room for such cells.
    fn is_unset(&self) -> bool;

    /// The header map; `row` is the delivery's row, `None` for an id without a row or a row that
    /// did not decode.
    fn read<'c>(&'c self, row: Option<&Row>) -> &'c HeaderMap;
}

impl<DB: QueueDatabase, Row: Events<DB>> HeaderCell<DB, Row> for HeaderMap {
    fn take(row: &mut Row) -> Self {
        Row::take_headers(row)
    }

    fn is_unset(&self) -> bool {
        self.is_empty()
    }

    fn read<'c>(&'c self, _: Option<&Row>) -> &'c HeaderMap {
        self
    }
}

/// The header map of a message assembled from a headers struct, built on the first read.
/// Machinery.
// A `OnceLock`, not a `OnceCell`: a delivery is `Sync`. A read after the first is one atomic
// load.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct LazyHeaders(OnceLock<HeaderMap>);

impl<DB, Row> HeaderCell<DB, Row> for LazyHeaders
where
    DB: QueueDatabase,
    Row: Events<DB> + Assembled,
{
    fn take(_: &mut Row) -> Self {
        Self::default()
    }

    fn is_unset(&self) -> bool {
        self.0.get().is_none()
    }

    fn read<'c>(&'c self, row: Option<&Row>) -> &'c HeaderMap {
        self.built(row)
    }
}

impl LazyHeaders {
    /// The header map of `row`, built on the first call.
    fn built<Row: Assembled>(&self, row: Option<&Row>) -> &HeaderMap {
        // A delivery without a row reads an empty map, which allocates nothing.
        self.0
            .get_or_init(|| row.map_or_else(HeaderMap::new, Assembled::header_map))
    }
}

#[cfg(test)]
mod tests {
    //! The lazy header map: built from the row on the first read, once.

    use std::cell::Cell;

    use ruststream::HeaderMap;

    use super::{Assembled, HeaderField, LazyHeaders, put_header, unnamed_header};

    /// A header value that counts how often it was read into a header.
    struct Tenant {
        name: &'static str,
        reads: Cell<usize>,
    }

    impl HeaderField for Tenant {
        fn header(&self) -> Option<Vec<u8>> {
            self.reads.set(self.reads.get() + 1);
            Some(self.name.as_bytes().to_vec())
        }
    }

    /// A message whose header map holds its tenant, and its trace where it has one.
    struct Job {
        tenant: Tenant,
        trace: Option<String>,
    }

    impl Assembled for Job {
        fn header_map(&self) -> HeaderMap {
            let mut headers = HeaderMap::new();
            put_header(&mut headers, "tenant", &self.tenant);
            put_header(&mut headers, "trace", &self.trace);
            headers
        }
    }

    fn job(trace: Option<&str>) -> Job {
        Job {
            tenant: Tenant {
                name: "acme",
                reads: Cell::new(0),
            },
            trace: trace.map(str::to_owned),
        }
    }

    #[test]
    fn the_map_is_built_on_the_first_read_and_only_once() {
        let job = job(Some("t-1"));
        let cell = LazyHeaders::default();
        assert_eq!(job.tenant.reads.get(), 0, "nothing is built before a read");
        let headers = cell.built(Some(&job));
        assert_eq!(headers.get_str("tenant"), Some("acme"));
        assert_eq!(headers.get_str("trace"), Some("t-1"));
        assert_eq!(cell.built(Some(&job)).len(), 2);
        assert_eq!(job.tenant.reads.get(), 1, "the second read reuses the map");
    }

    #[test]
    fn a_field_holding_none_is_left_out_and_a_missing_row_reads_empty() {
        let job = job(None);
        let cell = LazyHeaders::default();
        let headers = cell.built(Some(&job));
        assert_eq!(headers.get_str("tenant"), Some("acme"));
        assert!(!headers.contains("trace"), "a NULL trace is no header");
        assert!(LazyHeaders::default().built(None::<&Job>).is_empty());
    }

    #[test]
    fn a_header_no_field_is_named_for_is_unfit() {
        let headers: HeaderMap = [("Tenant", "acme"), ("x-other", "1")].into_iter().collect();
        assert_eq!(
            unnamed_header(&headers, &["tenant", "trace"]),
            Some("x-other")
        );
        assert_eq!(unnamed_header(&headers, &["tenant", "x-other"]), None);
    }
}
