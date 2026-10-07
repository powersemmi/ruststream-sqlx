//! The lazy header map: built from the row on the first read, once.

use std::cell::Cell;

#[cfg(feature = "chrono")]
use chrono::{DateTime, TimeZone, Utc};
#[cfg(feature = "time")]
use time::OffsetDateTime;
#[cfg(feature = "time")]
use time::format_description::well_known::Rfc3339;

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

#[test]
fn an_integer_header_is_its_decimal_text_at_every_width() {
    assert_eq!(
        i64::MIN.header().as_deref(),
        Some(&b"-9223372036854775808"[..])
    );
    assert_eq!(
        u64::MAX.header().as_deref(),
        Some(&b"18446744073709551615"[..])
    );
    assert_eq!(0_u8.header().as_deref(), Some(&b"0"[..]));
    assert_eq!((-7_i16).header().as_deref(), Some(&b"-7"[..]));
}

/// The value a header field writes, checked to hold no spare capacity: a `Vec` with room left
/// costs the map one more allocation as it turns into `Bytes`.
#[cfg(any(feature = "chrono", feature = "time"))]
fn exact(field: &impl HeaderField) -> Vec<u8> {
    let value = field.header().expect("a time is always a header");
    assert_eq!(
        value.capacity(),
        value.len(),
        "the header's text has spare capacity"
    );
    value
}

#[cfg(feature = "chrono")]
#[test]
fn a_chrono_time_header_is_its_rfc_3339_text_with_no_spare_room() {
    let times: [DateTime<Utc>; 3] = [
        Utc.with_ymd_and_hms(2026, 10, 7, 9, 30, 0).unwrap(),
        DateTime::from_timestamp(1_791_349_000, 123_456_789).unwrap(),
        DateTime::from_timestamp(59, 1_500_000_000).unwrap(),
    ];
    for time in times {
        assert_eq!(exact(&time), time.to_rfc3339().into_bytes(), "{time:?}");
    }
}

#[cfg(feature = "time")]
#[test]
fn a_time_header_is_its_rfc_3339_text_with_no_spare_room() {
    let times = [
        OffsetDateTime::from_unix_timestamp(1_791_349_000).unwrap(),
        OffsetDateTime::from_unix_timestamp_nanos(1_791_349_000_123_456_789).unwrap(),
    ];
    for time in times {
        assert_eq!(
            exact(&time),
            time.format(&Rfc3339).unwrap().into_bytes(),
            "{time:?}"
        );
    }
}
