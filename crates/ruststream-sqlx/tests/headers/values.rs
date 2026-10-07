//! The text a header field writes for a value of each type: an integer's decimal text and a
//! time's RFC 3339 text, each without spare capacity.

use chrono::{DateTime, TimeZone, Utc};
use ruststream_sqlx::HeaderField;
#[cfg(feature = "time")]
use time::OffsetDateTime;
#[cfg(feature = "time")]
use time::format_description::well_known::Rfc3339;

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
fn exact(field: &impl HeaderField) -> Vec<u8> {
    let value = field.header().expect("a time is always a header");
    assert_eq!(
        value.capacity(),
        value.len(),
        "the header's text has spare capacity"
    );
    value
}

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
