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
