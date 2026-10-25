//! Transactional mode: the delivery's transaction lent to its handler, in every form and on every
//! stand.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod advisory_session;
pub(crate) mod job;
mod lease_in_work;
mod writes;

use job::{JOB, Job, LEASE, POLL, audit};
