//! The job, the audit statement and the timings the transactional and isolation suites share.

use std::time::Duration;

use ruststream::Outgoing;
use serde::{Deserialize, Serialize};
use sqlx::AssertSqlSafe;

pub(crate) const POLL: Duration = Duration::from_millis(20);

/// The lease of the suite's lease tables: short, so a row whose lease the broker stopped extending
/// returns while the test waits.
pub(crate) const LEASE: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
pub(crate) struct Job {
    pub(crate) n: i64,
}

pub(crate) const JOB: Job = Job { n: 7 };

/// The audit row of `job` noting `note`, written as text so one statement serves every stand.
pub(crate) fn audit(job: &Job, note: &str) -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        "INSERT INTO audit (job_id, note) VALUES ({}, '{note}')",
        job.n
    ))
}
