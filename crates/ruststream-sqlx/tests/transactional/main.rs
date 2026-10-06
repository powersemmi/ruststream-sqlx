//! Transactional mode and the isolation a table opens at: the delivery's transaction lent to its
//! handler, in every form and on every stand, and the level and the mode a table's transactions
//! open at as its struct declares.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

#[path = "../live/mod.rs"]
mod live;

mod advisory_session;
mod lease_in_work;
mod openings;
mod repeatable_read;
mod sqlite_modes;
mod writes;

use std::time::Duration;

use ruststream::Outgoing;
use serde::{Deserialize, Serialize};
use sqlx::AssertSqlSafe;

const POLL: Duration = Duration::from_millis(20);

/// The lease of the suite's lease tables: short, so a row whose lease the broker stopped extending
/// returns while the test waits.
const LEASE: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Job {
    n: i64,
}

const JOB: Job = Job { n: 7 };

/// The audit row of `job` noting `note`, written as text so one statement serves every stand.
fn audit(job: &Job, note: &str) -> AssertSqlSafe<String> {
    AssertSqlSafe(format!(
        "INSERT INTO audit (job_id, note) VALUES ({}, '{note}')",
        job.n
    ))
}
