//! The level and the mode a table's transactions open at, as its struct declares.

#![cfg(all(
    feature = "inbox",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

use crate::transactional::job;

mod openings;
mod repeatable_read;
mod sqlite_modes;

use job::{JOB, Job, LEASE, POLL, audit};
