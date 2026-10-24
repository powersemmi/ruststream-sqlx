//! The integration suite, one test binary: each file or directory under `tests/` is a module
//! of it.

// Every suite that reaches a stand serves the inbox.
#[cfg(feature = "inbox")]
mod live;

mod conformance;
mod databases;
mod delivery;
mod describe;
mod failures;
mod forms;
mod headers;
mod isolation;
mod manual;
mod names;
mod outbox;
mod retries;
mod row_mode;
mod threads;
mod transactional;
mod ui;
mod wake;
