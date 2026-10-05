//! The one glob a service on the inbox imports: the framework's prelude and the inbox's own names.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "postgres")]
//! # mod demo {
//! use ruststream_sqlx::prelude::*;
//! use sqlx::PgPool;
//!
//! #[derive(Inbox, sqlx::FromRow)]
//! #[inbox(table = "jobs")]
//! pub struct Job {
//!     #[field(id)]
//!     id: i64,
//!     #[field(attempt)]
//!     attempt: i16,
//!     #[field(payload)]
//!     payload: Vec<u8>,
//! }
//!
//! #[derive(serde::Deserialize)]
//! struct Task {
//!     n: u32,
//! }
//!
//! #[subscriber(InboxQueue::<Job>::new("tasks"))]
//! async fn run(task: &Task, Ctx(attempt): Ctx<keys::Attempt>) -> HandlerOutcome {
//!     tracing::info!(task.n, ?attempt, "running");
//!     HandlerOutcome::ack()
//! }
//!
//! pub fn app(pool: PgPool) -> RustStream {
//!     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
//!         b.include(run);
//!     })
//! }
//! # }
//! # fn main() {}
//! ```

pub use ruststream::prelude::*;

pub use crate::{Inbox, InboxQueue, Insert, Publish, Repository, Routed, SqlxBroker, keys};
