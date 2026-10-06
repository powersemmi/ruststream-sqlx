use ruststream_sqlx::prelude::*;
use sqlx::{PgPool, Postgres};

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
struct Job {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(serde::Deserialize)]
struct Task {
    n: i64,
}

/// A handler that writes through the delivery's transaction.
#[subscriber(InboxQueue::<Job>::new("jobs"))]
async fn record(task: &Task, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
    let written = sqlx::query("INSERT INTO audit (job_id) VALUES ($1)")
        .bind(task.n)
        .execute(&mut *tx)
        .await;
    if written.is_ok() { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
}

/// A handler of a subscription by name.
#[subscriber("jobs")]
async fn by_name(task: &Task) -> HandlerOutcome {
    let _ = task.n;
    HandlerOutcome::ack()
}

fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("ui", "0.0.0")).with_broker(SqlxBroker::new(pool), |b| {
        // Mounted without transactional mode, the delivery lends no transaction.
        b.include(record);
        // A subscription by name has no transactional mode.
        b.include(by_name.transactional());
    })
}

fn main() {
    let _ = app;
}
