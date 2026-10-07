# Batches

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Email {
    to: String,
}

# async fn deliver(email: &Email) -> bool { !email.to.is_empty() }
// One claim is the batch, and each email settles its own row.
#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send_all(emails: &[Email]) -> Vec<HandlerOutcome> {
    let mut outcomes = Vec::with_capacity(emails.len());
    for email in emails {
        outcomes.push(if deliver(email).await {
            HandlerOutcome::ack()
        } else {
            HandlerOutcome::retry()
        });
    }
    outcomes
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send_all.batch(nonzero!(50)));
    })
}
# }
# fn main() {}
```

A batch is one claim: up to `batch(n)` rows of the queue. A row that does not decode is settled by
the decode policy, and the batch keeps its other rows. On SQLite the rows of a batch come in no
particular order.

In the row lock form the rows of a batch share the claim's transaction, which holds one
connection for the whole batch. Each delivery settles its own row in that transaction, and the
settlements take effect together, when the last delivery finishes. A settlement whose statement
fails rolls the whole batch back, its rows return, and the later settlements fail with
[`SqlxBrokerError::BatchRolledBack`]. A batch that holds a row whose statement always fails
therefore rolls back and returns all of its rows on every attempt, until the service's SQL or
schema is fixed.

In the lease form each delivery settles on its own, on a connection it takes for the settlement.

In the advisory lock form each delivery of a batch holds a connection of its own and settles on
it, on its own. A batch holds one row per key, and it ends where the pool has no connection to
spare.

