# Startup checks and rows that do not decode

```no_run
# #[cfg(all(feature = "postgres", feature = "chrono"))]
# mod demo {
use std::time::Duration;

use chrono::{DateTime, Utc};
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
    #[field(retry_after, generated)]
    retry_after: DateTime<Utc>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    // A row that does not decode stays in the table and comes back every ten minutes, until a
    // release that reads it ships.
    let keep = FailurePolicy::RetryAfter(Duration::from_secs(600));
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send.on_failure(FailurePolicies::default().with_decode(keep)));
    })
}
# }
# fn main() {}
```

A mistake stops the service as early as it can be seen. A struct that cannot drive a queue does
not compile, and [`Inbox`] lists the errors. A subscription builds its statements when it opens
and prepares each one on the server: a table or a column the struct names and the database lacks
stops it, and [`SqlxBrokerError::Schema`] names the table and the statement. Preparing checks the
names, not the column types: the types are the service's to get right. A retry declaration the
table cannot carry stops it too ([`SqlxBrokerError::Declaration`]): `max_attempts(..)` and
`dead_letter(..)` come together, and the cap needs an `attempt` column. On MySQL and MariaDB the
subscription also reads the server's version.

A claimed row whose columns do not decode into the struct reaches the subscription's
`on_failure(decode = ..)` policy, which settles it, and the subscription goes on with the next
row. The default policy drops the row: it is deleted, or marked where the table has
`processed_at`. Such a row still reports its attempt: the claim reads the `attempt` column alone,
as the struct reads it. So `max_attempts(..)` spends it like any other row, and a policy that
retries it keeps it only up to the cap. When the `attempt` column itself does not decode, the row
reports no attempt, and a policy that retries it keeps it in the queue until it decodes. The
policy settles such a row before the handler runs, whatever the handler takes, a `Deserialized`
type included. It settles the same way a claimed id that the service's own fetch returned no row
for. A row whose id does not decode fails the claim, and the error names the subscription and the
table; every claim that reaches the row fails the same way until the row is fixed.

