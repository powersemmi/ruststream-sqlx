
A handler of rows, mounted on a table in payload mode, does not compile. The error names the
subscription and the row type its deliveries do not carry:

```compile_fail,E0277
use ruststream_sqlx::prelude::*;
use sqlx::PgPool;

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
pub struct Job {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Inbox, sqlx::FromRow, Clone)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    to: String,
}

// `jobs` holds payloads, not `SendEmail` rows.
#[subscriber(InboxQueue::<Job>::new("emails"))]
async fn send(email: &SendEmail) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send);
    })
}
```

A batch handler of rows with a reply does not compile either. The error notes that such a batch
mounts in the plain form only:

```compile_fail,E0277
use ruststream_sqlx::prelude::*;
use serde::Serialize;
use sqlx::PgPool;

#[derive(Inbox, sqlx::FromRow, Clone)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    to: String,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "sent")]
pub struct Sent {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"), reply)]
async fn send_all(emails: &[SendEmail]) -> Vec<Sent> {
    emails.iter().map(|email| Sent { to: email.to.clone() }).collect()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send_all.batch(nonzero!(64)));
    })
}
```
