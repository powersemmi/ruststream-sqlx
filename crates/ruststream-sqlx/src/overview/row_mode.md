# Row mode

```no_run
# #[cfg(feature = "postgres")]
# mod demo {
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres};

// email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT, attempt SMALLINT DEFAULT 1, "to" TEXT,
// subject TEXT NULL, attachments JSONB
/// One mail to send: the table has no payload column, so a handler takes the row itself.
#[derive(Inbox, sqlx::FromRow, Clone)]
#[inbox(table = "email_jobs")]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    to: String,
    subject: Option<String>,
    #[sqlx(json)]
    attachments: Vec<Attachment>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Attachment {
    file: String,
}

# async fn deliver(_: &SendEmail) -> bool { true }
#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &SendEmail) -> HandlerOutcome {
    if deliver(email).await {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

/// One SMTP session per claim of up to 64 rows.
#[subscriber(InboxQueue::<SendEmail>::new("newsletters"))]
async fn send_newsletter(emails: &[SendEmail]) -> HandlerOutcome {
    tracing::info!(count = emails.len(), "sent a newsletter page");
    HandlerOutcome::ack()
}

/// The audit row commits with the acknowledgement of the mail's row, or neither does.
#[subscriber(InboxQueue::<SendEmail>::new("receipts"))]
async fn send_receipt(email: &SendEmail, Ctx(mut tx): Ctx<keys::Tx<Postgres>>) -> HandlerOutcome {
    let logged = sqlx::query("INSERT INTO sent_mail (job_id, recipient) VALUES ($1, $2)")
        .bind(email.job_id)
        .bind(&email.to)
        .execute(&mut *tx)
        .await;
    if logged.is_ok() && deliver(email).await {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
    }
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send);
        b.include(send_newsletter.batch(nonzero!(64)));
        b.include(send_receipt.transactional());
    })
}
# }
# fn main() {}
```

By hand, the table leaves `.payload(..)` out, names its own columns with `.data(..)`, and writes the
core's carried lane for its row. Its [`Insert`] writes a task with the text [`dialect::insert`]
renders from the description, as the derive's does:

```no_run
# #[cfg(feature = "postgres")]
# mod demo {
use ruststream::runtime::{Input, SoloCarried};
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::dialect::insert::{self, Sql};
use ruststream_sqlx::prelude::*;
use ruststream_sqlx::spec::Attempt;
use ruststream_sqlx::{AttemptRow, InboxSpec, InboxTable};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};

#[derive(sqlx::FromRow, Clone)]
pub struct SendEmail {
    job_id: i64,
    name: String,
    attempt: i16,
    to: String,
    subject: Option<String>,
    #[sqlx(json)]
    attachments: Vec<Attachment>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Attachment {
    file: String,
}

impl InboxTable for SendEmail {
    type Id = i64;
    type Table = InboxSpec<(Attempt,)>;
    const TABLE: Self::Table = InboxSpec::new("email_jobs", Column::new("job_id").generated())
        .group(Column::new("name"))
        .attempt(Column::new("attempt").generated())
        .data(&[Column::new("to"), Column::new("subject"), Column::new("attachments")]);

    fn id(&self) -> &i64 {
        &self.job_id
    }
}

// Row mode: a handler takes the row itself.
impl Input for SendEmail {
    type Axis = SoloCarried<Self>;
}

impl AttemptRow for SendEmail {
    type Attempt = i16;

    fn attempt(&self) -> &i16 {
        &self.attempt
    }
}

// INSERT INTO "email_jobs" ("name", "to", "subject", "attachments") VALUES ($1, $2, $3, $4)
const INSERT: Sql<128> = insert::postgres(&SendEmail::TABLE.spec());

impl Insert<PgConnection> for SendEmail {
    async fn insert(&self, conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        sqlx::query(INSERT.as_str())
            .bind(&self.name)
            .bind(&self.to)
            .bind(&self.subject)
            .bind(sqlx::types::Json(&self.attachments))
            .execute(conn)
            .await?;
        Ok(())
    }
}
# async fn deliver(_: &SendEmail) -> bool { true }
# #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
# async fn send(email: &SendEmail) -> HandlerOutcome {
#     if deliver(email).await { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
# }
# pub fn app(pool: PgPool) -> RustStream {
#     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
#         b.include(send);
#     })
# }
# }
# fn main() {}
```

A table whose struct has no `#[field(payload)]` field, or whose description sets no `.payload(..)`,
is in row mode. Its handler takes the struct itself, `&SendEmail`, as the driver read it. The struct
derives `Clone`, because the test harness keeps a copy of each value. It does not derive
`Deserialize`: a type that deserializes rides the codec, and [`Inbox`] shows the error. Serde
appears only where sqlx needs it, such as the `#[sqlx(json)]` column above.

A delivery lends its handler the row the driver read, with no codec and no copy. A batch handler
takes `&[SendEmail]`: the rows of one claim, lent as one slice in claim order ([`RowBatch`]). A
batch handler mounts in the plain form, with no reply and no `Out` slots. A handler of one row
mounts in every form the core offers, a reply and `Out` slots included.

The column types are the service's to get right, as in payload mode. A row the driver cannot read
settles by the subscription's decode policy, `on_failure(decode = ..)`, and the driver's error
goes to the log. A claimed id whose row is gone settles the same way. A nullable column takes
`Option<T>`: SQLite's driver reads a NULL text column into a `String` as an empty string.

Every form serves row mode: the row lock, the lease and the advisory lock. Transactional mode
serves it too, and the handler takes `&SendEmail` beside `Ctx<keys::Tx<DB>>`.

The `headers` column becomes the delivery's headers, which middleware reads. The row a handler
reads holds that column empty. A message assembled from a headers struct builds its delivery's
headers from that struct instead ([the headers layout](#the-headers-layout)).

A task is written by [`insert`](Insert::insert), in the service's own transaction or in the
handler's: the derive generates it, and a table described by hand writes it over the text
[`dialect::insert`] renders from its description. The columns bind in the description's order: the
roles, then the data columns. [`Repository`] writes a row-mode table through a [`Publish`] of the
service's own. Routes and by-name subscriptions read payload-mode tables: they choose a table by a
name at run time, and a row type is chosen at compile time.

A handler that decodes a payload, mounted on a row-mode table, fails each delivery by the decode
policy. The subscription logs one warning that names the table, row mode and the row type the
handler should take.

In a test, `with_value(&row)` compares the row a handler took, and
`received_values::<SendEmail>()` returns every row the subscription lent. The generated
`AsyncAPI` document describes the row through its `JsonSchema` derive
(`ruststream::schemars::JsonSchema`), as it describes any value a broker lends.
