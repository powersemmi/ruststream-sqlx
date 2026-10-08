# Statements checked at compile time

```
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use ruststream_sqlx::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;

// email_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL,
// attempt SMALLINT NOT NULL DEFAULT 1, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs", checked, db = postgres)]
pub struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
struct Email {
    to: String,
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(email: &Email) -> HandlerOutcome {
    tracing::info!(to = %email.to, "sending");
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(send);
    })
}
# }
# fn main() {}
```

`checked` puts the statements the derive generates into sqlx's compile-time check. The derive
builds them with the dialect `db` names, the code the broker runs when a subscription opens, and
wraps each in `sqlx::query!` inside a function nothing calls. The service's build then checks each
statement against its database: a column the struct names and the table lacks fails the build, and
`cargo sqlx prepare --check` fails in CI. The subscription runs the statements it runs without
`checked`, so a message costs the same.

## What it needs

- The service's own sqlx with its `macros` feature and the database's driver, under the name
  `sqlx`: `sqlx = { version = "0.9", features = ["macros", "postgres"] }`.
- `DATABASE_URL` at build time, or the `.sqlx` data that `cargo sqlx prepare` (`sqlx-cli` 0.9)
  writes, committed beside the crate. `SQLX_OFFLINE=true` makes a build read that data even where
  `DATABASE_URL` is set.
- The feature of the dialect `db` names on `ruststream-sqlx`: `postgres`, `mysql` or `sqlite`.

Each statement meets sqlx's own rules. A parameter takes the Rust type sqlx picks for its column,
so with both `chrono` and `time` on, `sqlx.toml` names the preferred crate; a column of a type sqlx
does not know takes a type override there.

## What it covers

The statements the struct determines: the claim, and the claim by role a by-name subscription runs
for a struct that writes no event itself; the guard of a FIFO group; the fetch after a claim of the
service's own; the acknowledgement, the retry, the delayed retry and the discard; the dead letter of
a group; the lease's extension and stamp; the advisory lock form's lock, unlock and take; the
generated insert. A statement the service writes itself is its own code, which `sqlx::query!`
checks the same way.

The startup check still runs: each statement is prepared when its subscription opens. It also holds
what the struct does not determine: the move into a dead-letter table, which the registration names,
and the opening of a transaction.

## The headers layout, checked

```
# #[cfg(all(feature = "inbox", feature = "postgres", feature = "chrono"))]
# mod demo {
use chrono::{DateTime, Utc};
use ruststream_sqlx::Fetch;
use ruststream_sqlx::prelude::*;
use sqlx::{Error, FromRow, PgConnection, Postgres};

// order_jobs: job_id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, processed_at TIMESTAMPTZ,
// trace TEXT, total BIGINT NOT NULL
#[derive(Debug, Clone, InboxHeaders, FromRow)]
#[inbox(table = "order_jobs", checked, db = postgres)]
pub struct OrderHeaders {
    #[field(id, generated)]
    pub job_id: i64,
    #[field(group)]
    pub name: String,
    #[field(processed_at)]
    pub processed_at: Option<DateTime<Utc>>,
    pub trace: Option<String>,
}

#[derive(Debug, Clone, Inbox, FromRow)]
#[inbox(custom(fetch))]
pub struct Order {
    #[field(headers)]
    #[sqlx(flatten)]
    pub headers: OrderHeaders,
    pub total: i64,
}

impl Fetch<Postgres> for Order {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, Error> {
        let rows = sqlx::query!(
            "SELECT job_id, name, processed_at, trace, total FROM order_jobs \
             WHERE job_id = ANY($1)",
            ids
        )
        .fetch_all(conn)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self {
                headers: OrderHeaders {
                    job_id: row.job_id,
                    name: row.name,
                    processed_at: row.processed_at,
                    trace: row.trace,
                },
                total: row.total,
            })
            .collect())
    }
}
# }
# fn main() {}
```

`checked` goes on the headers struct, which describes the table. Its statements claim ids alone,
so the message lists `fetch` in `custom(..)` and reads its rows in a `Fetch` of the service's own,
where `sqlx::query!` checks that statement too. A message that leaves the fetch to the crate does
not build. The headers struct cannot see the message's `custom(..)`, so it checks the crate's
statement of every other event, one the message writes itself included.

## What it refuses

A struct that `checked` cannot cover does not compile, and the error points at the attribute or
the field: `checked` without `db` or `db` without `checked`; a `db` that names no built-in dialect,
since a procedural macro cannot run a dialect of the service's own (its tables keep the startup
check); a `db` whose feature is off; a `#[sqlx(flatten)]` field, whose struct's columns the derive
cannot see; a clock that is a parameter of the struct; the row lock form on SQLite; `checked` on a
message assembled from a headers struct.
