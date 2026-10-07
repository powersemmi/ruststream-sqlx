# Transactional mode

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use std::error::Error;

use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres};

// signup_jobs: id BIGSERIAL PRIMARY KEY, attempt SMALLINT NOT NULL DEFAULT 1,
// payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "signup_jobs")]
pub struct OpenAccount {
    #[field(id, generated)]
    id: i64,
    #[field(attempt, generated)]
    attempt: i16,
    #[field(payload)]
    payload: Vec<u8>,
}

// welcome_jobs, the mailer's queue: id BIGSERIAL PRIMARY KEY, payload BYTEA NOT NULL
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "welcome_jobs")]
pub struct SendWelcome {
    #[field(id, generated)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Signup {
    email: String,
}

#[derive(Serialize)]
pub struct Welcome<'a> {
    to: &'a str,
}

// The account and its welcome email, written on the connection of the delivery's transaction.
async fn open(
    conn: &mut PgConnection,
    signup: &Signup,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    sqlx::query("INSERT INTO accounts (email) VALUES ($1)")
        .bind(&signup.email)
        .execute(&mut *conn)
        .await?;
    let welcome = SendWelcome {
        id: 0,
        payload: serde_json::to_vec(&Welcome { to: &signup.email })?,
    };
    welcome.insert(conn).await?;
    Ok(())
}

#[subscriber(InboxQueue::<OpenAccount>::new("signups"))]
async fn open_account(
    signup: &Signup,
    Ctx(mut tx): Ctx<keys::Tx<Postgres>>,
    Ctx(pool): Ctx<keys::Pool<Postgres>>,
    Ctx(attempt): Ctx<keys::Attempt>,
) -> HandlerOutcome {
    if let Err(error) = open(&mut *tx, signup).await {
        // Through the pool: the note stays when the retry rolls the transaction back.
        let _ = sqlx::query("INSERT INTO signup_errors (email, error) VALUES ($1, $2)")
            .bind(&signup.email)
            .bind(format!("attempt {}: {error}", attempt.unwrap_or(1)))
            .execute(&pool)
            .await;
        return HandlerOutcome::retry();
    }
    // The commit keeps the account and its welcome email, and finishes the signup.
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    RustStream::new(AppInfo::new("accounts", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
        b.include(open_account.transactional());
    })
}
# }
# fn main() {}
```

`.transactional()` at the mount site switches an [`InboxQueue`] subscription to transactional
mode, where the handler writes through its delivery's transaction. The handler takes that
transaction as its first `Ctx` parameter, `Ctx(mut tx): Ctx<keys::Tx<Postgres>>`, and runs its
statements through `&mut *tx`. A handler that takes `keys::Tx` on a subscription without the step
does not compile, and the error names the step.

Acknowledgement commits the handler's writes together with the row's settlement. Every other
outcome rolls them back first, then settles the row as it would without the step. A handler that
panics while it holds `tx` commits none of its writes, even where its panic policy acknowledges
the delivery. A delivery dropped unsettled closes its connection, and the server rolls its
transaction back.

A task the handler writes through `tx` into a queue table, with [`Insert`] or its own SQL, commits
with the acknowledgement too. A write through the pool that `Ctx<keys::Pool<DB>>` gives commits on
its own, whatever the outcome. So does a publish through the broker.

A handler's first `Ctx` decides which of the inbox's keys compile after it: the other two after
`keys::Tx`, only `keys::Attempt` after `keys::Pool`, none after `keys::Attempt`. The order
`keys::Tx`, `keys::Pool`, `keys::Attempt` fits every handler, with the keys it does not need left
out.

What the transaction is depends on the form:

- In the row lock form it is the claim's transaction. The subscription sets a savepoint right
  after each claim, one more statement per delivery. A settlement other than acknowledgement rolls
  back to it, so a retry discards the handler's writes and still counts the attempt.
- In the lease form the crate opens a transaction for each delivery once its claim has committed.
  Acknowledgement runs in it and takes effect only while the row still holds the delivery's lease.
  A lost lease rolls the whole transaction back, and the settlement returns
  [`SqlxBrokerError::LeaseLost`].
- In the advisory lock form the crate opens the transaction on the connection that holds the row's
  key, after the take. The settlement ends the transaction before it releases the lock.

Each of these transactions opens at the table's isolation level or SQLite mode
([Isolation and mode](#isolation-and-mode)). On SQLite a delivery's transaction takes the
database's write lock at its first write, or at its start under `mode = immediate` or
`exclusive`, and holds it until the delivery settles. It keeps every other writer of the database
out meanwhile, claims included.

Each delivery in work holds one connection of the pool for its transaction, in every form. A
handler that takes a second connection, through `keys::Pool` or a publish, needs a pool larger
than its `workers(n)`: a pool without that room makes it wait for the pool's `acquire_timeout`.
Transactional mode serves single deliveries: a batch handler mounted with `.transactional()` does
not compile.

The transaction goes back to the delivery when the handler's `Tx` drops. A settlement that finds
it still out, in a task the handler moved it into, returns [`SqlxBrokerError::TransactionHeld`],
and the transaction rolls back when that `Tx` drops.

On MySQL and MariaDB the settlement after a handler that cut a statement through `tx` short, as a
`select!` around a query does, can read that statement's reply as its own, a bug of sqlx-mysql
0.9.0.

