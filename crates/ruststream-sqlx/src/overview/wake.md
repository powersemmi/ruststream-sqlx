# Waking a subscription

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres};

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs")]
pub struct Job {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Publish<Postgres> for Job {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Deserialize)]
pub struct Signup {
    email: String,
}

#[derive(Serialize, Deserialize, Outgoing)]
#[outgoing(name = "welcome")]
pub struct Welcome {
    email: String,
}

// Another service writes a signup and announces it: `SELECT pg_notify('jobs', 'signups')`.
#[subscriber(InboxQueue::<Job>::new("signups"), reply)]
async fn greet(signup: &Signup) -> Welcome {
    Welcome {
        email: signup.email.clone(),
    }
}

// The reply's row wakes this subscription in the same process: no poll interval in between.
#[subscriber(InboxQueue::<Job>::new("welcome"))]
async fn send(welcome: &Welcome) -> HandlerOutcome {
    tracing::info!(to = %welcome.email, "sending the welcome");
    HandlerOutcome::ack()
}

pub fn app(pool: PgPool) -> RustStream {
    // The poll each minute reaches a row whose notification was lost.
    let broker = SqlxBroker::new(pool)
        .poll_interval(Duration::from_secs(60))
        .listen_notify();
    RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(broker, |b| {
        b.include(greet).out_reply(Repository::<Job>::default());
        b.include(send);
    })
}
# }
# fn main() {}
```

A subscription claims rows as long as its claims come back full. After a claim that found fewer
rows than it asked for, it waits the poll interval: one second unless the broker's
[`poll_interval`](SqlxBroker::poll_interval) or the subscription's own
[`poll_interval`](InboxQueue::poll_interval) sets another. Two things end the wait early: a
publish from the same process, and a Postgres notification.

## A publish from the same process

A publish through the broker wakes the subscriptions of its table on the same broker. That covers
a [`Repository`], a route, a reply or an `Out` slot published through either of them, and a
publish in a test through the harness. In a table with groups, a publish wakes the subscriptions
of its own group alone, and the others keep their interval. In a table without groups it wakes
every subscription of the table. A subscription busy with a claim keeps the wake-up for later, so
its next wait returns at once.

A row the service writes itself wakes nobody: an [`insert`](Insert::insert), a statement of the
service's own, a write through a handler's [`Tx`]. Such a row waits for the poll interval, or for a
notification the service sends.

The wake-up costs a publish one atomic operation per subscription it wakes. A subscription pays
nothing per message for it.

## `LISTEN/NOTIFY` on Postgres

[`listen_notify`](SqlxBroker::listen_notify) turns notifications on for a broker on Postgres. The
broker listens on one connection of the pool, taken at `connect` and kept until `shutdown`. Each
subscription listens on its table's channel before its first claim. The channel is the table's
name, qualified with its schema where the table has one (`app.jobs`). The payload is the
group. A notification with an empty payload wakes every subscription of the table.

Each publish of the broker announces its row: `SELECT pg_notify(channel, group)` runs on the
connection that wrote the row, right after the write. A subscription in another process therefore
claims the row once it is visible. A notification that fails is logged as a warning, and the
publish still succeeds: the row is written, and the poll interval reaches it.

A writer outside RustStream sends the same notification itself, in its transaction or from a
trigger:

```sql
CREATE FUNCTION announce_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, NEW.name);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_announced AFTER INSERT ON jobs
    FOR EACH ROW EXECUTE FUNCTION announce_job();
```

Postgres delivers a notification once its transaction commits. A notification sent while the
listening connection was lost is gone: the broker connects again and wakes every subscription
once. The poll interval stays the floor under every wake-up.

Notifications have a price, and the switch is off unless set:

- the broker keeps one connection of the pool for its whole life, so the pool needs one more;
- each publish runs one more statement, `pg_notify`, on its connection;
- a transaction that notifies takes one lock, global to the server, at commit, so commits of
  notifying transactions run one after another, which limits many concurrent writers.

Postgres keeps channel names up to 63 bytes. A subscription to a table whose qualified name is
longer stops at startup with [`SqlxBrokerError::Declaration`], which names the table. On another
database `listen_notify` does not compile: [`Notifies`] names the fix.
