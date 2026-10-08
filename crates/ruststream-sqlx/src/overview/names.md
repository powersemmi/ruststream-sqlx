# Routes and by-name subscriptions

```no_run
# #[cfg(all(feature = "inbox", feature = "postgres"))]
# mod demo {
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

#[derive(Serialize, Outgoing)]
#[outgoing(name = "welcome")]
pub struct Welcome {
    email: String,
}

#[subscriber("signups", reply)]
async fn greet(signup: &Signup) -> Welcome {
    Welcome {
        email: signup.email.clone(),
    }
}

pub fn app(pool: PgPool) -> RustStream {
    // Both names lead into `jobs`: `signups` is read by name, and the replies become rows of the
    // `welcome` group.
    let broker = SqlxBroker::new(pool)
        .route::<Job>("signups")
        .route::<Job>("welcome");
    RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(broker, |b| {
        b.include(greet);
    })
}
# }
# fn main() {}
```

[`SqlxBroker::route`] leads a name into the table of a row type, through the row's [`Publish`]:
the service's own insert, which lays the name, the bytes and the headers out in its columns. A
route serves both directions. A publish to the name writes a row, and `#[subscriber("signups")]`
opens a subscription by that name on the same table. A name ending in `*` leads every name that
starts with what precedes it, and an exact name wins over a prefix.

A [`Repository`] names its table at compile time, so a publish through it is a static call.
[`Routed`], the broker's default for replies and `Out` slots, looks each message's name up: one
hash lookup for an exact name, the prefixes scanned only when none matches, then one dynamic call
into the row's `Publish`. That call's future stays in a 1024-byte slot, so a routed publish
allocates what a repository publish does; a row whose `Publish` future is larger is boxed, one
allocation per publish on its route. A name no route leads anywhere fails the publish with
[`SqlxBrokerError::NoRoute`].

A by-name subscription takes the broker's poll interval and lease. Where the route's row leaves
every event to the crate and holds column types the crate reads itself, listed on
[`NamedSubscriber`], the subscription reads the rows by those columns: no box and no dynamic call
per message, in every form, as through an [`InboxQueue`]. Any other row runs its own code, at one
boxed delivery and one boxed settlement future per message. By-name subscriptions run on a
dialect that implements [`ByName`] for its database, as every built-in dialect does, and refuse
`max_attempts(..)` and `dead_letter(..)` at startup: an [`InboxQueue`] takes those.

