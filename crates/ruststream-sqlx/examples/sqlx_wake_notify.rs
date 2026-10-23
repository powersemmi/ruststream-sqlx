//! Signups another program writes into a Postgres table and announces with `pg_notify`, from a
//! trigger: the subscription claims each one as soon as its transaction commits.
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example sqlx_wake_notify --features inbox,postgres,chrono,testing
//! ```

use std::error::Error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgPool, Postgres};

use ruststream_sqlx::prelude::*;

/// One row of `signup_jobs`; the group is the name a signup was published to.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "signup_jobs")]
struct SignupJob {
    #[field(id, generated)]
    id: i64,
    #[field(group)]
    name: String,
    #[field(locked_until)]
    locked_until: Option<DateTime<Utc>>,
    #[field(payload)]
    payload: Vec<u8>,
}

// --8<-- [start:service]
#[subscriber(InboxQueue::<SignupJob>::new("signups"))]
async fn welcome(signup: &Signup) -> HandlerOutcome {
    tracing::info!(to = %signup.email, "sending the welcome");
    HandlerOutcome::ack()
}

fn service(pool: PgPool) -> RustStream {
    // A notification wakes the subscription; the poll each minute reaches a row whose
    // notification was lost.
    let broker = SqlxBroker::new(pool)
        .poll_interval(Duration::from_secs(60))
        .listen_notify()
        .route::<SignupJob>("signups");
    RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(broker, |b| {
        b.include(welcome);
    })
}

/// The trigger announces every row another program inserts: the channel is the table, the
/// payload its group.
const ANNOUNCE: &str = "
CREATE OR REPLACE FUNCTION announce_signup() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, NEW.name);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER signup_announced AFTER INSERT ON signup_jobs
    FOR EACH ROW EXECUTE FUNCTION announce_signup();
";
// --8<-- [end:service]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect_with(PgConnectOptions::new()).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    sqlx::raw_sql(ANNOUNCE).execute(&pool).await?;
    service(pool.clone()).run().await?;
    pool.close().await;
    Ok(())
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS signup_jobs (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    locked_until TIMESTAMPTZ,
    payload BYTEA NOT NULL
)";

/// A publish to `signups` writes a row; the broker announces it itself.
impl Publish<Postgres> for SignupJob {
    async fn publish(
        conn: &mut PgConnection,
        message: &OutgoingMessage<'_>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO signup_jobs (name, payload) VALUES ($1, $2)")
            .bind(message.name())
            .bind(message.payload())
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

// The live suites' gate: the test runs on a database of its own on the Postgres stand that
// `POSTGRES_TEST_URL` names, and skips without it.
#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_sqlx::prelude::*;
    use sqlx::Postgres;

    use super::{ANNOUNCE, SCHEMA, Signup, live, service};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_row_another_program_announces_is_claimed_at_once() -> Result<(), Box<dyn Error>> {
        let Some(db) = live::postgres::database().await else {
            return Ok(());
        };
        sqlx::raw_sql(SCHEMA).execute(&db.pool).await?;
        sqlx::raw_sql(ANNOUNCE).execute(&db.pool).await?;
        let tb = TestApp::start_live(service(db.pool.clone())).await?;
        // The subscription's first claim finds the table empty: it then waits its minute.
        tb.advance(Duration::from_millis(200)).await?;
        let signup = Signup {
            email: "ada@example.com".to_owned(),
        };
        // Another program's insert: the broker does not write it, the trigger announces it.
        sqlx::query("INSERT INTO signup_jobs (name, payload) VALUES ('signups', $1)")
            .bind(serde_json::to_vec(&signup)?)
            .execute(&db.pool)
            .await?;
        // Far less than the minute the poll waits.
        tb.advance(Duration::from_secs(3)).await?;

        tb.broker::<SqlxBroker<Postgres>>()
            .subscriber("signups")
            .assert_called_once()
            .with(&signup)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        db.finish().await;
        Ok(())
    }
}
