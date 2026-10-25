//! The service of `examples/sqlx_wake_notify.rs` without the macros: the table is described with
//! `InboxSpec`, and the handler is a named type mounted with `subscriber(..)`.
//!
//! The pool reads its settings from the `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and
//! `PGDATABASE` environment variables.
//!
//! ```text
//! cargo run -p ruststream-sqlx --example manual_sqlx_wake_notify --features inbox,postgres,chrono,testing,asyncapi
//! ```

use std::future::{Future, ready};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ruststream::OutgoingMessage;
use ruststream::schemars::JsonSchema;
use ruststream_sqlx::dialect::Column;
use ruststream_sqlx::spec::{Lease, Payload};
use ruststream_sqlx::{InboxSpec, InboxTable, PayloadRow};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgConnection, Postgres};

use ruststream_sqlx::prelude::*;
use sqlx::Connection;
use sqlx::postgres::PgPoolOptions;

/// One row of `signup_jobs`; the group is the name a signup was published to.
#[derive(sqlx::FromRow)]
struct SignupJob {
    id: i64,
    payload: Vec<u8>,
}

impl InboxTable for SignupJob {
    type Id = i64;
    type Table = InboxSpec<(Lease<DateTime<Utc>>, Payload)>;
    const TABLE: Self::Table = InboxSpec::new("signup_jobs", Column::new("id").generated())
        .group(Column::new("name"))
        .lease(Column::new("locked_until"))
        .payload(Column::new("payload"));

    fn id(&self) -> &i64 {
        &self.id
    }
}

impl PayloadRow for SignupJob {
    type Column = Vec<u8>;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

// --8<-- [start:service]
struct Welcome;

impl Handle<Signup> for Welcome {
    fn handle(
        &self,
        signup: &Signup,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(to = %signup.email, "sending the welcome");
        ready(Ok(()))
    }
}

/// The service on the database `options` reach.
fn service(options: PgConnectOptions) -> RustStream {
    let schema = options.clone();
    // Nothing connects here: the broker builds its pool when the service starts.
    let broker = SqlxBroker::from_connect_options(options, PgPoolOptions::new());
    // A notification wakes the subscription; the poll each minute reaches a row whose
    // notification was lost.
    let broker = broker
        .poll_interval(Duration::from_secs(60))
        .listen_notify()
        .route::<SignupJob>("signups");
    RustStream::new(AppInfo::new("signup", "0.1.0"))
        // The table, created before the subscription checks it.
        .on_startup(async move |()| {
            let mut conn = PgConnection::connect_with(&schema).await?;
            sqlx::raw_sql(SCHEMA).execute(&mut conn).await?;
            sqlx::raw_sql(ANNOUNCE).execute(&mut conn).await?;
            conn.close().await
        })
        .with_broker(broker, |b| {
            let welcome = subscriber("signups", Welcome)
                .map_source(|_| InboxQueue::<SignupJob>::new("signups"));
            b.include(welcome.build());
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

#[ruststream::app]
fn app() -> impl App {
    // The options read the `PG*` environment variables.
    service(PgConnectOptions::new())
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "signups")]
struct Signup {
    email: String,
}

// The live suites' gate: the test runs on a database of its own on the Postgres stand that
// `POSTGRES_TEST_URL` names, and skips without it.
#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
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
        let tb = TestApp::start_live(service(db.options())).await?;
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
