//! A dialect of the service's own on Postgres: it wraps the built-in dialect, writes its own
//! acknowledgement, and takes the forms whose traits it implements.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json",
    feature = "testing"
))]

mod live;

use std::num::NonZeroUsize;
use std::time::Duration;

use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream_sqlx::dialect::{
    self, ClaimShape, Dialect, Param, Role, RowLock, Statement, StatementError, TableName,
    TableSpec,
};
use ruststream_sqlx::{InboxQueue, SqlxBroker};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use live::postgres::{Db, database};
use live::rows::row_lock::SendEmail;

/// The group an acknowledged email moves to under [`Audited`].
const ACKNOWLEDGED: &str = "acknowledged";

/// The Postgres dialect with an acknowledgement of the service's own: an acknowledged row of a
/// table with groups moves to the `acknowledged` group and stays there for an audit, unmarked.
#[derive(Debug)]
struct Audited;

impl Dialect for Audited {
    fn name(&self) -> &'static str {
        "audited"
    }

    fn quote_into(&self, ident: &str, out: &mut String) {
        dialect::Postgres.quote_into(ident, out);
    }

    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
        dialect::Postgres.placeholder_into(index, out);
    }

    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.fetch(spec)
    }

    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        let Some(group) = spec.column(Role::Group) else {
            return dialect::Postgres.ack(spec);
        };
        let mut sql = String::from("UPDATE ");
        self.quote_into(spec.table(), &mut sql);
        sql.push_str(" SET ");
        self.quote_into(group.name(), &mut sql);
        sql.push_str(" = '");
        sql.push_str(ACKNOWLEDGED);
        sql.push_str("' WHERE ");
        self.quote_into(spec.id().name(), &mut sql);
        sql.push_str(" = ");
        self.placeholder_into(NonZeroUsize::MIN, &mut sql);
        Ok(Statement::new(sql, [Param::Id]))
    }

    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
        dialect::Postgres.retry(spec)
    }

    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.retry_after(spec)
    }

    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.discard(spec)
    }

    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.dead_letter_group(spec)
    }

    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError> {
        dialect::Postgres.dead_letter_table(spec, target)
    }

    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        dialect::Postgres.insert(spec)
    }
}

impl RowLock for Audited {
    fn lock_claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        dialect::Postgres.lock_claim(spec, shape)
    }

    fn begin_lock_claim(&self) -> Option<&'static str> {
        dialect::Postgres.begin_lock_claim()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Email {
    to: String,
}

fn email() -> Email {
    Email {
        to: "a@example.com".to_owned(),
    }
}

/// The broker of a service whose statements `Audited` builds.
fn broker(pool: &PgPool) -> SqlxBroker<Db, Audited> {
    SqlxBroker::with_dialect(pool.clone(), Audited)
        .poll_interval(Duration::from_millis(20))
        .route::<SendEmail>("emails")
}

#[subscriber(InboxQueue::<SendEmail>::new("emails"))]
async fn send(_email: &Email) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialect_of_the_services_own_takes_rows_by_row_lock_and_acknowledges_its_way() {
    let Some(db) = database().await else { return };
    let app = RustStream::new(AppInfo::new("audit", "0.0.0")).with_broker(broker(&db.pool), |b| {
        b.include(send);
    });
    let tb = TestApp::start_live(app).await.expect("the app starts");
    tb.broker::<SqlxBroker<Db, Audited>>()
        .message(&email())
        .to("emails")
        .publish()
        .await
        .expect("the publish settles");

    tb.broker::<SqlxBroker<Db, Audited>>()
        .subscriber("emails")
        .assert_called_once()
        .with(&email())
        .settled(HandlerOutcome::ack());
    assert_eq!(
        db.email_rows("email_jobs").await,
        [(
            ACKNOWLEDGED.to_owned(),
            br#"{"to":"a@example.com"}"#.to_vec(),
            1,
            false
        )],
        "the dialect's own acknowledgement moved the row and left it unmarked"
    );
    tb.shutdown().await.expect("the app stops");
    db.finish().await;
}
