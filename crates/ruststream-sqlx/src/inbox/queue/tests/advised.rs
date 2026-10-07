//! What a subscription to a table in the advisory lock form prepares through the dialect built into
//! the crate for its database: the candidates, the lock and the unlock where the database keeps the
//! locks, and the take, as the dialect the broker picked builds them.

use std::sync::Arc;

use ruststream::RetryDeclaration;
#[cfg(feature = "mysql")]
use ruststream_sqlx_dialect::MySql;
#[cfg(feature = "postgres")]
use ruststream_sqlx_dialect::Postgres;
#[cfg(feature = "sqlite")]
use ruststream_sqlx_dialect::Sqlite;
use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Column, Form, KeyPart, Param, Statement, TableSpec,
};

use super::described;
#[cfg(all(
    feature = "any",
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite"
))]
use crate::inbox::AnyDialect;
#[cfg(all(
    feature = "any",
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite"
))]
use crate::inbox::database::built_in::{MYSQL_BACKEND, POSTGRES_BACKEND, SQLITE_BACKEND};
use crate::inbox::engine::{Prepared, Shape, Stmt};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::form::{AdvisoryForm, FormOn};
use crate::inbox::queue::open::build;
use crate::inbox::{BuiltIn, BuiltInDialect, FormDialect};

/// The lock key of every email: its id.
const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("emails-"), KeyPart::Column("job_id")];

/// The emails in the advisory lock form: a group per name, a delayed retry and an attempt
/// the take counts.
const EMAILS: TableSpec<'static> =
    TableSpec::new("email_jobs", Column::new("job_id"), Form::Advisory(KEY))
        .group(Column::new("name"))
        .retry_after(Column::new("retry_after"))
        .attempt(Column::new("attempt"))
        .payload(Column::new("payload"));

/// A statement's text and parameters.
type Text = (String, Vec<Param>);

fn prepared_text(statement: Stmt) -> Text {
    (statement.sql.to_owned(), statement.params.to_vec())
}

fn built_text(statement: &Statement) -> Text {
    (statement.sql().to_owned(), statement.params().to_vec())
}

/// What a subscription to the emails prepares, its statements built by the dialect `form`
/// shows.
fn prepared(form: &FormDialect) -> Prepared {
    let fail = |reason: String| SqlxBrokerError::Declaration {
        subscription: "emails".to_owned(),
        table: "email_jobs".to_owned(),
        row: "SendEmail",
        reason,
    };
    let described = described(&EMAILS, Shape::default(), ClaimShape::Rows);
    build(form, &RetryDeclaration::new(), &described, &fail).expect("the emails' statements build")
}

/// The emails' statements as the built-in dialect of `DB` builds them for a subscription.
fn built_in<DB: BuiltInDialect>(picked: DB::Picked) -> Prepared {
    let form = <AdvisoryForm as FormOn<BuiltIn<DB>>>::erase(&Arc::new(BuiltIn::new(picked)));
    prepared(&form)
}

/// Checks that `prepared` holds what `dialect` itself builds for the emails: the candidates
/// as the claim, the lock and the unlock, the take, and no retry statement.
fn holds_what(dialect: &dyn Advisory, prepared: &Prepared) {
    let claim = dialect
        .advisory_claim(&EMAILS)
        .expect("the dialect claims the emails");
    assert_eq!(prepared.claim.map(prepared_text), Some(built_text(&claim)));
    assert_eq!(
        prepared.lock.map(prepared_text),
        dialect.lock().as_ref().map(built_text)
    );
    assert_eq!(
        prepared.unlock.map(prepared_text),
        dialect.unlock().as_ref().map(built_text)
    );
    let take = dialect
        .take(&EMAILS, ClaimShape::Rows)
        .expect("the dialect takes the emails");
    assert_eq!(
        prepared.take.map(prepared_text),
        take.first().map(built_text)
    );
    assert_eq!(
        prepared.take_then.map(prepared_text),
        take.get(1).map(built_text)
    );
    // The take counted the attempt: a retry has nothing left to write.
    assert_eq!(prepared.retry, None);
}

#[cfg(feature = "postgres")]
#[test]
fn the_built_in_postgres_dialect_builds_the_advisory_form() {
    let prepared = built_in::<sqlx::Postgres>(Postgres);
    holds_what(&Postgres, &prepared);
    assert!(prepared.lock.is_some() && prepared.unlock.is_some());
    // An update returns the row it counted: the take is one statement.
    assert!(prepared.take.is_some() && prepared.take_then.is_none());
}

#[cfg(feature = "mysql")]
#[test]
fn the_built_in_mysql_dialect_builds_the_advisory_form() {
    let prepared = built_in::<sqlx::MySql>(MySql);
    holds_what(&MySql, &prepared);
    assert!(prepared.lock.is_some() && prepared.unlock.is_some());
    // An update returns no rows: the take counts the attempt, then reads the row.
    assert!(prepared.take.is_some() && prepared.take_then.is_some());
}

#[cfg(feature = "sqlite")]
#[test]
fn the_built_in_sqlite_dialect_builds_the_advisory_form() {
    let prepared = built_in::<sqlx::Sqlite>(Sqlite);
    holds_what(&Sqlite, &prepared);
    // The process keeps the locks: no statement takes or releases one.
    assert!(prepared.lock.is_none() && prepared.unlock.is_none());
    assert!(prepared.take.is_some() && prepared.take_then.is_none());
}

#[cfg(all(
    feature = "any",
    feature = "postgres",
    feature = "mysql",
    feature = "sqlite"
))]
#[test]
fn an_any_backend_builds_the_advisory_form_as_its_database_does() {
    let any = |backend| {
        let picked = AnyDialect::of(backend).expect("the backend's feature is on");
        built_in::<sqlx::Any>(picked)
    };
    assert_eq!(any(POSTGRES_BACKEND), built_in::<sqlx::Postgres>(Postgres));
    assert_eq!(any(MYSQL_BACKEND), built_in::<sqlx::MySql>(MySql));
    assert_eq!(any(SQLITE_BACKEND), built_in::<sqlx::Sqlite>(Sqlite));
}
