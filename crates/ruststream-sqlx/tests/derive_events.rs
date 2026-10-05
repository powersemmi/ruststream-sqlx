//! What `#[derive(Inbox)]` hands the broker beyond the description: the row's parts, the events
//! it takes over, and the values its statements bind.

#![cfg(all(
    feature = "inbox",
    feature = "postgres",
    feature = "chrono",
    feature = "json"
))]

use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use ruststream_sqlx::__private::{Event, Events, IdAt, Now, Param, Prepared, Queue, Shape, Values};
use ruststream_sqlx::{
    Ack, Clock, DatabaseClock, Fetch, HeaderColumn, Inbox, InboxRow, PayloadRow,
};
use sqlx::postgres::PgArguments;
use sqlx::types::Json;
use sqlx::{Arguments, PgConnection, Postgres};

/// Every role this phase serves.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "email_jobs")]
struct SendEmail {
    #[field(id, generated)]
    job_id: i64,
    #[field(group)]
    name: String,
    #[field(partition_key)]
    customer: String,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(attempt)]
    attempt: i16,
    #[field(processed_at)]
    processed_at: Option<DateTime<Utc>>,
    #[field(headers)]
    meta: Json<BTreeMap<String, String>>,
    #[field(payload)]
    payload: Vec<u8>,
}

fn email() -> SendEmail {
    SendEmail {
        job_id: 7,
        name: "emails".to_owned(),
        customer: "acme".to_owned(),
        retry_after: Utc::now(),
        attempt: 2,
        processed_at: None,
        meta: Json(BTreeMap::from([("x-tenant".to_owned(), "acme".to_owned())])),
        payload: b"{\"to\":\"a@b\"}".to_vec(),
    }
}

#[test]
fn the_derive_hands_every_event_to_the_crate_by_default() {
    assert_eq!(<SendEmail as Events<Postgres>>::SHAPE, Shape::default());
    // Every column type of the row is one a by-name subscription reads and binds itself.
    assert!(<SendEmail as Events<Postgres>>::kinds().is_some());
}

/// A queue on a clock of the service's own.
struct Office;

impl Clock for Office {
    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH
    }
}

#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", clock = Office)]
struct OfficeHours {
    #[field(id)]
    id: i64,
    #[field(retry_after)]
    retry_after: DateTime<Utc>,
    #[field(payload)]
    payload: Vec<u8>,
}

#[test]
fn a_clock_of_the_services_own_needs_the_rows_code() {
    // Only the row's own code knows the clock: a by-name subscription keeps the row's events.
    assert_eq!(<OfficeHours as Events<Postgres>>::kinds(), None);
    let _ = |row: OfficeHours| (row.id, row.retry_after, row.payload);
}

#[test]
fn a_row_lends_its_parts_to_the_delivery() {
    let mut row = email();
    assert_eq!(row.payload(), b"{\"to\":\"a@b\"}");
    assert_eq!(<SendEmail as Events<Postgres>>::id(&row), &7);
    assert_eq!(
        <SendEmail as Events<Postgres>>::partition_key(&row),
        Some(b"acme".as_slice())
    );
    assert_eq!(<SendEmail as Events<Postgres>>::attempt(&row), Some(2));
    let headers = <SendEmail as Events<Postgres>>::take_headers(&mut row);
    assert_eq!(headers.get_str("x-tenant"), Some("acme"));
    // The delivery takes the headers once: they move out of the row rather than being copied.
    assert!(<SendEmail as Events<Postgres>>::take_headers(&mut row).is_empty());
    let _ = (&row.name, row.retry_after, row.processed_at, row.job_id);
}

/// The subscription the bound statements serve: the `emails` group of `email_jobs`.
static EMAILS: LazyLock<Queue> = LazyLock::new(|| Queue {
    name: "emails",
    table: "email_jobs",
    row: "SendEmail",
    spec: SendEmail::SPEC,
    id_at: IdAt::First,
    native_retry_after: true,
    kinds: None,
    prepared: Prepared::default(),
    poll_interval: Duration::from_secs(1),
    max_attempts: None,
    dead_letter: None,
});

fn values(event: Event, id: &i64) -> Values<'_, SendEmail> {
    Values {
        event,
        queue: &EMAILS,
        limit: 1,
        id: Some(id),
        ids: &[],
        delay: Duration::from_secs(30),
        destination: "emails.dead",
        now: Now::default(),
    }
}

#[test]
fn binding_follows_the_meaning_of_each_parameter() -> Result<(), sqlx::Error> {
    let id = 7;
    let bind = |param, arguments: &mut PgArguments, event| {
        <SendEmail as Events<Postgres>>::bind(param, arguments, &values(event, &id))
    };

    // The claim binds the group, the time the row may run at and the limit.
    let mut claim = PgArguments::default();
    for param in [Param::Group, Param::Now, Param::Limit] {
        assert!(bind(param, &mut claim, Event::Claim)?);
    }
    assert_eq!(claim.len(), 3);

    // An acknowledgement marks the row with the time it finished.
    let mut ack = PgArguments::default();
    assert!(bind(Param::Now, &mut ack, Event::Ack)?);
    assert!(bind(Param::Id, &mut ack, Event::Ack)?);
    assert_eq!(ack.len(), 2);

    // A delayed retry binds the time it comes back, or the delay for the database's clock.
    let mut later = PgArguments::default();
    assert!(bind(Param::RetryAfter, &mut later, Event::RetryAfter)?);
    assert!(bind(Param::Delay, &mut later, Event::RetryAfter)?);
    assert_eq!(later.len(), 2);

    // A dead letter binds where the row goes; nothing else has a destination.
    let mut dead = PgArguments::default();
    assert!(bind(Param::Destination, &mut dead, Event::DeadLetter)?);
    assert!(!bind(Param::Destination, &mut dead, Event::Ack)?);
    // The table has no id list to bind outside a custom claim.
    assert!(!bind(Param::Ids, &mut dead, Event::Fetch)?);
    Ok(())
}

/// A table that assembles its rows and acknowledges them itself, on the database's clock.
#[derive(Inbox, sqlx::FromRow)]
#[inbox(table = "jobs", custom(fetch, ack), clock = DatabaseClock)]
struct Assembled {
    #[field(id)]
    id: i64,
    #[field(payload)]
    payload: Vec<u8>,
}

impl Fetch<Postgres> for Assembled {
    async fn fetch(conn: &mut PgConnection, ids: &[i64]) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as("SELECT id, payload FROM jobs WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(conn)
            .await
    }
}

impl Ack<Postgres> for Assembled {
    async fn ack(conn: &mut PgConnection, id: &i64) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE jobs SET done = true WHERE id = $1")
            .bind(id)
            .execute(conn)
            .await?;
        Ok(())
    }
}

#[test]
fn listed_events_are_the_services_own() {
    let shape = <Assembled as Events<Postgres>>::SHAPE;
    assert!(shape.custom_fetch && shape.custom_ack);
    assert!(!shape.custom_claim && !shape.custom_discard);
    // The service's own events run only through the row's code.
    assert_eq!(<Assembled as Events<Postgres>>::kinds(), None);
    assert!(Assembled::SPEC.uses_database_clock());
    assert!(!SendEmail::SPEC.uses_database_clock());
}

#[test]
fn a_header_column_reads_and_writes_a_header_map() {
    let mut headers = ruststream::HeaderMap::new();
    headers.insert("content-type", "application/json");
    let mut column = Json::<BTreeMap<String, String>>::from_headers(&headers);
    assert_eq!(column.take_headers(), headers);
    assert!(column.0.is_empty(), "the values moved out of the column");
    let mut none: Option<Json<BTreeMap<String, String>>> = None;
    assert!(none.take_headers().is_empty());
    let _ = SystemTime::now();
}
