//! Publishing into tables: the typed repository, and the route table the broker publishes through
//! by default.

use std::any::type_name;

use ruststream::OutgoingMessage;
use ruststream_sqlx_dialect::TableSpec;
#[cfg(feature = "testing")]
use sqlx::Pool;

use super::broker::Shared;
use super::database::QueueDatabase;
#[cfg(feature = "testing")]
use super::database::notify::Listening;
use super::database::notify::announce;
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::events::Publish;
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};

mod repository;
mod routed;
mod routes;
mod wake;

pub use repository::{Repository, RepositoryPublisher};
pub use routed::{Routed, RoutedPublisher};
use routes::Route;
pub(crate) use routes::Routes;
pub(crate) use wake::{TableWake, Wakes};

/// The table `spec` describes, qualified with its schema, for messages.
pub(crate) fn table_of(spec: &TableSpec<'_>) -> String {
    spec.schema().map_or_else(
        || spec.table().to_owned(),
        |schema| format!("{schema}.{}", spec.table()),
    )
}

/// The failure of a write of `message` into `table`.
fn failed(
    message: &OutgoingMessage<'_>,
    table: &TableSpec<'_>,
    row: &'static str,
    source: sqlx::Error,
) -> SqlxBrokerError {
    SqlxBrokerError::Publish {
        name: message.name().to_owned(),
        table: table_of(table),
        row,
        source: Box::new(source),
    }
}

/// Refuses `message` when `Row`'s table cannot hold one of its headers.
fn fits<DB, Row>(message: &OutgoingMessage<'_>) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
{
    // Why a run-time check: headers are the message's, known only when it is published, and a
    // header the row cannot hold would reach the table lost or changed.
    Row::unfit_header(message.headers()).map_or(Ok(()), |header| {
        Err(SqlxBrokerError::Header {
            name: message.name().to_owned(),
            table: table_of(&Row::SPEC),
            row: type_name::<Row>(),
            header: header.to_owned(),
        })
    })
}

/// Writes `message` into `Row`'s table on a connection of the pool, unless the broker is shut
/// down.
async fn insert<DB, Row>(
    shared: &Shared<DB>,
    wake: &'static TableWake,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
{
    let failed = |source| failed(message, &Row::SPEC, type_name::<Row>(), source);
    // Why a run-time check: the pool is the service's and outlives the broker, so only the
    // broker's own flag can refuse a publisher handed out before shutdown.
    if shared.is_closed() {
        return Err(SqlxBrokerError::Closed);
    }
    fits::<DB, Row>(message)?;
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        let notify = shared.listening.as_ref().map(Listening::notify);
        return publish_off_clock::<DB, Row>(&shared.pool, notify, wake, message)
            .await
            .map_err(failed);
    }
    let mut conn = shared.pool.acquire().await.map_err(failed)?;
    Row::publish(&mut conn, message).await.map_err(failed)?;
    if let Some(listening) = &shared.listening {
        announce::<DB>(listening.notify(), &mut conn, wake, message.name()).await;
    }
    Ok(())
}

/// Writes `message` into `Row`'s table on `conn`, unless the table cannot hold one of its
/// headers: what a route runs behind its dynamic call.
async fn publish_row<DB, Row>(
    conn: &mut DB::Connection,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
{
    fits::<DB, Row>(message)?;
    Row::publish(conn, message)
        .await
        .map_err(|source| failed(message, &Row::SPEC, type_name::<Row>(), source))
}

/// Writes `message` through `route` on a connection of the pool, unless the broker is shut down,
/// then wakes the subscriptions `wake` holds for the message's group: the route table's write,
/// for the broker's publishes and the harness's injections alike.
pub(crate) async fn insert_routed<DB: QueueDatabase>(
    shared: &Shared<DB>,
    route: &dyn Route<DB>,
    wake: &'static TableWake,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError> {
    // Why a run-time check: the pool is the service's and outlives the broker, so only the
    // broker's own flag can refuse a publisher handed out before shutdown.
    if shared.is_closed() {
        return Err(SqlxBrokerError::Closed);
    }
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        route.insert_in_process(shared, wake, message).await?;
        wake.wake(message.name());
        return Ok(());
    }
    let mut conn = shared.pool.acquire().await.map_err(|source| {
        let description = route.description();
        failed(message, &description.spec, description.row, source)
    })?;
    route.insert(&mut conn, message).await?;
    if let Some(listening) = &shared.listening {
        announce::<DB>(listening.notify(), &mut conn, wake, message.name()).await;
    }
    wake.wake(message.name());
    Ok(())
}

/// The insert of an in-process connection, off a paused clock, announced with `notify` when the
/// broker listens.
#[cfg(feature = "testing")]
async fn publish_off_clock<DB, Row>(
    pool: &Pool<DB>,
    notify: Option<&'static str>,
    wake: &'static TableWake,
    message: &OutgoingMessage<'_>,
) -> Result<(), sqlx::Error>
where
    DB: QueueDatabase,
    Row: Publish<DB>,
{
    let pool = pool.clone();
    let name = message.name().to_owned();
    let payload = message.payload().to_vec();
    let headers = message.headers().clone();
    off_clock(async move {
        let mut conn = pool.acquire().await?;
        let message = OutgoingMessage::new(&name, &payload).with_headers(headers);
        Row::publish(&mut conn, &message).await?;
        if let Some(notify) = notify {
            announce::<DB>(notify, &mut conn, wake, &name).await;
        }
        Ok(())
    })
    .await
    .unwrap_or_else(|| Err(cancelled()))
}

/// A publisher's write: the insert, the harness's books of what the broker published, then the
/// wake-up of the subscriptions `wake` holds for the message's group once the row is written.
async fn write<DB, Row>(
    shared: &Shared<DB>,
    wake: &'static TableWake,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
{
    #[cfg(feature = "testing")]
    shared.harness.expect(message.name());
    let inserted = insert::<DB, Row>(shared, wake, message).await;
    #[cfg(feature = "testing")]
    match &inserted {
        Ok(()) => shared.harness.published(message),
        Err(_) => shared.harness.refused(message.name()),
    }
    if inserted.is_ok() {
        wake.wake(message.name());
    }
    inserted
}
