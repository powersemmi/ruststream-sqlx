//! By-name subscriptions: `#[subscriber("emails")]` and [`Subscribe`], through the route a name
//! takes.
//!
//! A route's row that leaves every event to the crate is read by its role columns into
//! [`NamedRow`], one concrete type whatever the route; any other row is read by its own code and
//! erased behind a box.

mod by_name;
mod database;
pub(crate) mod kinds;
mod row;

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{BoxFuture, Either};
use futures::{Stream, StreamExt};
use ruststream::{
    AckError, BrokerMoves, HeaderMap, IncomingMessage, RetryDeclaration, Subscribe, Subscriber,
};
use sync_wrapper::SyncWrapper;

pub use by_name::ByName;
pub use database::RoleColumns;
pub use row::{NamedBytes, NamedId, NamedRow, NamedTime};

use super::broker::{ConnectedSqlxBroker, Shared};
use super::database::QueueDatabase;
use super::delivery::InboxDelivery;
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::publish::table_of;
use super::queue::{Description, Timing, open};
use super::subscriber::InboxSubscriber;
use super::{BuiltIn, FormDialect, Plain};
use super::{InboxRow, PayloadRow};

/// A delivery of a by-name subscription whose row runs its own code, its row type erased.
pub(crate) trait Erased: fmt::Debug + Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &HeaderMap;
    fn partition_key(&self) -> Option<&[u8]>;
    fn redelivery_count(&self) -> Option<u64>;
    fn supports_nack_after(&self) -> bool;
    fn ack(self: Box<Self>) -> BoxFuture<'static, Result<(), AckError>>;
    fn nack(self: Box<Self>, requeue: bool) -> BoxFuture<'static, Result<(), AckError>>;
    fn nack_after(self: Box<Self>, delay: Duration) -> BoxFuture<'static, Result<(), AckError>>;
}

impl<DB, Row> Erased for InboxDelivery<DB, Row>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    fn payload(&self) -> &[u8] {
        IncomingMessage::payload(self)
    }

    fn headers(&self) -> &HeaderMap {
        IncomingMessage::headers(self)
    }

    fn partition_key(&self) -> Option<&[u8]> {
        IncomingMessage::partition_key(self)
    }

    fn redelivery_count(&self) -> Option<u64> {
        IncomingMessage::redelivery_count(self)
    }

    fn supports_nack_after(&self) -> bool {
        IncomingMessage::supports_nack_after(self)
    }

    fn ack(self: Box<Self>) -> BoxFuture<'static, Result<(), AckError>> {
        Box::pin(IncomingMessage::ack(*self))
    }

    fn nack(self: Box<Self>, requeue: bool) -> BoxFuture<'static, Result<(), AckError>> {
        Box::pin(IncomingMessage::nack(*self, requeue))
    }

    fn nack_after(self: Box<Self>, delay: Duration) -> BoxFuture<'static, Result<(), AckError>> {
        Box::pin(IncomingMessage::nack_after(*self, delay))
    }
}

/// A delivery of a by-name subscription: a row of the table its name's route leads to.
///
/// It settles as an [`InboxDelivery`](crate::InboxDelivery) does. A route whose row leaves every
/// event to the crate delivers its rows read by role: no box and no dynamic call per message, as
/// through an [`InboxQueue`](crate::InboxQueue). A row that overrides an event, or holds a column
/// type the crate does not read itself, settles through its own code: each delivery is boxed, and
/// each settlement is a dynamic call whose future is boxed too.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, Publish, SqlxBroker};
/// use serde::Deserialize;
/// use sqlx::{PgConnection, PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for SendEmail {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO email_jobs (name, payload) VALUES ($1, $2)")
///             .bind(message.name())
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// // `emails` is a name: its route leads it to `email_jobs`, and each delivery settles there.
/// #[subscriber("emails")]
/// async fn send(email: &Email) -> HandlerOutcome {
///     if email.to.is_empty() {
///         return HandlerOutcome::drop();
///     }
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(
///         SqlxBroker::new(pool).route::<SendEmail>("emails"),
///         |b| {
///             b.include(send);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub struct NamedDelivery<DB, D = BuiltIn<DB>>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    delivered: Delivered<DB, D>,
}

/// A by-name delivery of either path.
// Why the size difference stays: a row read by role is the delivery itself, and a box would cost an
// allocation per delivery. Only `testing` widens it past the lint's bound, with the harness's
// connection every in-process delivery carries.
#[expect(
    clippy::large_enum_variant,
    reason = "a by-name delivery carries its row inline; a box would allocate per delivery"
)]
enum Delivered<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    /// A row read by role.
    Described(InboxDelivery<DB, NamedRow<D>>),
    /// A row read by its own code.
    Erased(Box<dyn Erased>),
}

impl<DB, D> NamedDelivery<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    const fn described(delivery: InboxDelivery<DB, NamedRow<D>>) -> Self {
        Self {
            delivered: Delivered::Described(delivery),
        }
    }

    fn erased(delivery: Box<dyn Erased>) -> Self {
        Self {
            delivered: Delivered::Erased(delivery),
        }
    }
}

impl<DB, D> fmt::Debug for NamedDelivery<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut tuple = f.debug_tuple("NamedDelivery");
        match &self.delivered {
            Delivered::Described(delivery) => tuple.field(delivery),
            Delivered::Erased(delivery) => tuple.field(delivery),
        };
        tuple.finish()
    }
}

impl<DB, D> IncomingMessage for NamedDelivery<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    fn payload(&self) -> &[u8] {
        match &self.delivered {
            Delivered::Described(delivery) => IncomingMessage::payload(delivery),
            Delivered::Erased(delivery) => delivery.payload(),
        }
    }

    fn headers(&self) -> &HeaderMap {
        match &self.delivered {
            Delivered::Described(delivery) => IncomingMessage::headers(delivery),
            Delivered::Erased(delivery) => delivery.headers(),
        }
    }

    fn partition_key(&self) -> Option<&[u8]> {
        match &self.delivered {
            Delivered::Described(delivery) => IncomingMessage::partition_key(delivery),
            Delivered::Erased(delivery) => delivery.partition_key(),
        }
    }

    fn redelivery_count(&self) -> Option<u64> {
        match &self.delivered {
            Delivered::Described(delivery) => IncomingMessage::redelivery_count(delivery),
            Delivered::Erased(delivery) => delivery.redelivery_count(),
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        match self.delivered {
            Delivered::Described(delivery) => IncomingMessage::ack(delivery).await,
            Delivered::Erased(delivery) => delivery.ack().await,
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self.delivered {
            Delivered::Described(delivery) => IncomingMessage::nack(delivery, requeue).await,
            Delivered::Erased(delivery) => delivery.nack(requeue).await,
        }
    }

    fn supports_nack_after(&self) -> bool {
        match &self.delivered {
            Delivered::Described(delivery) => IncomingMessage::supports_nack_after(delivery),
            Delivered::Erased(delivery) => delivery.supports_nack_after(),
        }
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        match self.delivered {
            Delivered::Described(delivery) => IncomingMessage::nack_after(delivery, delay).await,
            Delivered::Erased(delivery) => delivery.nack_after(delay).await,
        }
    }
}

/// The deliveries of a by-name subscription whose row runs its own code.
pub(crate) type ErasedStream =
    Pin<Box<dyn Stream<Item = Result<Box<dyn Erased>, SqlxBrokerError>> + Send>>;

/// The subscriber a by-name subscription opens: the claim loop of the table its name's route
/// leads to.
///
/// It claims as an [`InboxSubscriber`](crate::InboxSubscriber) does, with the broker's poll
/// interval, on a broker whose dialect implements [`ByName`](crate::ByName) for its database, as
/// every built-in dialect does. A route whose row leaves every event to
/// the crate, with role columns of types the crate reads itself, is read by those columns alone,
/// in the row lock form and in the lease form: no box and no dynamic call per message. Each
/// column is held to the type the struct reads it as, so a row that would not decode into the
/// struct goes to the decode-failure policy here too. The types are an `i16`, `i32`, `i64`,
/// `String` or `Vec<u8>` id, a `Vec<u8>` or `String` payload, headers as JSON, a text or byte key,
/// an integer attempt, `chrono` or `time` times and leases, and
/// [`SystemClock`](crate::SystemClock) or [`DatabaseClock`](crate::DatabaseClock). Any other row
/// runs its own code: each delivery is boxed, each settlement is a dynamic call whose future is
/// boxed, and each poll of the stream is a dynamic call.
///
/// A mount that declares `max_attempts(..)` or `dead_letter(..)` on a name is refused at startup:
/// an [`InboxQueue`](crate::InboxQueue) descriptor takes those.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream_sqlx::{Inbox, Publish};
/// # use sqlx::{PgConnection, Postgres};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id, generated)] id: i64, #[field(group)] name: String, #[field(payload)] payload: Vec<u8> }
/// # impl Publish<Postgres> for Job {
/// #     async fn publish(_: &mut PgConnection, _: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> { Ok(()) }
/// # }
/// use futures::StreamExt;
/// use ruststream::{Broker, IncomingMessage, Subscribe, Subscriber};
/// use ruststream_sqlx::SqlxBroker;
///
/// // What the runtime does for `#[subscriber("reports")]`, written out.
/// pub async fn drain(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
///     let connected = SqlxBroker::new(pool).route::<Job>("reports").connect().await?;
///     let mut subscriber = connected.subscribe("reports").await?;
///     let mut deliveries = std::pin::pin!(subscriber.stream());
///     while let Some(delivery) = deliveries.next().await {
///         delivery?.ack().await?;
///     }
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct NamedSubscriber<DB, D = BuiltIn<DB>>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    opened: Opened<DB, D>,
}

/// A by-name subscription of either path.
enum Opened<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    /// Rows read by role.
    Described(InboxSubscriber<DB, NamedRow<D>>),
    /// Rows read by their own code. The stream is polled only through `&mut`, so the wrapper
    /// shares the subscriber between threads, as a mount that publishes requires, at no cost.
    Erased(SyncWrapper<ErasedStream>),
}

impl<DB, D> fmt::Debug for NamedSubscriber<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.opened {
            Opened::Described(subscriber) => {
                f.debug_tuple("NamedSubscriber").field(subscriber).finish()
            }
            Opened::Erased(_) => f.debug_struct("NamedSubscriber").finish_non_exhaustive(),
        }
    }
}

impl<DB, D> Subscriber for NamedSubscriber<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    type Message = NamedDelivery<DB, D>;
    type Error = SqlxBrokerError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        match &mut self.opened {
            Opened::Described(subscriber) => Either::Left(
                subscriber
                    .stream()
                    .map(|delivery| delivery.map(NamedDelivery::described)),
            ),
            Opened::Erased(stream) => Either::Right(
                stream
                    .get_mut()
                    .as_mut()
                    .map(|delivery| delivery.map(NamedDelivery::erased)),
            ),
        }
    }
}

/// Opens the by-name subscription to `name` of `Row`'s table through `Row`'s own code, its row
/// type erased, its statements built by `form`.
pub(crate) fn erased<'a, DB, Row>(
    shared: &'a Arc<Shared<DB>>,
    form: &'a FormDialect,
    name: &'a str,
) -> BoxFuture<'a, Result<ErasedStream, SqlxBrokerError>>
where
    DB: QueueDatabase,
    Row: InboxRow + Events<DB> + PayloadRow,
{
    Box::pin(async move {
        let description = Description::of::<DB, Row>();
        let subscriber = open::<DB, Row, Plain>(
            shared,
            form,
            name,
            Timing::default(),
            &RetryDeclaration::new(),
            &description,
        )
        .await?;
        let stream = subscriber
            .into_stream()
            .map(|delivery| delivery.map(|delivery| -> Box<dyn Erased> { Box::new(delivery) }));
        let stream: ErasedStream = Box::pin(stream);
        Ok(stream)
    })
}

impl<DB, D> Subscribe for ConnectedSqlxBroker<DB, D>
where
    DB: QueueDatabase + RoleColumns,
    D: ByName<DB> + 'static,
{
    type Subscriber = NamedSubscriber<DB, D>;
    type Copies = BrokerMoves;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, SqlxBrokerError> {
        let Some((route, form)) = self.shared.routes.find(name) else {
            return Err(SqlxBrokerError::NoRoute {
                name: name.to_owned(),
            });
        };
        let description = route.description();
        if description.kinds.is_some() {
            tracing::debug!(
                target: "ruststream_sqlx",
                path = "described",
                subscription = name,
                table = %table_of(&description.spec),
                row = description.row,
                "a by-name subscription reads its rows by role",
            );
            let subscriber = open::<DB, NamedRow<D>, Plain>(
                &self.shared,
                form,
                name,
                Timing::default(),
                &RetryDeclaration::new(),
                &description.by_role(),
            )
            .await?;
            return Ok(NamedSubscriber {
                opened: Opened::Described(subscriber),
            });
        }
        tracing::debug!(
            target: "ruststream_sqlx",
            path = "erased",
            subscription = name,
            table = %table_of(&description.spec),
            row = description.row,
            "a by-name subscription runs its row's own code, a box per delivery and per settlement",
        );
        let stream = route.subscribe(&self.shared, form, name).await?;
        Ok(NamedSubscriber {
            opened: Opened::Erased(SyncWrapper::new(stream)),
        })
    }
}
