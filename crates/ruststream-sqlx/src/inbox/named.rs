//! By-name subscriptions: `#[subscriber("emails")]` and [`Subscribe`], through the route a name
//! takes.

use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use ruststream::{
    AckError, BrokerMoves, HeaderMap, IncomingMessage, RetryDeclaration, Subscribe, Subscriber,
};

use super::PayloadRow;
use super::broker::{ConnectedSqlxBroker, Shared};
use super::database::QueueDatabase;
use super::delivery::InboxDelivery;
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::queue::open;

/// A delivery of a by-name subscription, its row type erased.
trait Erased: fmt::Debug + Send + Sync {
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
/// It settles as an [`InboxDelivery`](crate::InboxDelivery) does. The row type is chosen by the
/// route when the subscription opens, so it is erased: each delivery is boxed, and each settlement
/// is one dynamic call whose future is boxed too. A subscription through an
/// [`InboxQueue`](crate::InboxQueue) descriptor delivers without either.
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
pub struct NamedDelivery<DB> {
    inner: Box<dyn Erased>,
    _db: PhantomData<fn() -> DB>,
}

impl<DB> fmt::Debug for NamedDelivery<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NamedDelivery").field(&self.inner).finish()
    }
}

impl<DB: QueueDatabase> IncomingMessage for NamedDelivery<DB> {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    fn partition_key(&self) -> Option<&[u8]> {
        self.inner.partition_key()
    }

    fn redelivery_count(&self) -> Option<u64> {
        self.inner.redelivery_count()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }

    fn supports_nack_after(&self) -> bool {
        self.inner.supports_nack_after()
    }

    async fn nack_after(self, delay: Duration) -> Result<(), AckError> {
        self.inner.nack_after(delay).await
    }
}

type NamedStream<DB> =
    Pin<Box<dyn Stream<Item = Result<NamedDelivery<DB>, SqlxBrokerError>> + Send>>;

/// The subscriber a by-name subscription opens: the claim loop of the table its name's route
/// leads to.
///
/// It claims as an [`InboxSubscriber`](crate::InboxSubscriber) does, with the broker's poll
/// interval; each poll of its stream is one dynamic call.
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
pub struct NamedSubscriber<DB> {
    stream: NamedStream<DB>,
}

impl<DB> fmt::Debug for NamedSubscriber<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedSubscriber").finish_non_exhaustive()
    }
}

impl<DB: QueueDatabase> Subscriber for NamedSubscriber<DB> {
    type Message = NamedDelivery<DB>;
    type Error = SqlxBrokerError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.stream.as_mut()
    }
}

/// Opens the by-name subscription to `name` of `Row`'s table, its row type erased.
pub(crate) fn subscribe<'a, DB, Row>(
    shared: &'a Arc<Shared<DB>>,
    name: &'a str,
) -> BoxFuture<'a, Result<NamedSubscriber<DB>, SqlxBrokerError>>
where
    DB: QueueDatabase,
    Row: Events<DB> + PayloadRow,
{
    Box::pin(async move {
        let subscriber = open::<DB, Row>(shared, name, None, &RetryDeclaration::new()).await?;
        let stream = subscriber.into_stream().map(|delivery| {
            delivery.map(|delivery| NamedDelivery {
                inner: Box::new(delivery),
                _db: PhantomData,
            })
        });
        Ok(NamedSubscriber {
            stream: Box::pin(stream),
        })
    })
}

impl<DB: QueueDatabase> Subscribe for ConnectedSqlxBroker<DB> {
    type Subscriber = NamedSubscriber<DB>;
    type Copies = BrokerMoves;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, SqlxBrokerError> {
        let Some(route) = self.shared.routes.find(name) else {
            return Err(SqlxBrokerError::NoRoute {
                name: name.to_owned(),
            });
        };
        route.subscribe(&self.shared, name).await
    }
}
