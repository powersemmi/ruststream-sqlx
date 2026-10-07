//! `Routed`: the broker's default publish policy, which publishes where the route table leads a
//! name.

use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;

use ruststream::{DefaultPublish, Lend, OutgoingMessage, PairError, PublishPolicy, Publisher};
use ruststream_sqlx_dialect::Dialect;
use sqlx::Database;

use super::insert_routed;
use crate::inbox::broker::{ConnectedSqlxBroker, Shared};
use crate::inbox::database::QueueDatabase;
use crate::inbox::error::SqlxBrokerError;

/// The broker's default publish policy: a publish goes where the route table leads its name.
///
/// A name given per message costs one hash lookup and one dynamic call into the row's
/// [`Publish`](crate::Publish). Prefix routes are scanned only when no route names the message
/// exactly. The call's future stays in place, in 1024 bytes, so a publish allocates what a
/// [`Repository`](crate::Repository) publish does. A row whose `Publish` future is larger is
/// boxed: one allocation per publish on its route. A name no route leads anywhere fails the
/// publish with [`SqlxBrokerError::NoRoute`] and logs a warning naming it. Replies and `Out` slots
/// without a policy of their own publish through it.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, InboxQueue, Publish, Routed, SqlxBroker};
/// use serde::{Deserialize, Serialize};
/// use sqlx::{PgConnection, PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs")]
/// pub struct Job {
///     #[field(id, generated)]
///     id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for Job {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO jobs (name, payload) VALUES ($1, $2)")
///             .bind(message.name())
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "invoices")]
/// struct Invoice {
///     order: u64,
/// }
///
/// #[subscriber(InboxQueue::<Job>::new("orders"), reply)]
/// async fn bill(order: &Order) -> Invoice {
///     Invoice { order: order.id }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     let broker = SqlxBroker::new(pool).route::<Job>("invoices");
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
///         b.include(bill).out_reply(Routed);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Routed;

impl<DB: QueueDatabase, D: Dialect + 'static> PublishPolicy<ConnectedSqlxBroker<DB, D>> for Routed {
    type Live = RoutedPublisher<DB>;

    fn pair(
        self,
        connected: &ConnectedSqlxBroker<DB, D>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        ready(Ok(RoutedPublisher {
            shared: Arc::clone(&connected.shared),
        }))
    }
}

impl<DB: QueueDatabase, D: Dialect + 'static> DefaultPublish for ConnectedSqlxBroker<DB, D> {
    type Policy = Routed;
}

/// The live form of [`Routed`]: publishes into the table the route table leads each name to.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream_sqlx::prelude::*;
/// use serde::Serialize;
/// use sqlx::{PgConnection, PgPool, Postgres};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs")]
/// pub struct Job {
///     #[field(id, generated)]
///     id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// impl Publish<Postgres> for Job {
///     async fn publish(
///         conn: &mut PgConnection,
///         message: &OutgoingMessage<'_>,
///     ) -> Result<(), sqlx::Error> {
///         sqlx::query("INSERT INTO jobs (name, payload) VALUES ($1, $2)")
///             .bind(message.name())
///             .bind(message.payload())
///             .execute(conn)
///             .await?;
///         Ok(())
///     }
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders")]
/// pub struct Order {
///     id: u64,
/// }
///
/// pub async fn run(pool: PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let broker = SqlxBroker::new(pool).route::<Job>("orders").bindable();
///     let orders = broker.bind(Routed);
///     let running = RustStream::new(AppInfo::new("shop", "0.1.0"))
///         .with_broker(broker, |_b| {})
///         .start()
///         .await?;
///
///     // The HTTP task owns this `RoutedPublisher`: each order becomes a row of `jobs`.
///     let publisher = running.publisher(orders).await?;
///     publisher.message(&Order { id: 7 }).publish().await?;
///
///     running.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct RoutedPublisher<DB: Database> {
    shared: Arc<Shared<DB>>,
}

impl<DB: Database> fmt::Debug for RoutedPublisher<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutedPublisher")
            .field("routes", &self.shared.routes)
            .finish()
    }
}

impl<DB: QueueDatabase> Publisher for RoutedPublisher<DB> {
    type Payload = Lend;
    type Error = SqlxBrokerError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> Result<(), SqlxBrokerError> {
        let Some((route, _)) = self.shared.routes.find(msg.name()) else {
            tracing::warn!(
                target: "ruststream_sqlx",
                name = msg.name(),
                "no route leads the name to a table; the publish fails",
            );
            return Err(SqlxBrokerError::NoRoute {
                name: msg.name().to_owned(),
            });
        };
        #[cfg(feature = "testing")]
        self.shared.harness.expect(msg.name());
        let inserted = insert_routed(&self.shared, route, &msg).await;
        #[cfg(feature = "testing")]
        match &inserted {
            Ok(()) => self.shared.harness.published(&msg),
            Err(_) => self.shared.harness.refused(msg.name()),
        }
        inserted
    }
}
