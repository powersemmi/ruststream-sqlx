//! `Repository`: the typed publish policy of a row with `Publish`, and its publisher.

use std::any::type_name;
use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::Arc;

use ruststream::{Lend, OutgoingMessage, PairError, PublishPolicy, Publisher};
use ruststream_sqlx_dialect::Dialect;
use sqlx::Database;

use super::write;
use crate::inbox::broker::{ConnectedSqlxBroker, Shared};
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::Events;
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::events::Publish;

/// The typed publish policy of a row with [`Publish`]: a message published through it becomes a
/// row of `Row`'s table.
///
/// It names its table at compile time, so a publish costs a static call; a row without `Publish`
/// does not compile as a policy.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::OutgoingMessage;
/// use ruststream::prelude::*;
/// use ruststream_sqlx::{Inbox, InboxQueue, Publish, Repository, SqlxBroker};
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
/// struct Signup {
///     email: String,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "welcome")]
/// struct Welcome {
///     email: String,
/// }
///
/// #[subscriber(InboxQueue::<Job>::new("signups"), reply)]
/// async fn greet(signup: &Signup) -> Welcome {
///     Welcome { email: signup.email.clone() }
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("signup", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         // The welcome becomes a row of `jobs` in group `welcome`, with no route to look up.
///         b.include(greet).out_reply(Repository::<Job>::default());
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct Repository<Row>(PhantomData<fn() -> Row>);

impl<Row> Default for Repository<Row> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<Row> Clone for Repository<Row> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Row> Copy for Repository<Row> {}

impl<Row> fmt::Debug for Repository<Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Repository<{}>", type_name::<Row>())
    }
}

impl<DB, D, Row> PublishPolicy<ConnectedSqlxBroker<DB, D>> for Repository<Row>
where
    DB: QueueDatabase,
    D: Dialect + 'static,
    Row: Publish<DB> + Events<DB>,
{
    type Live = RepositoryPublisher<DB, Row>;

    fn pair(
        self,
        connected: &ConnectedSqlxBroker<DB, D>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        ready(Ok(RepositoryPublisher {
            shared: Arc::clone(&connected.shared),
            _row: PhantomData,
        }))
    }
}

/// The live form of a [`Repository`]: publishes into `Row`'s table through its [`Publish`].
///
/// After the broker shut down it answers [`SqlxBrokerError::Closed`], never a success.
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
/// # pub struct Job { #[field(id, generated)] id: i64, #[field(payload)] payload: Vec<u8> }
/// # impl Publish<Postgres> for Job {
/// #     async fn publish(_: &mut PgConnection, _: &OutgoingMessage<'_>) -> Result<(), sqlx::Error> { Ok(()) }
/// # }
/// use ruststream::{Broker, PublishPolicy, Publisher};
/// use ruststream_sqlx::{Repository, SqlxBroker};
///
/// // A task scheduled from outside any handler: an HTTP endpoint of the service.
/// pub async fn schedule(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
///     let connected = SqlxBroker::new(pool).connect().await?;
///     let jobs = Repository::<Job>::default().pair(&connected).await?;
///     jobs.publish(OutgoingMessage::new("reports", b"{}"), None).await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
pub struct RepositoryPublisher<DB: Database, Row> {
    shared: Arc<Shared<DB>>,
    _row: PhantomData<fn() -> Row>,
}

impl<DB: Database, Row> fmt::Debug for RepositoryPublisher<DB, Row> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RepositoryPublisher<{}>", type_name::<Row>())
    }
}

impl<DB, Row> Publisher for RepositoryPublisher<DB, Row>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
{
    type Payload = Lend;
    type Error = SqlxBrokerError;
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&()>,
    ) -> Result<(), SqlxBrokerError> {
        write::<DB, Row>(&self.shared, &msg).await
    }
}
