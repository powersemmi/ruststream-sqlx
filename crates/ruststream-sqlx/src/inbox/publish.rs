//! Publishing into tables: the typed repository, and the route table the broker publishes through
//! by default.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use ruststream::{DefaultPublish, Lend, OutgoingMessage, PairError, PublishPolicy, Publisher};
use sqlx::Database;

use super::InboxRow;
use super::broker::{ConnectedSqlxBroker, Shared};
use super::database::QueueDatabase;
use super::error::SqlxBrokerError;
use super::events::Publish;

/// The table of `Row`, qualified with its schema, for messages.
pub(crate) fn table_of<Row: InboxRow>() -> String {
    let spec = Row::SPEC;
    spec.schema().map_or_else(
        || spec.table().to_owned(),
        |schema| format!("{schema}.{}", spec.table()),
    )
}

/// Writes `message` into `Row`'s table on a connection of the pool, unless the broker is shut
/// down.
async fn write<DB, Row>(
    shared: &Shared<DB>,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Publish<DB>,
{
    let failed = |source| SqlxBrokerError::Publish {
        name: message.name().to_owned(),
        table: table_of::<Row>(),
        row: type_name::<Row>(),
        source: Box::new(source),
    };
    // Why a run-time check: the pool is the service's and outlives the broker, so only the
    // broker's own flag can refuse a publisher handed out before shutdown.
    if shared.closed.is_cancelled() {
        return Err(SqlxBrokerError::Closed);
    }
    let mut conn = shared.pool.acquire().await.map_err(failed)?;
    Row::publish(&mut conn, message).await.map_err(failed)
}

/// The typed publish policy of a row with [`Publish`]: a message published through it becomes a
/// row of `Row`'s table.
///
/// It names its table at compile time, so a publish costs a static call; a row without `Publish`
/// does not compile as a policy.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher};
/// use ruststream_sqlx::{Inbox, Publish, Repository, SqlxBroker};
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
/// // A welcome scheduled from an HTTP endpoint: a row of `jobs` in group `welcome`, written with
/// // no route to look up.
/// pub async fn welcome(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
///     let connected = SqlxBroker::new(pool).connect().await?;
///     let jobs = Repository::<Job>::default().pair(&connected).await?;
///     jobs.publish(OutgoingMessage::new("welcome", br#"{"email":"a@b"}"#), None).await?;
///     Ok(())
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

impl<DB, Row> PublishPolicy<ConnectedSqlxBroker<DB>> for Repository<Row>
where
    DB: QueueDatabase,
    Row: Publish<DB>,
{
    type Live = RepositoryPublisher<DB, Row>;

    fn pair(
        self,
        connected: &ConnectedSqlxBroker<DB>,
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
    Row: Publish<DB>,
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

/// A name's way into a table.
pub(crate) trait Route<DB: Database>: Send + Sync {
    /// Writes `message` into the route's table.
    fn publish<'a>(
        &'a self,
        shared: &'a Shared<DB>,
        message: &'a OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), SqlxBrokerError>>;

    /// The row type, for messages.
    fn row(&self) -> &'static str;
}

struct TypedRoute<Row>(PhantomData<fn() -> Row>);

impl<DB, Row> Route<DB> for TypedRoute<Row>
where
    DB: QueueDatabase,
    Row: Publish<DB>,
{
    fn publish<'a>(
        &'a self,
        shared: &'a Shared<DB>,
        message: &'a OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), SqlxBrokerError>> {
        Box::pin(write::<DB, Row>(shared, message))
    }

    fn row(&self) -> &'static str {
        type_name::<Row>()
    }
}

/// A route's name: exact, or a prefix written with a trailing `*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteName<'a> {
    Exact(&'a str),
    Prefix(&'a str),
}

impl<'a> RouteName<'a> {
    pub(crate) fn parse(name: &'a str) -> Self {
        name.strip_suffix('*')
            .map_or(Self::Exact(name), Self::Prefix)
    }

    /// The position among `names` of the route `name` takes: the exact one, else the longest
    /// prefix.
    pub(crate) fn best(names: impl Iterator<Item = Self>, name: &str) -> Option<usize> {
        let mut prefix: Option<(usize, usize)> = None;
        for (position, route) in names.enumerate() {
            match route {
                Self::Exact(exact) if exact == name => return Some(position),
                Self::Prefix(start)
                    if name.starts_with(start)
                        && prefix.is_none_or(|(_, longest)| start.len() > longest) =>
                {
                    prefix = Some((position, start.len()));
                }
                _ => {}
            }
        }
        prefix.map(|(position, _)| position)
    }
}

/// The routes a broker records, in registration order; a later route for a name replaces an
/// earlier one.
pub(crate) struct Routes<DB: Database> {
    routes: Vec<(Cow<'static, str>, Box<dyn Route<DB>>)>,
}

impl<DB: Database> Default for Routes<DB> {
    fn default() -> Self {
        Self { routes: Vec::new() }
    }
}

impl<DB: Database> fmt::Debug for Routes<DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.routes.iter().map(|(name, route)| (name, route.row())))
            .finish()
    }
}

impl<DB: QueueDatabase> Routes<DB> {
    pub(crate) fn add<Row: Publish<DB>>(&mut self, name: Cow<'static, str>) {
        self.routes.retain(|(existing, _)| *existing != name);
        self.routes
            .push((name, Box::new(TypedRoute::<Row>(PhantomData))));
    }
}

impl<DB: Database> Routes<DB> {
    /// The route `name` takes.
    pub(crate) fn find(&self, name: &str) -> Option<&dyn Route<DB>> {
        let names = self.routes.iter().map(|(route, _)| RouteName::parse(route));
        RouteName::best(names, name).map(|position| self.routes[position].1.as_ref())
    }
}

/// The broker's default publish policy: a publish goes where the route table leads its name.
///
/// A name given per message costs one route lookup and one dynamic call; a name no route leads
/// anywhere fails the publish with [`SqlxBrokerError::NoRoute`] and logs a warning naming it.
/// Replies and `Out` slots without a policy of their own publish through it.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher};
/// use ruststream_sqlx::{Inbox, Publish, Routed, SqlxBroker};
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
/// // Invoices become rows of `jobs`: the route leads the name there.
/// pub async fn invoice(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
///     let connected = SqlxBroker::new(pool).route::<Job>("invoices").connect().await?;
///     let publisher = Routed.pair(&connected).await?;
///     publisher.publish(OutgoingMessage::new("invoices", br#"{"order":7}"#), None).await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Routed;

impl<DB: QueueDatabase> PublishPolicy<ConnectedSqlxBroker<DB>> for Routed {
    type Live = RoutedPublisher<DB>;

    fn pair(
        self,
        connected: &ConnectedSqlxBroker<DB>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> + Send {
        ready(Ok(RoutedPublisher {
            shared: Arc::clone(&connected.shared),
        }))
    }
}

impl<DB: QueueDatabase> DefaultPublish for ConnectedSqlxBroker<DB> {
    type Policy = Routed;
}

/// The live form of [`Routed`]: publishes into the table the route table leads each name to.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "postgres")]
/// # async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher};
/// use ruststream_sqlx::{Routed, SqlxBroker, SqlxBrokerError};
///
/// let connected = SqlxBroker::new(pool).connect().await?;
/// let publisher = Routed.pair(&connected).await?;
/// // No route was recorded, so the publish says so instead of writing nowhere.
/// let refused = publisher.publish(OutgoingMessage::new("orders", b"{}"), None).await;
/// assert!(matches!(refused, Err(SqlxBrokerError::NoRoute { .. })));
/// # Ok(())
/// # }
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
        if self.shared.closed.is_cancelled() {
            return Err(SqlxBrokerError::Closed);
        }
        let Some(route) = self.shared.routes.find(msg.name()) else {
            tracing::warn!(
                target: "ruststream_sqlx",
                name = msg.name(),
                "no route leads the name to a table; the publish fails",
            );
            return Err(SqlxBrokerError::NoRoute {
                name: msg.name().to_owned(),
            });
        };
        route.publish(&self.shared, &msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::RouteName;

    #[test]
    fn an_exact_route_wins_over_a_prefix_and_the_longest_prefix_wins() {
        let names = [
            RouteName::parse("reports.*"),
            RouteName::parse("reports.daily"),
            RouteName::parse("reports.daily.*"),
            RouteName::parse("*"),
        ];
        let pick = |name: &str| RouteName::best(names.iter().copied(), name);
        assert_eq!(pick("reports.daily"), Some(1));
        assert_eq!(pick("reports.daily.eu"), Some(2));
        assert_eq!(pick("reports.weekly"), Some(0));
        assert_eq!(pick("orders"), Some(3));
        assert_eq!(
            RouteName::best([RouteName::parse("emails")].into_iter(), "orders"),
            None
        );
    }
}
