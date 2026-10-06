//! Publishing into tables: the typed repository, and the route table the broker publishes through
//! by default.

use std::any::type_name;
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt;
use std::future::{Future, ready};
use std::marker::PhantomData;
use std::sync::Arc;

use foldhash::fast::RandomState;
use futures::future::BoxFuture;
use ruststream::{DefaultPublish, Lend, OutgoingMessage, PairError, PublishPolicy, Publisher};
use ruststream_sqlx_dialect::TableSpec;
use sqlx::Database;
#[cfg(feature = "testing")]
use sqlx::Pool;
use stackfuture::StackFuture;

use super::PayloadRow;
use super::broker::{ConnectedSqlxBroker, Shared};
use super::database::QueueDatabase;
use super::engine::Events;
use super::error::SqlxBrokerError;
use super::events::Publish;
use super::named::{self, ErasedStream};
use super::queue::Description;
#[cfg(feature = "testing")]
use super::testing::{cancelled, off_clock};

/// The bytes a route keeps its write's future in. A row's `Publish` future that fits costs no
/// allocation, and a larger one is boxed. 1024 is the smallest power of two that holds the largest
/// route future of the test suites, the derive's insert (704 bytes).
pub(crate) const ROUTE_SLOT: usize = 1024;

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
        return publish_off_clock::<DB, Row>(&shared.pool, message)
            .await
            .map_err(failed);
    }
    let mut conn = shared.pool.acquire().await.map_err(failed)?;
    Row::publish(&mut conn, message).await.map_err(failed)
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

/// Writes `message` through `route` on a connection of the pool, unless the broker is shut down:
/// the route table's write, for the broker's publishes and the harness's injections alike.
pub(crate) async fn insert_routed<DB: QueueDatabase>(
    shared: &Shared<DB>,
    route: &dyn Route<DB>,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError> {
    // Why a run-time check: the pool is the service's and outlives the broker, so only the
    // broker's own flag can refuse a publisher handed out before shutdown.
    if shared.is_closed() {
        return Err(SqlxBrokerError::Closed);
    }
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        return route.insert_in_process(shared, message).await;
    }
    let mut conn = shared.pool.acquire().await.map_err(|source| {
        let description = route.description();
        failed(message, &description.spec, description.row, source)
    })?;
    route.insert(&mut conn, message).await
}

/// The insert of an in-process connection, off a paused clock.
#[cfg(feature = "testing")]
async fn publish_off_clock<DB, Row>(
    pool: &Pool<DB>,
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
        Row::publish(&mut conn, &message).await
    })
    .await
    .unwrap_or_else(|| Err(cancelled()))
}

/// A publisher's write: the insert, then the harness's books of what the broker published.
async fn write<DB, Row>(
    shared: &Shared<DB>,
    message: &OutgoingMessage<'_>,
) -> Result<(), SqlxBrokerError>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
{
    #[cfg(feature = "testing")]
    shared.harness.expect(message.name());
    let inserted = insert::<DB, Row>(shared, message).await;
    #[cfg(feature = "testing")]
    match &inserted {
        Ok(()) => shared.harness.published(message),
        Err(_) => shared.harness.refused(message.name()),
    }
    inserted
}

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

impl<DB, Row> PublishPolicy<ConnectedSqlxBroker<DB>> for Repository<Row>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB>,
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

/// A name's way into a table.
pub(crate) trait Route<DB: Database>: Send + Sync {
    /// Writes `message` into the route's table on `conn`, unless the table cannot hold one of its
    /// headers. The future stays in place unless it is larger than [`ROUTE_SLOT`].
    fn insert<'a>(
        &'a self,
        conn: &'a mut DB::Connection,
        message: &'a OutgoingMessage<'a>,
    ) -> StackFuture<'a, Result<(), SqlxBrokerError>, ROUTE_SLOT>;

    /// Writes `message` into the route's table on an in-process connection, off a paused clock.
    #[cfg(feature = "testing")]
    fn insert_in_process<'a>(
        &'a self,
        shared: &'a Shared<DB>,
        message: &'a OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), SqlxBrokerError>>;

    /// What a by-name subscription knows of the route's table: its description, and whether its
    /// row can be read by role.
    fn description(&self) -> Description;

    /// Opens a by-name subscription to `name` of the route's table through the row's own code.
    fn subscribe<'a>(
        &'a self,
        shared: &'a Arc<Shared<DB>>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<ErasedStream, SqlxBrokerError>>;

    /// The row type, for messages.
    fn row(&self) -> &'static str;
}

struct TypedRoute<Row>(PhantomData<fn() -> Row>);

impl<DB, Row> Route<DB> for TypedRoute<Row>
where
    DB: QueueDatabase,
    Row: Publish<DB> + Events<DB> + PayloadRow,
{
    fn insert<'a>(
        &'a self,
        conn: &'a mut DB::Connection,
        message: &'a OutgoingMessage<'a>,
    ) -> StackFuture<'a, Result<(), SqlxBrokerError>, ROUTE_SLOT> {
        StackFuture::from_or_box(publish_row::<DB, Row>(conn, message))
    }

    #[cfg(feature = "testing")]
    fn insert_in_process<'a>(
        &'a self,
        shared: &'a Shared<DB>,
        message: &'a OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), SqlxBrokerError>> {
        Box::pin(insert::<DB, Row>(shared, message))
    }

    fn description(&self) -> Description {
        Description::of::<DB, Row>()
    }

    fn subscribe<'a>(
        &'a self,
        shared: &'a Arc<Shared<DB>>,
        name: &'a str,
    ) -> BoxFuture<'a, Result<ErasedStream, SqlxBrokerError>> {
        named::erased::<DB, Row>(shared, name)
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
}

/// Where each name's route sits among the routes: exact names by hash, prefixes longest first.
#[derive(Debug, Default)]
struct RouteIndex {
    exact: HashMap<Box<str>, usize, RandomState>,
    prefixes: Vec<(Box<str>, usize)>,
}

impl RouteIndex {
    /// The index of `names`, each at its position.
    fn new<'a>(names: impl Iterator<Item = RouteName<'a>>) -> Self {
        let mut index = Self::default();
        for (position, name) in names.enumerate() {
            match name {
                RouteName::Exact(exact) => {
                    index.exact.insert(exact.into(), position);
                }
                RouteName::Prefix(prefix) => index.prefixes.push((prefix.into(), position)),
            }
        }
        index
            .prefixes
            .sort_by_key(|(prefix, _)| Reverse(prefix.len()));
        index
    }

    /// The position of the route `name` takes: the exact one, else the longest prefix.
    fn find(&self, name: &str) -> Option<usize> {
        self.exact.get(name).copied().or_else(|| {
            self.prefixes
                .iter()
                .find(|(prefix, _)| name.starts_with(&**prefix))
                .map(|&(_, position)| position)
        })
    }
}

/// The routes a broker records, in registration order, and their index; a later route for a name
/// replaces an earlier one.
pub(crate) struct Routes<DB: Database> {
    routes: Vec<(Cow<'static, str>, Box<dyn Route<DB>>)>,
    index: RouteIndex,
}

impl<DB: Database> Default for Routes<DB> {
    fn default() -> Self {
        Self {
            routes: Vec::new(),
            index: RouteIndex::default(),
        }
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
    pub(crate) fn add<Row>(&mut self, name: Cow<'static, str>)
    where
        Row: Publish<DB> + Events<DB> + PayloadRow,
    {
        self.routes.retain(|(existing, _)| *existing != name);
        self.routes
            .push((name, Box::new(TypedRoute::<Row>(PhantomData))));
        self.index = RouteIndex::new(self.routes.iter().map(|(name, _)| RouteName::parse(name)));
    }
}

impl<DB: Database> Routes<DB> {
    /// The route `name` takes: one hash lookup for an exact name, then the prefixes.
    pub(crate) fn find(&self, name: &str) -> Option<&dyn Route<DB>> {
        self.index
            .find(name)
            .and_then(|position| self.routes.get(position))
            .map(|(_, route)| route.as_ref())
    }
}

/// The broker's default publish policy: a publish goes where the route table leads its name.
///
/// A name given per message costs one hash lookup and one dynamic call into the row's
/// [`Publish`]. Prefix routes are scanned only when no route names the message exactly. The
/// call's future stays in place, in 1024 bytes, so a publish allocates what a [`Repository`]
/// publish does. A row whose `Publish` future is larger is boxed: one allocation per publish on
/// its route. A name no route leads anywhere fails the publish with [`SqlxBrokerError::NoRoute`]
/// and logs a warning naming it. Replies and `Out` slots without a policy of their own publish
/// through it.
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

#[cfg(test)]
mod tests {
    use super::{RouteIndex, RouteName};

    #[test]
    fn an_exact_route_wins_over_a_prefix_and_the_longest_prefix_wins() {
        let names = [
            RouteName::parse("reports.*"),
            RouteName::parse("reports.daily"),
            RouteName::parse("reports.daily.*"),
            RouteName::parse("*"),
        ];
        let index = RouteIndex::new(names.into_iter());
        assert_eq!(index.find("reports.daily"), Some(1));
        assert_eq!(index.find("reports.daily.eu"), Some(2));
        assert_eq!(index.find("reports.weekly"), Some(0));
        assert_eq!(index.find("orders"), Some(3));
        assert_eq!(
            RouteIndex::new([RouteName::parse("emails")].into_iter()).find("orders"),
            None
        );
    }
}
