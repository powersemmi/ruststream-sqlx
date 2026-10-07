//! The route table: a name's way into a table, exact or by prefix, and what each route runs.

use std::any::type_name;
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use foldhash::fast::RandomState;
use futures::future::BoxFuture;
use ruststream::OutgoingMessage;
use sqlx::Database;
use stackfuture::StackFuture;

#[cfg(feature = "testing")]
use super::insert;
use super::{TableWake, publish_row};
use crate::inbox::broker::Shared;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::Events;
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::events::Publish;
use crate::inbox::named::{self, ErasedStream};
use crate::inbox::queue::Description;
use crate::inbox::{FormDialect, PayloadRow};

/// The bytes a route keeps its write's future in. A row's `Publish` future that fits costs no
/// allocation, and a larger one is boxed. 1024 is the smallest power of two that holds the largest
/// route future of the test suites, the derive's insert (704 bytes).
pub(crate) const ROUTE_SLOT: usize = 1024;

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
        wake: &'static TableWake,
        message: &'a OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), SqlxBrokerError>>;

    /// What a by-name subscription knows of the route's table: its description, and whether its
    /// row can be read by role.
    fn description(&self) -> Description;

    /// Opens a by-name subscription to `name` of the route's table through the row's own code,
    /// its statements built by `form`.
    fn subscribe<'a>(
        &'a self,
        shared: &'a Arc<Shared<DB>>,
        form: &'a FormDialect,
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
        wake: &'static TableWake,
        message: &'a OutgoingMessage<'a>,
    ) -> BoxFuture<'a, Result<(), SqlxBrokerError>> {
        Box::pin(insert::<DB, Row>(shared, wake, message))
    }

    fn description(&self) -> Description {
        Description::of::<DB, Row>()
    }

    fn subscribe<'a>(
        &'a self,
        shared: &'a Arc<Shared<DB>>,
        form: &'a FormDialect,
        name: &'a str,
    ) -> BoxFuture<'a, Result<ErasedStream, SqlxBrokerError>> {
        named::erased::<DB, Row>(shared, form, name)
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

/// One route: its name, its way into the table, its table's form on the broker's dialect, and the
/// wake-up of its table's subscriptions.
struct Entry<DB: Database, Form, Wake> {
    name: Cow<'static, str>,
    route: Box<dyn Route<DB>>,
    form: Form,
    wake: Wake,
}

/// The routes a broker records, in registration order, and their index; a later route for a name
/// replaces an earlier one.
///
/// Each route carries its table's form: before `connect`, the way to reach it on the dialect the
/// broker connects with; after, the dialect seen through that form's trait. After `connect` each
/// also carries the wake-up of its table's subscriptions on the connection.
pub(crate) struct Routes<DB: Database, Form, Wake = ()> {
    routes: Vec<Entry<DB, Form, Wake>>,
    index: RouteIndex,
}

impl<DB: Database, Form, Wake> Default for Routes<DB, Form, Wake> {
    fn default() -> Self {
        Self {
            routes: Vec::new(),
            index: RouteIndex::default(),
        }
    }
}

impl<DB: Database, Form, Wake> fmt::Debug for Routes<DB, Form, Wake> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.routes
                    .iter()
                    .map(|entry| (&entry.name, entry.route.row())),
            )
            .finish()
    }
}

impl<DB: QueueDatabase, Form> Routes<DB, Form> {
    /// Leads `name` to `Row`'s table, whose form `form` reaches.
    pub(crate) fn add<Row>(&mut self, name: Cow<'static, str>, form: Form)
    where
        Row: Publish<DB> + Events<DB> + PayloadRow,
    {
        self.routes.retain(|entry| entry.name != name);
        self.routes.push(Entry {
            name,
            route: Box::new(TypedRoute::<Row>(PhantomData)),
            form,
            wake: (),
        });
        self.index = RouteIndex::new(
            self.routes
                .iter()
                .map(|entry| RouteName::parse(&entry.name)),
        );
    }
}

impl<DB: Database, Form> Routes<DB, Form> {
    /// The routes with each form turned by `resolve` and each table's wake-up found by `wake`, at
    /// the positions the index knows.
    pub(crate) fn resolve<Resolved>(
        self,
        resolve: impl Fn(Form) -> Resolved,
        wake: impl Fn(&dyn Route<DB>) -> &'static TableWake,
    ) -> Routes<DB, Resolved, &'static TableWake> {
        Routes {
            routes: self
                .routes
                .into_iter()
                .map(|entry| Entry {
                    wake: wake(entry.route.as_ref()),
                    name: entry.name,
                    route: entry.route,
                    form: resolve(entry.form),
                })
                .collect(),
            index: self.index,
        }
    }
}

impl<DB: Database, Form, Wake> Routes<DB, Form, Wake> {
    fn entry(&self, name: &str) -> Option<&Entry<DB, Form, Wake>> {
        self.index
            .find(name)
            .and_then(|position| self.routes.get(position))
    }

    /// The route `name` takes, with its table's form: one hash lookup for an exact name, then
    /// the prefixes.
    pub(crate) fn find(&self, name: &str) -> Option<(&dyn Route<DB>, &Form)> {
        self.entry(name)
            .map(|entry| (entry.route.as_ref(), &entry.form))
    }
}

impl<DB: Database, Form> Routes<DB, Form, &'static TableWake> {
    /// The route `name` takes, with the wake-up of its table's subscriptions: the lookup of
    /// [`find`](Self::find).
    pub(crate) fn find_with_wake(
        &self,
        name: &str,
    ) -> Option<(&dyn Route<DB>, &'static TableWake)> {
        self.entry(name)
            .map(|entry| (entry.route.as_ref(), entry.wake))
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
