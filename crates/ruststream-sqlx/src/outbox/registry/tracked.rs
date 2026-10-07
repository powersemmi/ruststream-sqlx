//! Names tracked as types: a name tracked twice stops the build.

use crate::outbox::error::OutboxError;
use ruststream::runtime::{Context, Handler, HandlerOutcome};
use ruststream::{Bytes, OutgoingMessage, Publisher};
use sqlx::{Database, Pool};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::OnceLock;

use super::list::{Nil, RecordList, RecordNames, Registered};

/// A name the outbox tracks, as a type: [`Outbox::track`] refuses a name tracked twice while the
/// service compiles.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use ruststream_sqlx::{Outbox, outbox};
/// # use serde::{Deserialize, Serialize};
/// # use sqlx::postgres::{PgConnection, PgPool, Postgres};
/// # #[derive(Outbox, sqlx::FromRow)]
/// # #[outbox(table = "outbox")]
/// # pub struct OrderOutbox { #[field(id)] id: i64, #[field(name)] name: String, #[field(payload)] payload: Vec<u8> }
/// # impl outbox::Publish<Postgres> for OrderOutbox {
/// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
/// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
/// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
/// #     }
/// # }
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream_sqlx::outbox::TrackedName;
///
/// /// The name `OrderPlaced` is published under, as a type.
/// pub struct Orders;
///
/// impl TrackedName for Orders {
///     const NAME: &'static str = "orders";
/// }
///
/// #[derive(Serialize, Deserialize, Outgoing)]
/// #[outgoing(name = "orders")]
/// pub struct OrderPlaced {
///     id: u64,
/// }
///
/// pub fn app(pool: PgPool) -> impl App {
///     let tracking = Outbox::new(pool).track::<OrderOutbox, Orders>();
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(tracking.layer())
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a name the outbox tracks",
    label = "not a `TrackedName`",
    note = "implement `TrackedName` for `{Self}` with the name as `NAME`, or register the name as \
            a string with `register`"
)]
pub trait TrackedName: 'static {
    /// The name.
    const NAME: &'static str;
}

/// A name tracked by type, kept in the registry's type for the compile-time check; it holds no
/// record and forwards every call. Machinery behind [`Outbox::track`].
#[derive(Debug)]
pub struct Checked<Name, Rest>(pub(super) Rest, pub(super) PhantomData<fn() -> Name>);

impl<Name, Rest: Clone> Clone for Checked<Name, Rest> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<Name, Rest: Copy> Copy for Checked<Name, Rest> {}

/// Whether a list of registrations lacks `Name` among the names tracked by type. Machinery.
#[doc(hidden)]
pub trait Lacks<Name: TrackedName> {
    /// `true` when no node tracks `Name::NAME` by type.
    const LACKS: bool;
}

impl<Name: TrackedName> Lacks<Name> for Nil {
    const LACKS: bool = true;
}

impl<Name: TrackedName, Record, Rest: Lacks<Name>> Lacks<Name> for Registered<Record, Rest> {
    const LACKS: bool = <Rest as Lacks<Name>>::LACKS;
}

impl<Name: TrackedName, Earlier: TrackedName, Rest: Lacks<Name>> Lacks<Name>
    for Checked<Earlier, Rest>
{
    const LACKS: bool = !same(Earlier::NAME, Name::NAME) && <Rest as Lacks<Name>>::LACKS;
}

/// Whether two names are one, while the service compiles.
const fn same(one: &str, other: &str) -> bool {
    let (one, other) = (one.as_bytes(), other.as_bytes());
    if one.len() != other.len() {
        return false;
    }
    let mut index = 0;
    while index < one.len() {
        if one[index] != other[index] {
            return false;
        }
        index += 1;
    }
    true
}

impl<Name: 'static, Rest: RecordNames> RecordNames for Checked<Name, Rest> {
    #[inline]
    fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }
}

impl<DB: Database, Name: 'static, Rest: RecordList<DB>> RecordList<DB> for Checked<Name, Rest> {
    fn record<'a>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        msg: &'a OutgoingMessage<'_>,
    ) -> impl Future<Output = Option<Result<Bytes, OutboxError>>> + Send + 'a {
        self.0.record(pool, msg)
    }

    fn deliver<'a, M, C, S, H>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        handler: &'a H,
        msg: &'a M,
        ctx: &'a mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send + 'a
    where
        M: Sync,
        C: Send,
        S: Send + Sync,
        H: Handler<M, C, S>,
    {
        self.0.deliver(pool, handler, msg, ctx)
    }

    fn republish<'a, Live: Publisher>(
        &'a self,
        pool: &'a OnceLock<Pool<DB>>,
        publisher: &'a Live,
        only: Option<&'a [&'static str]>,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send + 'a {
        self.0.republish(pool, publisher, only)
    }
}

#[cfg(test)]
mod tests {
    use super::{Checked, Lacks, Nil, PhantomData, RecordNames, Registered, TrackedName};
    use crate::outbox::registry::list::tests::node;

    struct Orders;

    impl TrackedName for Orders {
        const NAME: &'static str = "orders";
    }

    struct Order;

    impl TrackedName for Order {
        const NAME: &'static str = "order";
    }

    struct Refunds;

    impl TrackedName for Refunds {
        const NAME: &'static str = "refunds";
    }

    type Tracked = Registered<(), Checked<Refunds, Registered<(), Checked<Orders, Nil>>>>;

    #[test]
    fn a_name_tracked_by_type_is_found_by_its_text_alone() {
        const {
            assert!(!<Tracked as Lacks<Orders>>::LACKS);
            assert!(!<Tracked as Lacks<Refunds>>::LACKS);
            assert!(<Tracked as Lacks<Order>>::LACKS);
            assert!(<Nil as Lacks<Orders>>::LACKS);
        }
        let names = node(
            "refunds",
            Checked(node("orders", Nil), PhantomData::<fn() -> Refunds>),
        );
        assert!(names.contains("orders"));
        assert!(names.contains("refunds"));
        assert!(!names.contains("order"));
    }
}
