//! `OutboxSpec`, the typed builder of an outbox table's description, and `OutboxTable`, the trait a
//! record described by hand implements.

use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;
use std::str::FromStr;

use ruststream_sqlx_dialect::{Column, Form, TableSpec};

use super::{Declaration, Headers, OwnEvent, ProcessedAt, Push};

/// An outbox record described by hand: the trait `#[derive(Outbox)]` implements, and what a
/// service implements instead of the derive.
///
/// `TABLE` describes the table with [`OutboxSpec`], and `type Table` names the builder's final
/// type; the compiler holds the chain to it. The registry builds the default statements from
/// `TABLE` once, when the record type is registered, and runs the events the type names with
/// [`OutboxSpec::own`] from the record's own impls. The record reads its row through
/// `sqlx::FromRow`, and holds only the columns it reads.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// # use ruststream::memory::prelude::*;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Deserialize)] pub struct PlaceOrder { id: u64 }
/// # #[derive(Serialize, Deserialize, Outgoing)] #[outgoing(name = "orders")] pub struct OrderPlaced { id: u64 }
/// # #[subscriber("checkout", reply)] async fn place(cmd: &PlaceOrder) -> OrderPlaced { OrderPlaced { id: cmd.id } }
/// # #[subscriber("orders")] async fn fulfil(_: &OrderPlaced) -> HandlerOutcome { HandlerOutcome::ack() }
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::outbox::spec::ProcessedAt;
/// use ruststream_sqlx::outbox::{self, Outbox, OutboxSpec, OutboxTable, TrackedName};
/// use sqlx::{PgConnection, PgPool, Postgres};
///
/// #[derive(sqlx::FromRow)]
/// pub struct OrderEvent {
///     id: i64,
///     name: String,
///     payload: Vec<u8>,
/// }
///
/// impl OutboxTable for OrderEvent {
///     type Id = i64;
///     type Table = OutboxSpec<(ProcessedAt,)>;
///     const TABLE: Self::Table = OutboxSpec::new(
///         "outbox",
///         Column::new("id"),
///         Column::new("name"),
///         Column::new("payload"),
///     )
///     .processed_at(Column::new("processed_at"));
///
///     fn id(&self) -> &i64 {
///         &self.id
///     }
///
///     fn name(&self) -> &str {
///         &self.name
///     }
///
///     fn payload(&self) -> &[u8] {
///         &self.payload
///     }
/// }
///
/// impl outbox::Publish<Postgres> for OrderEvent {
///     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
///         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
///             .bind(msg.name())
///             .bind(msg.payload())
///             .fetch_one(conn)
///             .await
///     }
/// }
///
/// pub struct Orders;
///
/// impl TrackedName for Orders {
///     const NAME: &'static str = "orders";
/// }
///
/// pub fn app(pool: PgPool) -> impl App {
///     let tracking = Outbox::new(pool).track::<OrderEvent, Orders>();
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .layer(tracking.layer())
///         .publish_layer(tracking.publish_layer())
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(place).out_reply(Publish);
///             b.include(fulfil);
///             b.after_startup(Publish, tracking.republish());
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an outbox record",
    label = "this type does not describe an outbox table",
    note = "derive `Outbox` for `{Self}` and mark its `id`, `name` and `payload` fields, or \
            implement `OutboxTable` for it"
)]
pub trait OutboxTable: Sized + Send + Sync + Unpin + 'static {
    /// The type of the column that identifies a record: the value the id header carries.
    type Id: Display + FromStr + Send + Sync + 'static;

    /// The table's description as a type: `OutboxSpec<(..)>`, listing the markers the chain of
    /// `TABLE` sets, in the order it sets them.
    type Table: Described;

    /// The table's description.
    const TABLE: Self::Table;

    /// The record's id.
    fn id(&self) -> &Self::Id;

    /// The name the record was published under.
    fn name(&self) -> &str;

    /// The published payload.
    fn payload(&self) -> &[u8];
}

/// The description of an outbox table, with its settings as types.
///
/// `new` names the table and its three required columns: the id, the name a record was published
/// under, and the payload. The column-only setters (`within`, `data`, `selecting_all`) keep the
/// type. Each typed setter (`headers`, `processed_at`, `own`) adds its marker of
/// [`outbox::spec`](super) to the type, and a setting set twice does not compile. The outbox reads
/// the database's clock. Every setter is a `const fn`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "outbox")]
/// # mod demo {
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::outbox::OutboxSpec;
/// use ruststream_sqlx::outbox::spec::{Headers, ProcessedAt, own};
///
/// /// An outbox in the `shop` schema whose records keep their headers, are marked when
/// /// processed, and are recovered by the service's own statement.
/// pub const SHOP_OUTBOX: OutboxSpec<(Headers, ProcessedAt, own::Recover)> = OutboxSpec::new(
///     "outbox",
///     Column::new("id"),
///     Column::new("topic"),
///     Column::new("body"),
/// )
/// .within("shop")
/// .headers(Column::new("headers"))
/// .processed_at(Column::new("processed_at"))
/// .own::<own::Recover>();
/// # }
/// # fn main() {}
/// ```
pub struct OutboxSpec<Settings = ()> {
    spec: TableSpec<'static>,
    settings: PhantomData<fn() -> Settings>,
}

// By hand: a derive would require each marker of `Settings` to implement the trait, and the
// markers are never values.
impl<Settings> Debug for OutboxSpec<Settings> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxSpec")
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

impl<Settings> Clone for OutboxSpec<Settings> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Settings> Copy for OutboxSpec<Settings> {}

impl<Settings> PartialEq for OutboxSpec<Settings> {
    fn eq(&self, other: &Self) -> bool {
        self.spec == other.spec
    }
}

impl<Settings> Eq for OutboxSpec<Settings> {}

impl OutboxSpec {
    /// The outbox table `table`, whose records are identified by `id`, published under the name
    /// in `name`, with the bytes in `payload`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "outbox")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::OutboxSpec;
    ///
    /// /// The plainest outbox: a processed record is deleted.
    /// pub const OUTBOX: OutboxSpec = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// );
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn new(
        table: &'static str,
        id: Column<'static>,
        name: Column<'static>,
        payload: Column<'static>,
    ) -> Self {
        Self {
            spec: TableSpec::new(table, id, Form::RowLock)
                .group(name)
                .payload(payload)
                .database_clock(),
            settings: PhantomData,
        }
    }
}

impl<Settings> OutboxSpec<Settings> {
    /// The table's description, as the dialects read it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::{Column, OutboxDialect, Postgres, StatementError};
    /// use ruststream_sqlx::outbox::OutboxSpec;
    ///
    /// const OUTBOX: OutboxSpec = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// );
    ///
    /// /// The statement the outbox's default recovery runs on Postgres.
    /// pub fn recovery() -> Result<String, StatementError> {
    ///     Ok(Postgres.outbox_recover(&OUTBOX.spec())?.sql().to_owned())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn spec(&self) -> TableSpec<'static> {
        self.spec
    }

    /// The table in `schema`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "outbox")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::OutboxSpec;
    ///
    /// pub const OUTBOX: OutboxSpec = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// )
    /// .within("shop");
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn within(self, schema: &'static str) -> Self {
        Self {
            spec: self.spec.within(schema),
            ..self
        }
    }

    /// The columns the record reads besides its roles; the default fetch and recovery select them.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "outbox")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::OutboxSpec;
    ///
    /// /// A record that also reads when it was created.
    /// pub const OUTBOX: OutboxSpec = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// )
    /// .data(&[Column::new("created_at")]);
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn data(self, columns: &'static [Column<'static>]) -> Self {
        Self {
            spec: self.spec.data(columns),
            ..self
        }
    }

    /// The default fetch and recovery select every column (`SELECT *`): for a record whose
    /// columns the description cannot list, such as one that flattens another struct.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "outbox")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::OutboxSpec;
    ///
    /// pub const OUTBOX: OutboxSpec = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// )
    /// .selecting_all();
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn selecting_all(self) -> Self {
        Self {
            spec: self.spec.selecting_all(),
            ..self
        }
    }

    /// The column the published headers are kept in. The record implements
    /// [`HeaderRow`](crate::HeaderRow), and a republished record carries them again.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "outbox")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::OutboxSpec;
    /// use ruststream_sqlx::outbox::spec::Headers;
    ///
    /// pub const OUTBOX: OutboxSpec<(Headers,)> = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// )
    /// .headers(Column::new("headers"));
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn headers(
        self,
        column: Column<'static>,
    ) -> OutboxSpec<<Settings as Push<Headers>>::Out>
    where
        Settings: Push<Headers>,
        <Settings as Push<Headers>>::Out: Declaration,
    {
        Self::with(&self.spec.headers(column))
    }

    /// The column the default acknowledgement writes the database's current time to; the default
    /// fetch and recovery read only the records where it is `NULL`. Without it a processed
    /// record is deleted.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "outbox")]
    /// # mod demo {
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::OutboxSpec;
    /// use ruststream_sqlx::outbox::spec::ProcessedAt;
    ///
    /// pub const OUTBOX: OutboxSpec<(ProcessedAt,)> = OutboxSpec::new(
    ///     "outbox",
    ///     Column::new("id"),
    ///     Column::new("name"),
    ///     Column::new("payload"),
    /// )
    /// .processed_at(Column::new("processed_at"));
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn processed_at(
        self,
        column: Column<'static>,
    ) -> OutboxSpec<<Settings as Push<ProcessedAt>>::Out>
    where
        Settings: Push<ProcessedAt>,
        <Settings as Push<ProcessedAt>>::Out: Declaration,
    {
        Self::with(&self.spec.processed_at(column))
    }

    /// The record runs `Event` itself, through its impl of the event's trait of
    /// [`outbox`](mod@crate::outbox), instead of the crate's default; a record that names the event
    /// without the impl is not registered.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "outbox", feature = "postgres"))]
    /// # mod demo {
    /// # use ruststream::OutgoingMessage;
    /// use ruststream_sqlx::dialect::Column;
    /// use ruststream_sqlx::outbox::spec::{ProcessedAt, own};
    /// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
    /// use sqlx::{PgConnection, Postgres};
    ///
    /// #[derive(sqlx::FromRow)]
    /// pub struct OrderEvent {
    ///     id: i64,
    ///     name: String,
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl OutboxTable for OrderEvent {
    ///     type Id = i64;
    ///     type Table = OutboxSpec<(ProcessedAt, own::Retry)>;
    ///     const TABLE: Self::Table = OutboxSpec::new(
    ///         "outbox",
    ///         Column::new("id"),
    ///         Column::new("name"),
    ///         Column::new("payload"),
    ///     )
    ///     .processed_at(Column::new("processed_at"))
    ///     .own::<own::Retry>();
    ///
    ///     fn id(&self) -> &i64 {
    ///         &self.id
    ///     }
    ///
    ///     fn name(&self) -> &str {
    ///         &self.name
    ///     }
    ///
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    /// }
    /// # impl outbox::Publish<Postgres> for OrderEvent {
    /// #     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
    /// #         sqlx::query_scalar("INSERT INTO outbox (name, payload) VALUES ($1, $2) RETURNING id")
    /// #             .bind(msg.name()).bind(msg.payload()).fetch_one(conn).await
    /// #     }
    /// # }
    ///
    /// // The service counts how often a consumer asked for a record again.
    /// impl outbox::Retry<Postgres> for OrderEvent {
    ///     async fn retry(conn: &mut PgConnection, id: &i64) -> sqlx::Result<()> {
    ///         sqlx::query("UPDATE outbox SET retries = retries + 1 WHERE id = $1")
    ///             .bind(id)
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn own<Event: OwnEvent>(self) -> OutboxSpec<<Settings as Push<Event>>::Out>
    where
        Settings: Push<Event>,
        <Settings as Push<Event>>::Out: Declaration,
    {
        Self::with(&self.spec)
    }

    const fn with<Next>(spec: &TableSpec<'static>) -> OutboxSpec<Next> {
        OutboxSpec {
            spec: *spec,
            settings: PhantomData,
        }
    }
}

/// An outbox table's description: [`OutboxSpec`], with settings that fold into one
/// [`Declaration`]. Machinery behind [`OutboxTable::Table`].
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not the description of an outbox table",
    label = "not an `OutboxSpec`",
    note = "describe the table with `OutboxSpec::new(..)` and name the chain's type: `type Table = \
            OutboxSpec<(..)>`"
)]
pub trait Described: Copy + Send + Sync + 'static {
    /// The settings, folded.
    type Declared: Declaration;

    /// The table's description, as the dialects read it.
    fn spec(&self) -> TableSpec<'static>;
}

impl<Settings: Declaration + 'static> Described for OutboxSpec<Settings> {
    type Declared = Settings;

    fn spec(&self) -> TableSpec<'static> {
        self.spec
    }
}
