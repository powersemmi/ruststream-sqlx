//! The type of a field playing `headers`, shared by the inbox and the outbox.

#[cfg(feature = "json")]
use std::collections::BTreeMap;
#[cfg(feature = "json")]
use std::mem;

use ruststream::HeaderMap;
#[cfg(feature = "json")]
use sqlx::types::Json;

/// The type of a field playing `headers`: what the delivery's header map holds.
///
/// `sqlx::types::Json<BTreeMap<String, String>>` implements it under the `json` feature (a
/// `jsonb` or `json` column of string values), and so does an `Option` of any implementation.
/// A publish carrying a header the column cannot hold byte for byte is refused before the
/// service's [`Publish`](crate::Publish) runs: a header never reaches the table changed.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "inbox", feature = "postgres"))]
/// # mod demo {
/// use ruststream::HeaderMap;
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::HeaderColumn;
/// use serde::Deserialize;
/// use sqlx::PgPool;
///
/// /// Headers stored one per line, `name: value`, in a `TEXT` column.
/// #[derive(sqlx::Type)]
/// #[sqlx(transparent)]
/// pub struct Lines(String);
///
/// impl HeaderColumn for Lines {
///     fn take_headers(&mut self) -> HeaderMap {
///         let headers = self
///             .0
///             .lines()
///             .filter_map(|line| line.split_once(": "))
///             .map(|(name, value)| (name.to_owned(), value.to_owned()))
///             .collect();
///         self.0.clear();
///         headers
///     }
///
///     fn from_headers(headers: &HeaderMap) -> Self {
///         let lines: Vec<String> = headers
///             .iter()
///             .map(|(name, value)| format!("{name}: {}", String::from_utf8_lossy(value)))
///             .collect();
///         Self(lines.join("\n"))
///     }
///
///     fn unfit(headers: &HeaderMap) -> Option<&str> {
///         // A value comes back as it went in when it is text on one line.
///         let fits = |value: &[u8]| str::from_utf8(value).is_ok_and(|text| !text.contains('\n'));
///         headers
///             .iter()
///             .find(|(_, value)| !fits(value))
///             .map(|(name, _)| name)
///     }
/// }
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "jobs")]
/// pub struct Job {
///     #[field(id, generated)]
///     id: i64,
///     #[field(headers)]
///     headers: Lines,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Task {
///     n: u32,
/// }
///
/// // The delivery's headers come from `headers`, so the service's middleware reads `x-tenant`
/// // there as on any broker.
/// #[subscriber(InboxQueue::<Job>::new("tasks"))]
/// async fn run(task: &Task) -> HandlerOutcome {
///     tracing::info!(task.n, "running");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("worker", "0.1.0")).with_broker(SqlxBroker::new(pool), |b| {
///         b.include(run);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait HeaderColumn {
    /// Moves the headers the column holds into a header map, leaving the column empty.
    ///
    /// A delivery takes them once, when its row is claimed, so a column that owns its strings
    /// hands them over without a copy.
    fn take_headers(&mut self) -> HeaderMap;

    /// The column value that holds `headers`, for a service's `Publish`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "json"))]
    /// # mod demo {
    /// use std::collections::BTreeMap;
    ///
    /// use ruststream::OutgoingMessage;
    /// use ruststream_sqlx::{HeaderColumn, Inbox, Publish};
    /// use sqlx::types::Json;
    /// use sqlx::{PgConnection, Postgres};
    ///
    /// #[derive(Inbox, sqlx::FromRow)]
    /// #[inbox(table = "jobs")]
    /// pub struct Job {
    ///     #[field(id, generated)]
    ///     id: i64,
    ///     #[field(headers)]
    ///     headers: Json<BTreeMap<String, String>>,
    ///     #[field(payload)]
    ///     payload: Vec<u8>,
    /// }
    ///
    /// impl Publish<Postgres> for Job {
    ///     async fn publish(
    ///         conn: &mut PgConnection,
    ///         message: &OutgoingMessage<'_>,
    ///     ) -> Result<(), sqlx::Error> {
    ///         // The `jsonb` column holds the message's headers as the delivery will read them back.
    ///         let headers = Json::<BTreeMap<String, String>>::from_headers(message.headers());
    ///         sqlx::query("INSERT INTO jobs (headers, payload) VALUES ($1, $2)")
    ///             .bind(headers)
    ///             .bind(message.payload())
    ///             .execute(conn)
    ///             .await?;
    ///         Ok(())
    ///     }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    fn from_headers(headers: &HeaderMap) -> Self;

    /// The first of `headers` the column cannot hold byte for byte, if any: the broker refuses a
    /// publish that carries it.
    fn unfit(headers: &HeaderMap) -> Option<&str>;
}

/// A record or a row whose headers live in one column: the field holding the column, which the
/// table's description names with its `headers` setter.
///
/// `#[derive(Outbox)]` implements it for the field playing `headers`; a record described by hand
/// implements it where its `OutboxSpec` sets `outbox::spec::Headers`, and a
/// republished record carries the headers it took out of the field.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "outbox", feature = "json", feature = "postgres"))]
/// # mod demo {
/// # use ruststream::OutgoingMessage;
/// use std::collections::BTreeMap;
///
/// use ruststream_sqlx::dialect::Column;
/// use ruststream_sqlx::outbox::spec::Headers;
/// use ruststream_sqlx::outbox::{self, OutboxSpec, OutboxTable};
/// use ruststream_sqlx::{HeaderColumn, HeaderRow};
/// use sqlx::types::Json;
/// use sqlx::{PgConnection, Postgres};
///
/// #[derive(sqlx::FromRow)]
/// pub struct OrderEvent {
///     id: i64,
///     name: String,
///     payload: Vec<u8>,
///     headers: Option<Json<BTreeMap<String, String>>>,
/// }
///
/// impl HeaderRow for OrderEvent {
///     type Column = Option<Json<BTreeMap<String, String>>>;
///
///     fn headers_mut(&mut self) -> &mut Self::Column {
///         &mut self.headers
///     }
/// }
///
/// impl OutboxTable for OrderEvent {
///     type Id = i64;
///     type Table = OutboxSpec<(Headers,)>;
///     const TABLE: Self::Table = OutboxSpec::new(
///         "outbox",
///         Column::new("id"),
///         Column::new("name"),
///         Column::new("payload"),
///     )
///     .headers(Column::new("headers"));
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
/// // The record keeps what the message carried, so a republish sends the headers again.
/// impl outbox::Publish<Postgres> for OrderEvent {
///     async fn publish(conn: &mut PgConnection, msg: &OutgoingMessage<'_>) -> sqlx::Result<i64> {
///         sqlx::query_scalar(
///             "INSERT INTO outbox (name, payload, headers) VALUES ($1, $2, $3) RETURNING id",
///         )
///         .bind(msg.name())
///         .bind(msg.payload())
///         .bind(Option::<Json<BTreeMap<String, String>>>::from_headers(msg.headers()))
///         .fetch_one(conn)
///         .await
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` names a headers column and does not implement `HeaderRow`",
    label = "no `HeaderRow` for this record",
    note = "implement `HeaderRow` for `{Self}`, handing out the field that holds the headers \
            column, or drop `headers` from its description"
)]
pub trait HeaderRow {
    /// The column's type.
    type Column: HeaderColumn;

    /// The field holding the column, which the headers are taken out of.
    fn headers_mut(&mut self) -> &mut Self::Column;
}

impl<T: HeaderColumn> HeaderColumn for Option<T> {
    fn take_headers(&mut self) -> HeaderMap {
        self.as_mut()
            .map_or_else(HeaderMap::new, HeaderColumn::take_headers)
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        (!headers.is_empty()).then(|| T::from_headers(headers))
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        T::unfit(headers)
    }
}

/// A JSON object of strings. A value that is not UTF-8 does not fit: the broker refuses a publish
/// that carries one, so the replacement `from_headers` would write never reaches the table.
#[cfg(feature = "json")]
impl HeaderColumn for Json<BTreeMap<String, String>> {
    fn take_headers(&mut self) -> HeaderMap {
        mem::take(&mut self.0).into_iter().collect()
    }

    fn from_headers(headers: &HeaderMap) -> Self {
        Self(
            headers
                .iter()
                .map(|(name, value)| (name.to_owned(), String::from_utf8_lossy(value).into_owned()))
                .collect(),
        )
    }

    fn unfit(headers: &HeaderMap) -> Option<&str> {
        headers
            .iter()
            .find(|(_, value)| str::from_utf8(value).is_err())
            .map(|(name, _)| name)
    }
}
