//! The contract `#[derive(Outbox)]` implements: what the registry reads off a record.

use std::fmt::Display;
use std::str::FromStr;

use ruststream::HeaderMap;

/// A record of an outbox table: a struct deriving [`Outbox`](derive@crate::Outbox).
///
/// The derive implements it from the fields' roles; a service names it only as a bound.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an outbox record",
    label = "this type does not derive `Outbox`",
    note = "derive `Outbox` for `{Self}` and mark its `id`, `name` and `payload` fields"
)]
pub trait OutboxRow: Sized + Send + Sync + Unpin + 'static {
    /// The type of the `id` field: the value the id header carries.
    type Id: Display + FromStr + Send + Sync + 'static;

    /// Whether the record's `Retry` writes anything: `false` for the default, which leaves the
    /// record as it is, so the subscription layer takes no connection for it.
    #[doc(hidden)]
    const RETRY_WRITES: bool;

    /// The record's id.
    #[doc(hidden)]
    fn id(&self) -> &Self::Id;

    /// The name the record was published under.
    #[doc(hidden)]
    fn name(&self) -> &str;

    /// The published payload.
    #[doc(hidden)]
    fn payload(&self) -> &[u8];

    /// The published headers, moved out of the `headers` field; empty without one.
    #[doc(hidden)]
    fn take_headers(&mut self) -> HeaderMap;
}
