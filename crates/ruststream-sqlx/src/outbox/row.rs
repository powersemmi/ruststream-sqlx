//! What the registry reads off a record, for every [`OutboxTable`].

use super::dispatch::{Declared, Slot};
use super::spec::{Declaration, OutboxTable};

/// A record of an outbox table: a struct deriving [`Outbox`](derive@crate::Outbox) or
/// implementing [`OutboxTable`].
///
/// The crate implements it for every [`OutboxTable`]; a service names it only as a bound.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an outbox record",
    label = "this type does not describe an outbox table",
    note = "derive `Outbox` for `{Self}` and mark its `id`, `name` and `payload` fields, or \
            implement `OutboxTable` for it"
)]
pub trait OutboxRow: OutboxTable {
    /// Whether the record's `Retry` writes anything: `false` for the default, which leaves the
    /// record as it is, so the subscription layer takes no connection for it.
    #[doc(hidden)]
    const RETRY_WRITES: bool;
}

impl<Record> OutboxRow for Record
where
    Record: OutboxTable,
    <Declared<Record> as Declaration>::OwnRetry: Slot,
{
    const RETRY_WRITES: bool = <<Declared<Record> as Declaration>::OwnRetry as Slot>::SET;
}
