//! One trait per event of an outbox record: what a service implements when it lists the event in
//! `#[outbox(custom(..))]`, and `Publish`, which has no default.

use std::future::Future;

use ruststream::OutgoingMessage;
use sqlx::{Database, Error};

use super::OutboxRow;

/// Creates the record of a message a handler publishes under a registered name, and returns its
/// id, which the message then carries in [`OUTBOX_ID_HEADER`](super::OUTBOX_ID_HEADER).
///
/// It has no default: the service's statement lays the name, the payload and the headers out in
/// its columns. A record type without it does not register.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `outbox::Publish<{DB}>`, so nothing records what is published under its names",
    label = "no `outbox::Publish` for this record",
    note = "implement `outbox::Publish<{DB}>` for `{Self}`: its statement inserts the record and returns the id"
)]
pub trait Publish<DB: Database>: OutboxRow {
    /// Inserts the record of `msg` and returns its id.
    ///
    /// # Errors
    ///
    /// The database's error; the message is not sent, and the publish fails with it.
    fn publish(
        conn: &mut DB::Connection,
        msg: &OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<Self::Id, Error>> + Send;
}

/// Takes the record `id` into work when its message reaches a subscription.
///
/// The default selects the record while it is unprocessed. `None` means the record is taken or
/// processed already: the handler does not run, and the delivery is acknowledged.
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `fetch` in `#[outbox(custom(..))]` and does not implement `outbox::Fetch<{DB}>`",
    label = "the service's own fetch is missing",
    note = "implement `outbox::Fetch<{DB}>` for `{Self}`, or drop `fetch` from `custom(..)`"
)]
pub trait Fetch<DB: Database>: OutboxRow {
    /// Takes the record `id` into work, or `None` when it is taken or processed already.
    ///
    /// # Errors
    ///
    /// The database's error; the handler does not run, and the delivery is retried.
    fn fetch(
        conn: &mut DB::Connection,
        id: &Self::Id,
    ) -> impl Future<Output = Result<Option<Self>, Error>> + Send;
}

macro_rules! outcome_event {
    (
        $(#[$doc:meta])* $trait:ident, $method:ident, $event:literal,
        message = $message:literal, note = $note:literal
    ) => {
        $(#[$doc])*
        #[diagnostic::on_unimplemented(
            message = $message,
            label = "the service's own event is missing",
            note = $note
        )]
        pub trait $trait<DB: Database>: OutboxRow {
            #[doc = concat!("Runs the `", $event, "` event for the record `id` after its handler finished.")]
            #[doc = ""]
            #[doc = "# Errors"]
            #[doc = ""]
            #[doc = "The database's error; the record stays unprocessed, and the next startup publishes it again."]
            fn $method(
                conn: &mut DB::Connection,
                id: &Self::Id,
            ) -> impl Future<Output = Result<(), Error>> + Send;
        }
    };
}

outcome_event!(
    /// Marks the record processed once its handler acknowledged the message.
    ///
    /// The default sets `processed_at` from the database's clock, or deletes the record when the
    /// struct has no `processed_at` field.
    Ack, ack, "ack",
    message = "`{Self}` lists `ack` in `#[outbox(custom(..))]` and does not implement `outbox::Ack<{DB}>`",
    note = "implement `outbox::Ack<{DB}>` for `{Self}`, or drop `ack` from `custom(..)`"
);

outcome_event!(
    /// Runs when the handler asked for the message again.
    ///
    /// The default leaves the record unprocessed and runs no statement, so the next startup
    /// publishes it again.
    Retry, retry, "retry",
    message = "`{Self}` lists `retry` in `#[outbox(custom(..))]` and does not implement `outbox::Retry<{DB}>`",
    note = "implement `outbox::Retry<{DB}>` for `{Self}`, or drop `retry` from `custom(..)`"
);

outcome_event!(
    /// Runs when the handler dropped the message.
    ///
    /// The default marks the record processed as [`Ack`] does: a dropped message is not sent
    /// again.
    Discard, discard, "discard",
    message = "`{Self}` lists `discard` in `#[outbox(custom(..))]` and does not implement `outbox::Discard<{DB}>`",
    note = "implement `outbox::Discard<{DB}>` for `{Self}`, or drop `discard` from `custom(..)`"
);

/// Selects the unprocessed records of one name, for the republish at startup.
///
/// The default selects the records of `name` whose `processed_at` is `NULL`, or every record of
/// `name` when the struct has no `processed_at` field.
#[diagnostic::on_unimplemented(
    message = "`{Self}` lists `recover` in `#[outbox(custom(..))]` and does not implement `outbox::Recover<{DB}>`",
    label = "the service's own recovery is missing",
    note = "implement `outbox::Recover<{DB}>` for `{Self}`, or drop `recover` from `custom(..)`"
)]
pub trait Recover<DB: Database>: OutboxRow {
    /// The unprocessed records published under `name`.
    ///
    /// # Errors
    ///
    /// The database's error; startup fails with it.
    fn recover(
        conn: &mut DB::Connection,
        name: &str,
    ) -> impl Future<Output = Result<Vec<Self>, Error>> + Send;
}

/// Every event of a record type, which registering it requires; implemented for any type that
/// has them all.
#[doc(hidden)]
pub trait Tracked<DB: Database>:
    Publish<DB> + Fetch<DB> + Ack<DB> + Retry<DB> + Discard<DB> + Recover<DB>
{
}

impl<DB, Record> Tracked<DB> for Record
where
    DB: Database,
    Record: Publish<DB> + Fetch<DB> + Ack<DB> + Retry<DB> + Discard<DB> + Recover<DB>,
{
}
