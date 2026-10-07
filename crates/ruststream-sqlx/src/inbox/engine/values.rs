//! What a default statement binds: the values one statement may bind, by meaning.

use std::time::Duration;

use super::{Claiming, Event, Events, Leasing, Now, Settling};
use crate::inbox::database::QueueDatabase;
use crate::inbox::queue::Queue;

/// The values one statement may bind, by meaning.
#[derive(Debug)]
pub struct Values<'a, DB: QueueDatabase, Row: Events<DB>> {
    /// The event the statement serves.
    pub event: Event,
    /// The subscription: its name (its group, or the table's address) and what it knows of the
    /// table.
    pub queue: &'static Queue,
    /// The most rows a claim takes.
    pub limit: i64,
    /// The row a settlement settles.
    pub id: Option<&'a Row::Id>,
    /// The ids a fetch reads.
    pub ids: &'a [Row::Id],
    /// A delayed retry's delay.
    pub delay: Duration,
    /// A dead letter's destination.
    pub destination: &'a str,
    /// Where "now" comes from where the statement takes no lease.
    pub now: Now,
    /// The lease a claim, a stamp or an extension writes.
    pub lease: Option<Row::Token>,
    /// The lease a claim and its stamps take: every time they bind starts from its instant.
    pub leasing: Option<&'a Leasing<Row::Token>>,
    /// The lease a settlement or an extension matches: the delivery's ownership token.
    pub held: Option<Row::Token>,
    /// The lock key the advisory lock form's lock or unlock names.
    pub key: Option<&'a str>,
}

impl<'a, DB: QueueDatabase, Row: Events<DB>> Values<'a, DB, Row> {
    pub(crate) const fn claiming(
        cx: Claiming,
        event: Event,
        leasing: Option<&'a Leasing<Row::Token>>,
    ) -> Self {
        Self {
            event,
            queue: cx.queue,
            limit: cx.limit,
            id: None,
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
            lease: match leasing {
                Some(leasing) => Some(leasing.expiry),
                None => None,
            },
            leasing,
            held: None,
            key: None,
        }
    }

    pub(super) const fn settling(
        cx: Settling,
        event: Event,
        id: &'a Row::Id,
        held: Option<Row::Token>,
    ) -> Self {
        Self {
            event,
            queue: cx.queue,
            limit: 0,
            id: Some(id),
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
            lease: None,
            leasing: None,
            held,
            key: None,
        }
    }

    /// The values of an unlock of `key`: a settlement that names no row.
    pub(crate) const fn unlocking(cx: Settling, key: &'a str) -> Self {
        Self {
            event: Event::Unlock,
            queue: cx.queue,
            limit: 0,
            id: None,
            ids: &[],
            delay: Duration::ZERO,
            destination: "",
            now: cx.now,
            lease: None,
            leasing: None,
            held: None,
            key: Some(key),
        }
    }
}
