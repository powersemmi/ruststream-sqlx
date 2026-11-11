//! The batches of a subscription, read one at a time by a test whose subject is the claim itself.

use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::pin::Pin;

use ruststream::{BatchSlot, BatchStream};

/// Reads the next batch off a subscription's batch stream, into a slot of its own: what the
/// runtime does with the one slot it lends a worker, one batch at a time.
pub(crate) trait NextBatch<Slot> {
    /// What a failed claim reports.
    type Error;

    /// The next batch, `None` once the subscription ended.
    fn next(&mut self) -> impl Future<Output = Option<Result<Slot, Self::Error>>>;
}

impl<Batches, Slot> NextBatch<Slot> for Pin<&mut Batches>
where
    Batches: BatchStream<Slot> + ?Sized,
    Slot: BatchSlot,
{
    type Error = Batches::Error;

    async fn next(&mut self) -> Option<Result<Slot, Self::Error>> {
        let mut slot = Slot::with_capacity(NonZeroUsize::MIN);
        let filled = poll_fn(|cx| self.as_mut().poll_fill(cx, &mut slot)).await?;
        Some(filled.map(|()| slot))
    }
}
