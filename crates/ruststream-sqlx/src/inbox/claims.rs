//! How many claims a subscription of single deliveries keeps in flight, and the slots they run in.
//!
//! The runtime polls a subscription's stream once for each free worker and does not say how many
//! workers it has. The subscription counts its deliveries in work instead: each poll shows one more
//! worker free than that count, so the largest such count it has seen is its estimate of the
//! workers, and the estimate less the deliveries in work is its free workers. It keeps a claim in
//! flight for each, within the connections it may hold.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use tokio::sync::Notify;

/// What a subscription of single deliveries knows of its workers and its connections, from poll to
/// poll.
#[derive(Debug)]
pub(crate) struct Flow {
    /// The workers the subscription has seen at once: the most deliveries in work at a poll, plus
    /// the worker that polled.
    workers: usize,
    /// After a claim that took nothing, one claim at a time until one takes a row: an empty table
    /// sees one claim per wait, not one per worker.
    probing: bool,
    /// Whether the subscription keeps one claim in flight: on a table whose groups keep their
    /// order, where a second claim of the group takes nothing while the first holds it, and on a
    /// database that takes one writer at a time.
    one_claim: bool,
    /// The connections the subscription holds at most: the pool's size less one, which it leaves
    /// to the handlers' own queries, their publishes and the settlements that take one.
    budget: usize,
    /// Whether a delivery in work holds a connection: in every form but the lease form, and in
    /// transactional mode in that one too.
    holds: bool,
}

impl Flow {
    /// The flow of a subscription on a pool of `max_connections`, whose deliveries hold a
    /// connection each where `holds`, with one claim in flight where `one_claim`.
    pub(crate) fn new(max_connections: u32, holds: bool, one_claim: bool) -> Self {
        let size = usize::try_from(max_connections).unwrap_or(usize::MAX);
        let budget = size.saturating_sub(1).max(1);
        Self {
            workers: 1,
            probing: true,
            one_claim,
            budget,
            holds,
        }
    }

    /// A poll of the stream, a worker free beside `in_work` deliveries.
    pub(crate) fn polled(&mut self, in_work: usize) {
        self.workers = self.workers.max(in_work.saturating_add(1));
    }

    /// Whether one more claim starts beside `running` claims and `in_work` deliveries. A claim
    /// beside another also needs `spare`, the pool's idle connections and room for new ones, to
    /// leave one of them free; the first one waits for the pool.
    pub(crate) fn may_start(
        &self,
        running: usize,
        in_work: usize,
        spare: impl FnOnce() -> usize,
    ) -> bool {
        let wanted = if self.probing {
            1
        } else {
            self.workers
                .saturating_sub(in_work)
                .clamp(1, if self.one_claim { 1 } else { self.budget })
        };
        running < wanted
            && self.held(running, in_work) < self.budget
            && (running == 0 || spare() >= 2)
    }

    /// Whether the subscription, with `running` claims and `in_work` deliveries, holds all it may
    /// and waits for a delivery to settle before it claims again.
    pub(crate) fn waits_for_a_settlement(&self, running: usize, in_work: usize) -> bool {
        running == 0 && self.held(running, in_work) >= self.budget
    }

    /// A claim took a row.
    pub(crate) fn took(&mut self) {
        self.probing = false;
    }

    /// A claim took nothing, or failed.
    pub(crate) fn found_nothing(&mut self) {
        self.probing = true;
    }

    fn held(&self, running: usize, in_work: usize) -> usize {
        running + if self.holds { in_work } else { 0 }
    }
}

/// The waiting bit of [`Settled::count`]: the settlement count lives in the bits above it.
const WAITING: usize = 1;

/// How many deliveries of a subscription settled, and the wake of its stream when it waits for one.
#[derive(Debug, Default)]
pub(crate) struct Settled {
    /// The settlements, shifted past [`WAITING`], which a stream sets while it waits: a settlement
    /// pays one atomic add, and a notify only when a stream waits.
    count: AtomicUsize,
    wake: Notify,
}

impl Settled {
    /// The deliveries settled or dropped so far.
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire) >> 1
    }

    /// Waits until the count is past `seen`.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropped midway it clears its mark, and a wake it leaves stored ends the next
    /// wait early, which then looks at the count again.
    pub(crate) async fn change(&self, seen: usize) {
        let _waiting = Waiting::mark(self);
        while self.count() == seen {
            self.wake.notified().await;
        }
    }

    fn settle(&self) {
        if self.count.fetch_add(2, Ordering::AcqRel) & WAITING != 0 {
            self.wake.notify_one();
        }
    }
}

/// A stream's mark on [`Settled`] while it waits for a settlement.
struct Waiting<'a>(&'a Settled);

impl<'a> Waiting<'a> {
    fn mark(settled: &'a Settled) -> Self {
        settled.count.fetch_or(WAITING, Ordering::AcqRel);
        Self(settled)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.count.fetch_and(!WAITING, Ordering::AcqRel);
    }
}

/// A delivery of a subscription in work: dropped when the delivery settles or drops, it counts
/// one settlement.
#[derive(Debug)]
pub(crate) struct InWork(&'static Settled);

impl InWork {
    pub(crate) const fn new(settled: &'static Settled) -> Self {
        Self(settled)
    }
}

impl Drop for InWork {
    fn drop(&mut self) {
        self.0.settle();
    }
}

/// A slot a claim runs in: boxed once, its storage reused by every claim after.
type Slot<Fut> = Pin<Box<Option<Fut>>>;

/// The claims of a subscription in flight, each in a slot of its own, polled together on the
/// subscription's task.
///
/// Each claim owns its `Buffers` while it runs and hands them back with its outcome; idle ones
/// wait here for the next claim. Slots and buffers are made at most once per claim in flight at
/// once, so a claim allocates nothing once the subscription has run that many.
pub(crate) struct Claims<Args, Buffers, Make, Fut> {
    make: Make,
    _args: PhantomData<fn(Args)>,
    slots: Vec<Slot<Fut>>,
    idle: Vec<Buffers>,
    running: usize,
}

impl<Args, Buffers, Make, Fut> Claims<Args, Buffers, Make, Fut> {
    /// No claims yet; each one runs the future `make` builds from its arguments and buffers.
    pub(crate) const fn new(make: Make) -> Self {
        Self {
            make,
            _args: PhantomData,
            slots: Vec::new(),
            idle: Vec::new(),
            running: 0,
        }
    }

    /// The claims in flight.
    pub(crate) const fn running(&self) -> usize {
        self.running
    }

    /// Buffers a claim handed back, for the next one.
    pub(crate) fn put_back(&mut self, buffers: Buffers) {
        self.idle.push(buffers);
    }

    #[cfg(test)]
    fn slots(&self) -> usize {
        self.slots.len()
    }
}

impl<Args, Buffers, Make, Fut> Claims<Args, Buffers, Make, Fut>
where
    Buffers: Default,
    Make: Fn(Args, Buffers) -> Fut,
    Fut: Future,
{
    /// Starts a claim with `args` in a free slot, or a new one, and returns its slot.
    pub(crate) fn start(&mut self, args: Args) -> usize {
        let claim = (self.make)(args, self.idle.pop().unwrap_or_default());
        self.running += 1;
        if let Some(index) = self.slots.iter().position(|slot| slot.is_none()) {
            self.slots[index].as_mut().set(Some(claim));
            return index;
        }
        self.slots.push(Box::pin(Some(claim)));
        self.slots.len() - 1
    }

    /// Polls the claim in slot `index`, which runs.
    pub(crate) fn poll_slot(&mut self, index: usize, cx: &mut Context<'_>) -> Poll<Fut::Output> {
        let slot = &mut self.slots[index];
        let Some(claim) = slot.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        let done = std::task::ready!(claim.poll(cx));
        slot.as_mut().set(None);
        self.running -= 1;
        Poll::Ready(done)
    }

    /// Polls every claim in flight, and returns the first that finished; `None` when none runs.
    pub(crate) fn poll_any(&mut self, cx: &mut Context<'_>) -> Poll<Option<Fut::Output>> {
        if self.running == 0 {
            return Poll::Ready(None);
        }
        for index in 0..self.slots.len() {
            if let Poll::Ready(done) = self.poll_slot(index, cx) {
                return Poll::Ready(Some(done));
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, ready};
    use std::pin::pin;
    use std::time::Duration;

    use super::{Claims, Flow, InWork, Settled};

    /// A pool of eight with every connection to spare.
    const ROOMY: fn() -> usize = || 8;

    fn row_lock() -> Flow {
        Flow::new(8, true, false)
    }

    /// The claims a flow starts at once, beside `in_work` deliveries, with `spare` connections in
    /// the pool before the first claim takes one.
    fn burst(flow: &Flow, in_work: usize, spare: usize) -> usize {
        let mut running = 0;
        while flow.may_start(running, in_work, || spare.saturating_sub(running)) {
            running += 1;
        }
        running
    }

    #[test]
    fn a_plain_mount_keeps_one_claim() {
        let mut flow = row_lock();
        for _ in 0..3 {
            flow.polled(0);
            flow.took();
            assert_eq!(burst(&flow, 0, 8), 1);
        }
    }

    #[test]
    fn a_free_worker_gets_a_claim_of_its_own() {
        let mut flow = row_lock();
        // Four deliveries went into work one poll after another: four workers at least.
        for in_work in 0..4 {
            flow.polled(in_work);
            flow.took();
        }
        // All four settled: the next poll claims for every one of them.
        flow.polled(0);
        assert_eq!(burst(&flow, 0, 8), 4);
        // Two still in work: two free.
        assert_eq!(burst(&flow, 2, 8), 2);
    }

    #[test]
    fn a_claim_that_took_nothing_leaves_one_claim_in_flight() {
        let mut flow = row_lock();
        for in_work in 0..4 {
            flow.polled(in_work);
            flow.took();
        }
        flow.found_nothing();
        flow.polled(0);
        assert_eq!(burst(&flow, 0, 8), 1);
        flow.took();
        assert_eq!(burst(&flow, 0, 8), 4);
    }

    #[test]
    fn a_table_in_order_or_a_database_of_one_writer_keeps_one_claim() {
        let mut flow = Flow::new(8, true, true);
        for in_work in 0..4 {
            flow.polled(in_work);
            flow.took();
        }
        flow.polled(0);
        assert_eq!(burst(&flow, 0, 8), 1);
    }

    #[test]
    fn a_subscription_holds_one_connection_less_than_the_pool() {
        let mut flow = row_lock();
        for in_work in 0..8 {
            flow.polled(in_work);
            flow.took();
        }
        // Seven deliveries in work hold seven of eight: the last one stays.
        assert!(!flow.may_start(0, 7, ROOMY));
        assert!(flow.waits_for_a_settlement(0, 7));
        assert!(flow.may_start(0, 6, ROOMY));
        assert!(!flow.waits_for_a_settlement(0, 6));
    }

    #[test]
    fn a_lease_counts_its_claims_alone() {
        let mut flow = Flow::new(8, false, false);
        for in_work in 0..12 {
            flow.polled(in_work);
            flow.took();
        }
        // Deliveries in work hold no connection: the claims stop one short of the pool.
        assert!(!flow.waits_for_a_settlement(0, 11));
        flow.polled(0);
        assert_eq!(burst(&flow, 0, 8), 7);
    }

    #[test]
    fn a_claim_beside_another_needs_a_connection_to_spare() {
        let mut flow = row_lock();
        for in_work in 0..4 {
            flow.polled(in_work);
            flow.took();
        }
        flow.polled(0);
        // The first claim waits for the pool; each next one leaves a connection free beside it.
        assert_eq!(burst(&flow, 0, 0), 1);
        assert_eq!(burst(&flow, 0, 2), 1);
        assert_eq!(burst(&flow, 0, 3), 2);
        assert_eq!(burst(&flow, 0, 4), 3);
    }

    #[test]
    fn a_pool_of_one_serves_one_delivery() {
        let mut flow = Flow::new(1, true, false);
        flow.polled(0);
        flow.took();
        assert!(flow.may_start(0, 0, || 1));
        assert!(flow.waits_for_a_settlement(0, 1));
    }

    #[tokio::test]
    async fn a_settlement_counts_once_and_wakes_a_waiter() {
        let settled: &'static Settled = Box::leak(Box::default());
        assert_eq!(settled.count(), 0);
        let mut waiting = pin!(settled.change(0));
        assert!(
            futures::poll!(waiting.as_mut()).is_pending(),
            "nothing settled yet"
        );
        drop(InWork::new(settled));
        assert_eq!(settled.count(), 1);
        // A wake the settlement lost would leave the wait hanging: the bound fails it instead.
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the settlement wakes the waiter");
        // A count already past the one asked for ends the wait at once.
        settled.change(0).await;
    }

    #[test]
    fn a_slot_is_reused_by_the_next_claim() {
        let mut claims = Claims::<(), Vec<u8>, _, _>::new(|(): (), mut buffer: Vec<u8>| {
            buffer.push(1);
            ready(buffer)
        });
        futures::executor::block_on(async {
            for round in 1..=3 {
                let started = claims.start(());
                let buffer = poll_fn(|cx| claims.poll_slot(started, cx)).await;
                assert_eq!(buffer.len(), round, "the buffer comes back each round");
                claims.put_back(buffer);
                assert_eq!(claims.running(), 0);
            }
            assert_eq!(claims.slots(), 1);
            let (first, second) = (claims.start(()), claims.start(()));
            assert_ne!(first, second);
            assert_eq!(claims.running(), 2);
            let done = poll_fn(|cx| claims.poll_any(cx)).await;
            claims.put_back(done.expect("a claim ran"));
            let done = poll_fn(|cx| claims.poll_any(cx)).await;
            claims.put_back(done.expect("a claim ran"));
            assert_eq!(claims.slots(), 2);
            assert!(
                poll_fn(|cx| claims.poll_any(cx)).await.is_none(),
                "nothing runs"
            );
        });
    }
}
