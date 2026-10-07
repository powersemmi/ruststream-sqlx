use std::pin::pin;

use futures::poll;

use super::{Extended, LeaseGone, Leases, Round, Standing};

/// A book of text ids, which hold storage of their own, and leases that are plain numbers.
type Book = Leases<String, u32>;

fn standing(book: &Book, slot: usize) -> Standing<u32> {
    book.slots().entries[slot].standing
}

/// Ends `round` with `outcome` for every delivery it marked.
fn resolve(book: &Book, round: &mut Round<String, u32>, outcome: Extended) {
    for target in round.marked_mut() {
        target.outcome = Some(outcome);
    }
    book.resolve(round);
}

#[tokio::test]
async fn an_extension_racing_a_settlement_never_loses_the_lease() {
    let book = Book::default();
    let slot = book.enter(&"job-1".to_owned(), 10);
    let mut round = Round::default();
    assert_eq!(book.mark(20, &mut round), 1);
    let mut settling = pin!(book.settling(slot));
    assert!(
        poll!(settling.as_mut()).is_pending(),
        "the settlement waits while its lease is extended"
    );
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(
        settling.await,
        Ok(20),
        "the settlement holds the lease the extension wrote, not the one it replaced"
    );
    assert_eq!(standing(&book, 0), Standing::Free);
}

#[tokio::test]
async fn a_settlement_that_waited_for_a_lost_lease_learns_it_is_gone() {
    let book = Book::default();
    let lost = book.enter(&"job-1".to_owned(), 10);
    let mut round = Round::default();
    book.mark(20, &mut round);
    let mut settling = pin!(book.settling(lost));
    assert!(poll!(settling.as_mut()).is_pending());
    resolve(&book, &mut round, Extended::Lost);
    assert_eq!(settling.await, Err(LeaseGone));
    // A failed extension may not have taken effect: the delivery keeps the lease it held.
    let failed = book.enter(&"job-2".to_owned(), 30);
    book.mark(40, &mut round);
    let mut settling = pin!(book.settling(failed));
    assert!(poll!(settling.as_mut()).is_pending());
    resolve(&book, &mut round, Extended::Failed);
    assert_eq!(settling.await, Ok(30));
}

#[tokio::test]
async fn a_lease_lost_between_settlements_is_gone_for_the_next_one() {
    let book = Book::default();
    let slot = book.enter(&"job-1".to_owned(), 10);
    let mut round = Round::default();
    book.mark(20, &mut round);
    resolve(&book, &mut round, Extended::Lost);
    assert_eq!(standing(&book, 0), Standing::Lost);
    assert!(!book.any_held(), "the keeper extends a lost lease no more");
    assert_eq!(book.settling(slot).await, Err(LeaseGone));
    assert_eq!(standing(&book, 0), Standing::Free);
}

#[tokio::test]
async fn a_settling_slot_is_skipped_by_the_next_round() {
    let book = Book::default();
    let settled = book.enter(&"job-1".to_owned(), 10);
    let kept = book.enter(&"job-2".to_owned(), 10);
    let mut round = Round::default();
    assert_eq!(book.mark(20, &mut round), 2);
    let mut settling = pin!(book.settling(settled));
    assert!(poll!(settling.as_mut()).is_pending());
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(standing(&book, 0), Standing::Settling(20));
    assert_eq!(standing(&book, 1), Standing::Held(20));
    // The settlement has not run again yet; the next round extends the other delivery alone.
    assert_eq!(book.mark(30, &mut round), 1);
    assert_eq!(round.marked_mut()[0].slot, 1);
    assert_eq!(standing(&book, 0), Standing::Settling(20));
    assert_eq!(settling.await, Ok(20));
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(book.settling(kept).await, Ok(30));
    // A round with nothing held marks nothing.
    assert!(!book.any_held());
    assert_eq!(book.mark(40, &mut round), 0);
}

#[tokio::test]
async fn a_settlement_dropped_while_it_waits_frees_its_slot() {
    let book = Book::default();
    let slot = book.enter(&"job-1".to_owned(), 10);
    let mut round = Round::default();
    book.mark(20, &mut round);
    {
        let mut settling = pin!(book.settling(slot));
        assert!(poll!(settling.as_mut()).is_pending());
    }
    assert_eq!(standing(&book, 0), Standing::Free);
    // The slot holds another delivery before the round ends; the round leaves it alone.
    let next = book.enter(&"job-2".to_owned(), 15);
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(standing(&book, 0), Standing::Held(15));
    assert_eq!(book.settling(next).await, Ok(15));
}

#[tokio::test]
async fn enter_after_leave_reuses_the_slot_and_its_id_allocation() {
    let book = Book::default();
    let first = book.enter(&"job-0001".to_owned(), 10);
    let storage = book.slots().entries[0].id.as_deref().map(str::as_ptr);
    assert_eq!(book.settling(first).await, Ok(10));
    let second = book.enter(&"job-0002".to_owned(), 20);
    {
        let slots = book.slots();
        assert_eq!(slots.entries.len(), 1, "the delivery took the free slot");
        assert_eq!(slots.entries[0].id.as_deref(), Some("job-0002"));
        assert_eq!(
            slots.entries[0].id.as_deref().map(str::as_ptr),
            storage,
            "the id was copied into the storage the last one left"
        );
    }
    assert_eq!(book.settling(second).await, Ok(20));
}

#[tokio::test]
async fn a_round_copies_each_id_into_the_storage_the_round_before_left() {
    let book = Book::default();
    let first = book.enter(&"job-0001".to_owned(), 10);
    let mut round = Round::default();
    book.mark(20, &mut round);
    let storage = round.targets[0].id.as_ptr();
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(book.settling(first).await, Ok(20));
    let second = book.enter(&"job-0002".to_owned(), 30);
    assert_eq!(book.mark(40, &mut round), 1);
    assert_eq!(round.targets[0].id, "job-0002");
    assert_eq!(round.targets[0].id.as_ptr(), storage);
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(book.settling(second).await, Ok(40));
}

#[tokio::test]
async fn an_acknowledgement_takes_the_lease_ahead_of_its_extension() {
    let book = Book::default();
    let slot = book.enter(&"job-1".to_owned(), 10);
    let mut round = Round::default();
    assert_eq!(book.mark(20, &mut round), 1);
    assert_eq!(
        book.take_ahead(slot),
        Ok(10),
        "the acknowledgement holds the lease the delivery held before the extension"
    );
    assert!(
        book.taken_ahead(0),
        "the round skips the extension it has not run"
    );
    resolve(&book, &mut round, Extended::Written);
    assert_eq!(
        standing(&book, 0),
        Standing::Free,
        "the round frees the slot"
    );
    let next = book.enter(&"job-2".to_owned(), 30);
    assert!(
        book.slots().free.is_empty(),
        "the next delivery took the freed slot"
    );
    assert_eq!(book.take_ahead(next), Ok(30));
}

#[tokio::test]
async fn a_lease_taken_ahead_with_no_extension_in_flight_leaves_at_once() {
    let book = Book::default();
    let held = book.enter(&"job-1".to_owned(), 10);
    assert_eq!(book.take_ahead(held), Ok(10));
    assert_eq!(standing(&book, 0), Standing::Free);
    let lost = book.enter(&"job-2".to_owned(), 10);
    let mut round = Round::default();
    book.mark(20, &mut round);
    resolve(&book, &mut round, Extended::Lost);
    assert_eq!(
        book.take_ahead(lost),
        Err(LeaseGone),
        "the keeper found the row gone"
    );
    assert_eq!(standing(&book, 0), Standing::Free);
}
