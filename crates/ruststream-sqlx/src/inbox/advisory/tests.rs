use std::pin::pin;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::poll;
use ruststream_sqlx_dialect::{Column, Form, KeyPart, TableSpec};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Error, Sqlite, SqliteConnection, SqlitePool};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use super::{KeptBy, LockBook, ProcessLocks, Slots, Standing, Unlock};
use crate::inbox::engine::{IdAt, Now, Prepared, Settling};
use crate::inbox::queue::Queue;
use crate::inbox::session::{Closing, Session};

const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("id")];

/// A subscription to the jobs of an advisory table.
fn queue() -> &'static Queue {
    Box::leak(Box::new(Queue {
        name: "jobs",
        table: "jobs",
        row: "Job",
        spec: TableSpec::new("jobs", Column::new("id"), Form::Advisory(KEY))
            .payload(Column::new("payload")),
        id_at: IdAt::First,
        native_retry_after: false,
        kinds: None,
        prepared: Prepared::default(),
        begin_claim: None,
        counted_attempt: false,
        poll_interval: Duration::from_secs(1),
        lease: None,
        cap: None,
    }))
}

/// An unlock that always finds its lock.
fn unlocked<'a>(
    _: &'a mut SqliteConnection,
    _: Settling,
    _: &'a str,
) -> BoxFuture<'a, Result<bool, Error>> {
    Box::pin(async { Ok(true) })
}

/// An unlock that finds the session not holding the key.
fn not_held<'a>(
    _: &'a mut SqliteConnection,
    _: Settling,
    _: &'a str,
) -> BoxFuture<'a, Result<bool, Error>> {
    Box::pin(async { Ok(false) })
}

/// A book whose keys the process keeps, or the database, released with `unlock`.
fn book_with(process: bool, unlock: Unlock<Sqlite>) -> &'static LockBook<Sqlite> {
    Box::leak(Box::new(LockBook {
        slots: Mutex::new(Slots {
            entries: Vec::new(),
            free: Vec::new(),
            released: false,
        }),
        returned: Notify::new(),
        closing: Closing::leak(Handle::current()),
        queue: queue(),
        kept_by: if process {
            KeptBy::Process
        } else {
            KeptBy::Database
        },
        database: 0,
        unlock,
        now: Now::default(),
    }))
}

/// A book whose keys the process keeps, or the database.
fn book(process: bool) -> &'static LockBook<Sqlite> {
    book_with(process, unlocked)
}

async fn pool() -> Result<SqlitePool, Error> {
    SqlitePoolOptions::new()
        .max_connections(2)
        .connect("sqlite::memory:")
        .await
}

/// Where the key of the slot at `index` keeps its bytes.
fn key_storage(book: &LockBook<Sqlite>, index: usize) -> *const u8 {
    book.slots().entries[index].key.as_ptr()
}

fn is_free(book: &LockBook<Sqlite>, index: usize) -> bool {
    matches!(book.slots().entries[index].standing, Standing::Free)
}

fn is_released(book: &LockBook<Sqlite>, index: usize) -> bool {
    matches!(book.slots().entries[index].standing, Standing::Released)
}

/// A session of `pool` that holds a lock in the database.
async fn locked_session(
    pool: &SqlitePool,
    book: &'static LockBook<Sqlite>,
) -> Result<Session<Sqlite>, Error> {
    let mut session = Session::acquire(pool, book.closing).await?;
    session.set_locked(true);
    Ok(session)
}

#[tokio::test]
async fn a_free_slot_keeps_its_key_buffer_for_the_next_delivery() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    let hold = book.enter("jobs-0001", Session::acquire(&pool, book.closing).await?);
    let storage = key_storage(book, 0);
    let (session, key) = hold
        .lend()
        .expect("the session waits in its slot")
        .into_parts();
    assert_eq!(key, "jobs-0001");
    assert!(hold.lend().is_none(), "a lent session is lent once");
    session.release();
    hold.leave(key.into_owned());
    assert!(is_free(book, 0));
    let next = book.enter("jobs-0002", Session::acquire(&pool, book.closing).await?);
    {
        let slots = book.slots();
        assert_eq!(slots.entries.len(), 1, "the delivery took the free slot");
        assert_eq!(slots.entries[0].key, "jobs-0002");
    }
    assert_eq!(
        key_storage(book, 0),
        storage,
        "the key was copied into the buffer the last one left"
    );
    let (session, key) = next
        .lend()
        .expect("the session waits in its slot")
        .into_parts();
    session.release();
    next.leave(key.into_owned());
    Ok(())
}

#[tokio::test]
async fn a_lent_session_comes_back_to_its_slot() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    let hold = book.enter("jobs-7", Session::acquire(&pool, book.closing).await?);
    let lent = hold.lend().expect("the session waits in its slot");
    hold.give_back(lent);
    let (session, key) = hold
        .lend()
        .expect("the session is back in its slot")
        .into_parts();
    assert_eq!(key, "jobs-7");
    session.release();
    hold.leave(key.into_owned());
    assert!(is_free(book, 0));
    Ok(())
}

#[tokio::test]
async fn a_hold_dropped_unsettled_frees_its_slot_and_its_process_key() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(true);
    let mut session = Session::acquire(&pool, book.closing).await?;
    assert!(session.take_in_process(0, "unit-dropped-hold"));
    let hold = book.enter("unit-dropped-hold", session);
    assert!(
        ProcessLocks::try_take(0, "unit-dropped-hold").is_none(),
        "the delivery holds its key"
    );
    drop(hold);
    assert!(is_free(book, 0));
    assert_eq!(book.slots().free, [0]);
    assert!(
        ProcessLocks::try_take(0, "unit-dropped-hold").is_some(),
        "the unsettled drop freed the key"
    );
    Ok(())
}

#[tokio::test]
async fn a_settlement_dropped_midway_frees_its_slot() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    let hold = book.enter("jobs-9", Session::acquire(&pool, book.closing).await?);
    let lent = hold.lend().expect("the session waits in its slot");
    // The settlement's future drops with the hold it owns, and the session it lent ends on
    // its own.
    drop(hold);
    assert!(is_free(book, 0));
    assert_eq!(book.slots().free, [0]);
    drop(lent);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_lent_and_dropped_closes_after_the_unlock_of_its_key() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    book.closing.begin_shutdown();
    let hold = book.enter("jobs-8", locked_session(&pool, book).await?);
    let lent = hold.lend().expect("the session waits in its slot");
    drop(lent);
    assert_eq!(pool.size(), 0, "the connection left its pool to close");
    book.closing.settled().await;
    assert_eq!(
        book.closing.forced(),
        1,
        "the session closed holding its lock"
    );
    drop(hold);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_release_unlocks_each_held_session_and_returns_it_to_the_pool() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    book.closing.begin_shutdown();
    let hold = book.enter("jobs-1", locked_session(&pool, book).await?);
    assert_eq!(pool.size(), 1);
    assert_eq!(book.release_all().await, 1);
    assert!(is_released(book, 0));
    assert!(hold.lend().is_none(), "the delivery settles no more");
    assert_eq!(pool.size(), 1, "the connection went back to the pool");
    drop(hold);
    assert!(is_free(book, 0), "the released delivery leaves its slot");
    book.closing.settled().await;
    assert_eq!(book.closing.forced(), 0, "no session closed");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_the_database_does_not_confirm_closes_the_session() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book_with(false, not_held);
    book.closing.begin_shutdown();
    let hold = book.enter("jobs-1", locked_session(&pool, book).await?);
    assert_eq!(book.release_all().await, 0);
    assert_eq!(pool.size(), 0, "the connection left its pool to close");
    book.closing.settled().await;
    assert_eq!(book.closing.forced(), 1, "the close is counted");
    drop(hold);
    Ok(())
}

#[tokio::test]
async fn the_release_frees_each_key_the_process_keeps() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(true);
    let mut session = Session::acquire(&pool, book.closing).await?;
    assert!(session.take_in_process(0, "unit-released-key"));
    let hold = book.enter("unit-released-key", session);
    assert_eq!(book.release_all().await, 1);
    assert!(
        ProcessLocks::try_take(0, "unit-released-key").is_some(),
        "the release freed the key"
    );
    drop(hold);
    Ok(())
}

#[tokio::test]
async fn the_release_waits_for_a_lent_session_and_releases_it_once_it_is_back() -> Result<(), Error>
{
    let pool = pool().await?;
    let book = book(false);
    let hold = book.enter("jobs-3", locked_session(&pool, book).await?);
    let lent = hold.lend().expect("the session waits in its slot");
    let mut releasing = pin!(book.release_all());
    assert!(
        poll!(releasing.as_mut()).is_pending(),
        "the release waits while a session is lent"
    );
    hold.give_back(lent);
    assert_eq!(releasing.await, 1, "the session given back is released");
    assert!(hold.lend().is_none());
    drop(hold);
    Ok(())
}

#[tokio::test]
async fn the_release_waits_for_a_settlement_until_it_leaves() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    let hold = book.enter("jobs-4", Session::acquire(&pool, book.closing).await?);
    let (session, key) = hold
        .lend()
        .expect("the session waits in its slot")
        .into_parts();
    let mut releasing = pin!(book.release_all());
    assert!(poll!(releasing.as_mut()).is_pending());
    session.release();
    hold.leave(key.into_owned());
    assert_eq!(releasing.await, 0, "the settlement released its own lock");
    assert!(is_free(book, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_that_enters_a_released_book_ends_its_session() -> Result<(), Error> {
    let pool = pool().await?;
    let book = book(false);
    book.closing.begin_shutdown();
    assert_eq!(book.release_all().await, 0);
    let hold = book.enter("jobs-5", locked_session(&pool, book).await?);
    assert!(is_released(book, 0));
    assert!(hold.lend().is_none(), "the late delivery settles no more");
    book.closing.settled().await;
    assert_eq!(
        book.closing.forced(),
        1,
        "its session closed after the unlock of its key"
    );
    drop(hold);
    assert!(is_free(book, 0));
    Ok(())
}
