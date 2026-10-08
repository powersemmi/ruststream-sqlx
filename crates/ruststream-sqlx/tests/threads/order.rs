//! Order on dedicated threads: `threads(n, by_key)` keeps the rows of a partition key in order,
//! and a FIFO group stays in order on any number of threads.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::hint::spin_loop;
use std::time::{Duration, Instant};

use chrono::Utc;
use ruststream::runtime::RunningApp;
use ruststream_sqlx::prelude::*;
use serde::{Deserialize, Serialize};

use crate::live;
use crate::probe::{GUARD, Probe};

const POLL: Duration = Duration::from_millis(20);

/// The keys the rows spread over: fewer than the threads, so a round robin would part a key.
const KEYS: [&str; 2] = ["a", "b"];

/// The rows each suite writes.
const ROWS: usize = 24;

/// How long the first row of each key computes: long enough that a later row of the key on
/// another thread would finish first.
const FIRST: Duration = Duration::from_millis(20);

/// A row's payload: its key and its place among the rows.
#[derive(Debug, Serialize, Deserialize)]
struct Step {
    key: String,
    n: i64,
}

/// Holds the thread for `span`, as a handler that computes does.
fn compute(span: Duration) {
    let start = Instant::now();
    while start.elapsed() < span {
        spin_loop();
    }
}

fn payload(step: &Step) -> Vec<u8> {
    serde_json::to_vec(step).expect("json")
}

/// Stops the service, within the guard.
async fn stopped(running: RunningApp) {
    tokio::time::timeout(GUARD, running.shutdown())
        .await
        .expect("the service stops in time")
        .expect("the service stops");
}

/// The rows of each key in the order the handlers took them.
fn by_key(order: Vec<(String, i64)>) -> BTreeMap<String, Vec<i64>> {
    let mut keys: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for (key, n) in order {
        keys.entry(key).or_default().push(n);
    }
    keys
}

/// The `n`th row's place, as its payload carries it.
fn place(n: usize) -> i64 {
    i64::try_from(n).expect("a count")
}

/// The rows of each key in the order they were written.
fn written() -> BTreeMap<String, Vec<i64>> {
    by_key(
        (0..ROWS)
            .map(|n| (KEYS[n % KEYS.len()].to_owned(), place(n)))
            .collect(),
    )
}

/// The rows of a partition key, on every form.
mod keyed {
    use super::*;

    live::matrix! {
        #[subscriber(InboxQueue::<SendEmail>::new("mail"), threads(3, by_key))]
        async fn keyed(step: &Step, State(probe): State<Probe>) {
            if step.n < i64::try_from(KEYS.len()).expect("fits") {
                compute(FIRST);
            }
            probe.handled(&step.key, step.n);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_partition_key_keeps_its_order_across_threads() {
            let Some(db) = database().await else { return };
            let rows: Vec<SendEmail> = (0..ROWS)
                .map(|n| {
                    let key = KEYS[n % KEYS.len()];
                    let step = Step { key: key.to_owned(), n: place(n) };
                    let mut row = SendEmail::queued("mail", payload(&step));
                    row.customer = Some(key.to_owned());
                    row
                })
                .collect();
            db.mail(&rows).await;
            let probe = Probe::default();
            let state = probe.clone();
            let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(state))
                .with_broker(SqlxBroker::new(db.pool.clone()).poll_interval(POLL), |b| {
                    b.include(keyed);
                });
            let running = tokio::time::timeout(GUARD, app.start())
                .await
                .expect("the service starts in time")
                .expect("the service starts");
            probe.reached(ROWS).await;
            stopped(running).await;
            assert_eq!(by_key(probe.order()), written(), "each key in the order it was written");
            db.finish().await;
        }
    }
}

// The advisory lock form keeps the rows of a shared key in work one at a time, not in claim
// order, so the FIFO suite runs on the forms with FIFO groups.
mod fifo {
    use super::*;

    live::fifo_matrix! {
        #[subscriber(InboxQueue::<Entry>::new("acme"), threads(3))]
        async fn booked(step: &Step, State(probe): State<Probe>) {
            if step.n == 0 {
                compute(FIRST);
            }
            probe.handled(&step.key, step.n);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_fifo_group_keeps_its_order_across_threads() {
            let Some(db) = database().await else { return };
            let due = Utc::now() - chrono::Duration::minutes(1);
            let rows: Vec<Entry> = (0..ROWS)
                .map(|n| {
                    let step = Step { key: "acme".to_owned(), n: i64::try_from(n).expect("a count") };
                    let at = due + chrono::Duration::milliseconds(step.n);
                    Entry::new("acme", 0, at, &payload(&step))
                })
                .collect();
            db.mail(&rows).await;
            let probe = Probe::default();
            let state = probe.clone();
            let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
                .on_startup(async move |()| Ok::<_, Infallible>(state))
                .with_broker(SqlxBroker::new(db.pool.clone()).poll_interval(POLL), |b| {
                    b.include(booked);
                });
            let running = tokio::time::timeout(GUARD, app.start())
                .await
                .expect("the service starts in time")
                .expect("the service starts");
            probe.reached(ROWS).await;
            stopped(running).await;
            let expected: Vec<i64> = (0..i64::try_from(ROWS).expect("a count")).collect();
            assert_eq!(
                by_key(probe.order()).remove("acme"),
                Some(expected),
                "the group in the order it was written"
            );
            db.finish().await;
        }
    }
}
