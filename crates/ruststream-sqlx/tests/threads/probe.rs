//! What the handlers of a suite report to the test: how many deliveries they handled, in which
//! order, and that each ran on a dedicated thread.

// The outbox suite shares this module and uses a part of it, so what it leaves alone is not dead
// code.
#![allow(dead_code)]

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ruststream::runtime::FromRef;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::Notify;

/// The longest a suite waits for its handlers: a hang becomes a failure.
pub(crate) const GUARD: Duration = Duration::from_secs(30);

/// The app's state the suites' handlers report through.
#[derive(Clone, Debug, Default)]
pub(crate) struct Probe(Arc<Inner>);

impl FromRef<Self> for Probe {
    fn from_ref(state: &Self) -> Self {
        state.clone()
    }
}

#[derive(Debug, Default)]
struct Inner {
    handled: AtomicUsize,
    order: Mutex<Vec<(String, i64)>>,
    kept: Mutex<Vec<Box<dyn Any + Send>>>,
    wake: Notify,
}

impl Probe {
    /// A handler took the delivery `id` of `key`. It runs on one of the subscription's threads,
    /// each a current-thread runtime, never on the test's multi-threaded one.
    pub(crate) fn handled(&self, key: &str, id: i64) {
        assert_eq!(
            Handle::current().runtime_flavor(),
            RuntimeFlavor::CurrentThread,
            "the handler runs on a dedicated thread"
        );
        self.0
            .order
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((key.to_owned(), id));
        self.0.handled.fetch_add(1, Ordering::AcqRel);
        self.0.wake.notify_waiters();
    }

    /// The deliveries the handlers took, as `(key, id)` in the order they took them.
    pub(crate) fn order(&self) -> Vec<(String, i64)> {
        self.0
            .order
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Keeps `value`, a connection a handler holds on to, until [`Probe::release`].
    pub(crate) fn keep(&self, value: impl Any + Send) {
        self.0
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Box::new(value));
    }

    /// How many values the handlers keep.
    pub(crate) fn kept(&self) -> usize {
        self.0
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Lets go of everything the handlers kept.
    pub(crate) fn release(&self) {
        self.0
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Waits until the handlers took `count` deliveries.
    ///
    /// # Panics
    ///
    /// Panics when they did not within [`GUARD`].
    pub(crate) async fn reached(&self, count: usize) {
        let wait = async {
            loop {
                let woken = self.0.wake.notified();
                tokio::pin!(woken);
                woken.as_mut().enable();
                if self.0.handled.load(Ordering::Acquire) >= count {
                    return;
                }
                woken.await;
            }
        };
        tokio::time::timeout(GUARD, wait).await.unwrap_or_else(|_| {
            panic!(
                "the handlers took {} deliveries of {count}",
                self.0.handled.load(Ordering::Acquire)
            )
        });
    }
}

/// Waits until `check` holds, looking again after each yield to the runtime.
///
/// # Panics
///
/// Panics when it did not hold within [`GUARD`], naming `what`.
pub(crate) async fn until(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let wait = async {
        while !check().await {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(GUARD, wait)
        .await
        .unwrap_or_else(|_| panic!("{what} within {GUARD:?}"));
}
