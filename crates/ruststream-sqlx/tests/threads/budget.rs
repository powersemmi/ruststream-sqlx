//! The connection limit: the service's pool and the connections of every subscription's
//! dedicated threads fit it, or the service does not start, and the error names the numbers.

use std::convert::Infallible;
use std::num::NonZeroU32;

use ruststream::nonzero;
use ruststream_sqlx::SqlxBrokerError;
use ruststream_sqlx::prelude::*;

use crate::live;
use crate::live::rows::lease::Plain;
use crate::probe::{GUARD, Probe};

/// `error` and every source under it, one after another.
fn chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        text.push_str(": ");
        text.push_str(&next.to_string());
        source = next.source();
    }
    text
}

live::stands! {
    #[subscriber(InboxQueue::<Plain>::new("plain"))]
    async fn computed(n: &i64, State(probe): State<Probe>) {
        probe.handled("plain", *n);
    }

    /// Starts the service with `computed` on 4 threads of 2 connections each, under `limit`.
    async fn started(db: &live::Database<Db>, limit: u32) -> Result<(), String> {
        let probe = Probe::default();
        let broker = SqlxBroker::new(db.pool.clone())
            .connection_limit(NonZeroU32::new(limit).expect("positive"));
        let app = RustStream::new(AppInfo::new("threads", "0.0.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(probe))
            .with_broker(broker, |b| {
                b.include(computed.on_threads(InboxThreads::new(nonzero!(4)).connections(nonzero!(2))));
            });
        let started = tokio::time::timeout(GUARD, app.start())
            .await
            .expect("the start ends in time");
        match started {
            Ok(running) => {
                running.shutdown().await.expect("the service stops");
                Ok(())
            }
            Err(error) => Err(chain(&error)),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_threads_connections_count_against_the_limit() {
        let Some(db) = database().await else { return };
        let pool = db.pool.options().get_max_connections();
        // The pool's connections and 4 x 2 on the threads fit exactly.
        assert_eq!(started(&db, pool + 8).await, Ok(()), "the service fits its limit");
        let refused = started(&db, pool + 7).await.expect_err("one connection too many");
        let expected = SqlxBrokerError::ConnectionLimit {
            subscription: "plain".to_owned(),
            limit: pool + 7,
            pool: u64::from(pool),
            threads: 8,
        }
        .to_string();
        assert!(refused.contains(&expected), "the refusal names the numbers: {refused}");
        db.finish().await;
    }
}
