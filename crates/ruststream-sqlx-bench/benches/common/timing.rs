//! What the wall-clock benchmarks share: the rounds' statistics, the verdict on a difference,
//! the settings a run reads from the environment, and the runtime every loop runs on.

use std::env;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use serde::Serialize;
use sqlx::PgPool;
use tokio::runtime::{Builder, Runtime};

/// Worker threads every loop is driven on.
pub const WORKERS: usize = 4;

/// A multi-threaded runtime: the one a service runs on, where atomics and cache effects that an
/// instruction count under-reports show.
pub fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// # Panics
///
/// Panics when the variable is set to anything but a positive number.
pub fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

/// Best, median and worst of the rounds, in messages per second.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Stats {
    pub best: f64,
    pub median: f64,
    pub worst: f64,
}

impl Stats {
    pub fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1].round(),
            median: median.round(),
            worst: sorted[0].round(),
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

/// Messages per second over a run's window.
#[allow(clippy::cast_precision_loss)]
pub fn rate(messages: usize, window: Duration) -> f64 {
    messages as f64 / window.as_secs_f64()
}

/// How much slower the service is than the raw loop, and whether the difference outgrew the
/// noise.
///
/// The honesty rule of the procedure, applied to every percentage the document carries: a
/// difference smaller than the run-to-run spread of either loop is a verdict, never a figure.
pub fn against(raw: Stats, service: Stats) -> (f64, &'static str) {
    let difference = raw.best - service.best;
    let verdict = if difference.abs() < raw.spread().max(service.spread()) {
        "indistinguishable"
    } else {
        "measured"
    };
    ((difference / raw.best * 1000.0).round() / 10.0, verdict)
}

/// Round trips the probe takes before it reports a median.
const ROUND_TRIPS: usize = 2_000;

/// How long one statement round trip to the database takes on this machine: the floor under
/// every claim and every settlement, which the page reports with the numbers and which decides
/// whether a scenario was paced by the database.
///
/// The median over many samples on one connection, so a scheduler hiccup does not move it.
pub async fn round_trip(pool: &PgPool) -> Duration {
    let mut conn = pool.acquire().await.expect("the pool lends a connection");
    let mut samples = Vec::with_capacity(ROUND_TRIPS);
    for _ in 0..ROUND_TRIPS {
        let start = Instant::now();
        sqlx::query("SELECT 1")
            .execute(&mut *conn)
            .await
            .expect("the database answers");
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

/// The round trip as the environment of the document states it.
pub fn describe_round_trip(round_trip: Duration) -> String {
    format!(
        "{:.1} us (median of {ROUND_TRIPS} `SELECT 1` round trips on one Postgres connection)",
        round_trip.as_secs_f64() * 1e6
    )
}

/// Whether the raw loop spent at least half of each message waiting on the database: the round
/// trips a delivery costs it, at the latency [`round_trip`] measured, against the time a message
/// took at its best round. Such a row says more about the database than about this crate, and
/// everything above the socket does its work inside that wait.
pub fn broker_bound(round_trips: f64, round_trip: Duration, raw: Stats) -> bool {
    round_trips * round_trip.as_secs_f64() >= 0.5 / raw.best
}
