//! The subscription middleware: a delivery under a registered name that carries an id takes its
//! record before the handler runs, and settles it by the handler's outcome.

use std::any::type_name;
use std::fmt;
use std::sync::{Arc, OnceLock};

use ruststream::runtime::{BlanketLayer, Context, Handler, HandlerOutcome};
use sqlx::{Database, Error, Pool};
use tracing::warn;

use super::database::Defaults;
use super::events::Tracked;
use super::publish::carried_id;
use super::registry::RecordList;
use super::switch::enabled;

/// The subscription middleware of an [`Outbox`](super::Outbox), from
/// [`layer`](super::Outbox::layer).
///
/// A delivery under a registered name that carries [`OUTBOX_ID_HEADER`](super::OUTBOX_ID_HEADER)
/// takes its record with `Fetch` before the handler runs: a record taken or processed already is
/// acknowledged without the handler, and a fetch that fails, or a missing pool, retries the
/// delivery without it. After the handler, `Ack`, `Retry` or `Discard` settles the record by the
/// outcome; a settlement that fails is logged, and the handler's outcome stands.
///
/// A delivery under any other name, or without the header, runs the handler as it is, and its
/// headers are not read.
pub struct TrackingLayer<DB: Database, Records> {
    pool: Arc<OnceLock<Pool<DB>>>,
    records: Records,
}

impl<DB: Database, Records> TrackingLayer<DB, Records> {
    pub(super) const fn new(pool: Arc<OnceLock<Pool<DB>>>, records: Records) -> Self {
        Self { pool, records }
    }
}

impl<DB: Database, Records: Copy> Clone for TrackingLayer<DB, Records> {
    fn clone(&self) -> Self {
        Self {
            pool: Arc::clone(&self.pool),
            records: self.records,
        }
    }
}

impl<DB: Database, Records: fmt::Debug> fmt::Debug for TrackingLayer<DB, Records> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackingLayer")
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}

impl<DB: Database, Records: RecordList<DB>> BlanketLayer for TrackingLayer<DB, Records> {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Tracking {
            inner: handler,
            pool: Arc::clone(&self.pool),
            records: self.records,
        }
    }
}

/// A handler wrapped by [`TrackingLayer`].
struct Tracking<H, DB: Database, Records> {
    inner: H,
    pool: Arc<OnceLock<Pool<DB>>>,
    records: Records,
}

impl<M, C, S, H, DB, Records> Handler<M, C, S> for Tracking<H, DB, Records>
where
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
    DB: Database,
    Records: RecordList<DB>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        if enabled() && self.records.contains(ctx.name()) {
            self.records
                .deliver(&self.pool, &self.inner, msg, ctx)
                .await
        } else {
            self.inner.handle(msg, ctx).await
        }
    }
}

/// The event that settles a record after its handler.
#[derive(Clone, Copy)]
enum Settlement {
    Ack,
    Retry,
    Discard,
}

impl Settlement {
    /// The event of `outcome` for `Record`, or `None` when it writes nothing.
    fn of<DB: Database, Record: Tracked<DB>>(outcome: &HandlerOutcome) -> Option<Self> {
        if outcome.is_ack() {
            Some(Self::Ack)
        } else if outcome.is_drop() {
            Some(Self::Discard)
        } else if outcome.is_retry() && Record::RETRY_WRITES {
            Some(Self::Retry)
        } else {
            None
        }
    }

    async fn run<DB: Database, Record: Tracked<DB>>(
        self,
        pool: &Pool<DB>,
        id: &Record::Id,
        defaults: &Defaults,
    ) -> Result<(), Error> {
        let mut conn = pool.acquire().await?;
        match self {
            Self::Ack => Record::ack_record(&mut conn, id, defaults).await,
            Self::Retry => Record::retry_record(&mut conn, id, defaults).await,
            Self::Discard => Record::discard_record(&mut conn, id, defaults).await,
        }
    }
}

/// Takes the record `Record` of a delivery under `name`, runs `handler`, and settles the record.
pub(super) async fn deliver<DB, Record, M, C, S, H>(
    name: &'static str,
    defaults: &Defaults,
    pool: &OnceLock<Pool<DB>>,
    handler: &H,
    msg: &M,
    ctx: &mut Context<'_, C, S>,
) -> HandlerOutcome
where
    DB: Database,
    Record: Tracked<DB>,
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    let record = type_name::<Record>();
    let id = match carried_id::<Record::Id>(ctx.headers()) {
        None => return handler.handle(msg, ctx).await,
        Some(Ok(id)) => id,
        Some(Err(text)) => {
            warn!(
                target: "ruststream_sqlx",
                subscription = name,
                record,
                id = text,
                "the outbox id does not parse as the record's id; the delivery runs untracked",
            );
            return handler.handle(msg, ctx).await;
        }
    };
    let Some(pool) = pool.get() else {
        warn!(
            target: "ruststream_sqlx",
            subscription = name,
            record,
            id = %id,
            "the outbox has no pool yet; the delivery is retried without its handler",
        );
        return HandlerOutcome::retry();
    };
    // The connection goes back before the handler runs: the handler may publish tracked messages,
    // and each of those takes one.
    let taken = async {
        let mut conn = pool.acquire().await?;
        Record::fetch_record(&mut conn, &id, defaults).await
    }
    .await;
    match taken {
        Ok(Some(_)) => {}
        Ok(None) => return HandlerOutcome::ack(),
        Err(error) => {
            warn!(
                target: "ruststream_sqlx",
                subscription = name,
                record,
                id = %id,
                %error,
                "the outbox did not take the record; the delivery is retried without its handler",
            );
            return HandlerOutcome::retry();
        }
    }
    let outcome = handler.handle(msg, ctx).await;
    if let Some(settlement) = Settlement::of::<DB, Record>(&outcome)
        && let Err(error) = settlement.run::<DB, Record>(pool, &id, defaults).await
    {
        warn!(
            target: "ruststream_sqlx",
            subscription = name,
            record,
            id = %id,
            %error,
            "the outbox did not settle the record; it stays for the next startup",
        );
    }
    outcome
}
