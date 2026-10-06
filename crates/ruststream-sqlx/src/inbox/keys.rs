//! What a handler reads off the delivery it handles, through `Ctx<Key>`.

use ruststream::{BuildContext, ContextField, IncomingMessage};

/// A delivery's context: what the inbox lends a handler besides the message.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream_sqlx::keys::Attempt;
/// # use ruststream_sqlx::{Inbox, InboxQueue};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(attempt)] attempt: i16, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Report { id: u64 }
///
/// #[subscriber(InboxQueue::<Job>::new("reports"))]
/// async fn render(report: &Report, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
///     tracing::info!(report.id, ?attempt, "rendering");
///     HandlerOutcome::ack()
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct InboxContext {
    attempt: Option<u64>,
}

impl<M: IncomingMessage> BuildContext<M> for InboxContext {
    fn build(msg: &M) -> Self {
        Self {
            attempt: msg.redelivery_count(),
        }
    }
}

/// The delivery's attempt, 1 for the first delivery: the row's `attempt` as it stood before the
/// claim; `None` for a table without one.
///
/// Retry backoff is the handler's: it reads the attempt and answers with
/// `HandlerOutcome::retry_after(..)`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream_sqlx::keys::Attempt;
/// # use ruststream_sqlx::{Inbox, InboxQueue};
/// # #[derive(Inbox, sqlx::FromRow)]
/// # #[inbox(table = "jobs")]
/// # pub struct Job { #[field(id)] id: i64, #[field(retry_after)] retry_after: chrono::DateTime<chrono::Utc>, #[field(attempt)] attempt: i16, #[field(payload)] payload: Vec<u8> }
/// # #[derive(serde::Deserialize)]
/// # struct Charge { order: u64 }
/// # async fn try_charge(_: &Charge) -> bool { true }
///
/// #[subscriber(InboxQueue::<Job>::new("charges"))]
/// async fn charge(request: &Charge, Ctx(attempt): Ctx<Attempt>) -> HandlerOutcome {
///     if try_charge(request).await {
///         return HandlerOutcome::ack();
///     }
///     // Doubling backoff: 1s, 2s, 4s, ...
///     let exponent = u32::try_from(attempt.unwrap_or(1).min(16)).unwrap_or(16);
///     HandlerOutcome::retry_after(Duration::from_secs(1 << (exponent - 1)))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Attempt;

impl ContextField for Attempt {
    type Context = InboxContext;
    type Value = Option<u64>;

    fn read(self, src: &InboxContext) -> Option<u64> {
        src.attempt
    }
}
