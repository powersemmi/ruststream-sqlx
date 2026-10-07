//! `LISTEN/NOTIFY`: the databases whose sessions notify each other, the broker's listening
//! connection, and the notification a publish sends.
//!
//! A broker with [`listen_notify`](crate::SqlxBroker::listen_notify) holds one connection of the
//! pool for its life, driven by one task: each subscription's open listens on its table's channel
//! there, and each notification wakes the subscriptions of the table and group it names. A publish
//! announces its row on the same connection right after the write, so a listener never claims
//! before the row is visible.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use sqlx::{Database, Pool};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::QueueDatabase;
use crate::inbox::broker::Shared;
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::publish::TableWake;
#[cfg(feature = "testing")]
use crate::inbox::testing::{cancelled, off_clock};

/// The longest channel name Postgres keeps: an identifier of `NAMEDATALEN - 1` bytes.
const CHANNEL_LIMIT: usize = 63;

/// A database whose sessions notify each other: a broker on it may
/// [`listen_notify`](crate::SqlxBroker::listen_notify).
///
/// Postgres alone implements it, with `LISTEN` and `pg_notify`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// use ruststream_sqlx::prelude::*;
/// use ruststream_sqlx::{BuiltInDialect, Notifies};
/// use serde::Deserialize;
/// use sqlx::{PgPool, Pool};
///
/// #[derive(Inbox, sqlx::FromRow)]
/// #[inbox(table = "email_jobs")]
/// pub struct SendEmail {
///     #[field(id, generated)]
///     job_id: i64,
///     #[field(group)]
///     name: String,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// #[derive(Deserialize)]
/// struct Email {
///     to: String,
/// }
///
/// #[subscriber(InboxQueue::<SendEmail>::new("emails"))]
/// async fn send(email: &Email) -> HandlerOutcome {
///     tracing::info!(to = %email.to, "sending");
///     HandlerOutcome::ack()
/// }
///
/// // The service's broker wakes on notifications, on whichever database sends them.
/// pub fn notified<DB: Notifies + BuiltInDialect>(pool: Pool<DB>) -> SqlxBroker<DB> {
///     SqlxBroker::new(pool).listen_notify()
/// }
///
/// pub fn app(pool: PgPool) -> RustStream {
///     RustStream::new(AppInfo::new("mailer", "0.1.0")).with_broker(notified(pool), |b| {
///         b.include(send);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`LISTEN/NOTIFY` is Postgres's: `{Self}` has no notifications",
    label = "this database cannot listen",
    note = "drop `.listen_notify()`: the subscriptions wake on the poll interval and on publishes \
            from this process"
)]
pub trait Notifies: QueueDatabase + sealed::Listens {}

#[cfg(feature = "postgres")]
impl Notifies for sqlx::Postgres {}

/// What starts a broker's listening connection on its pool, once, at `connect`.
pub(crate) type StartListening<DB> = fn(Pool<DB>) -> Starting;

/// The start of a listening connection: boxed, once per broker.
pub(crate) type Starting = Pin<Box<dyn Future<Output = Result<Listening, sqlx::Error>> + Send>>;

/// The start of the listening connection of a database that notifies.
pub(crate) fn start_listening<DB: Notifies>() -> StartListening<DB> {
    <DB as sealed::Listens>::listen
}

mod sealed {
    use sqlx::{Database, Pool};

    use super::Starting;

    /// The machinery of [`Notifies`](super::Notifies): a database outside this crate cannot
    /// implement it.
    pub trait Listens: Database {
        /// Opens the listening connection on `pool` and starts the task that drives it.
        #[expect(
            private_interfaces,
            reason = "the trait is sealed: nothing outside the crate names or implements it"
        )]
        fn listen(pool: Pool<Self>) -> Starting;
    }
}

/// A broker's listening connection: the task that drives it, and the way to it.
pub(crate) struct Listening {
    /// The statement a publish announces its row with, binding the channel and the group.
    notify: &'static str,
    commands: mpsc::UnboundedSender<Listen>,
    stop: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

/// A subscription's request to listen on its table's channel, answered once it is listened.
#[cfg_attr(
    not(feature = "postgres"),
    expect(dead_code, reason = "a listening task runs on Postgres alone")
)]
struct Listen {
    table: &'static TableWake,
    listened: oneshot::Sender<Result<(), sqlx::Error>>,
}

impl Listening {
    /// The statement a publish announces its row with.
    pub(crate) const fn notify(&self) -> &'static str {
        self.notify
    }

    /// Listens on the channel of `table`, and returns once the connection listens there.
    async fn listen(&self, table: &'static TableWake) -> Result<(), sqlx::Error> {
        let (listened, answer) = oneshot::channel();
        // Why a run-time check: the task stops when the broker shuts down, and a subscription may
        // open while it does.
        if self.commands.send(Listen { table, listened }).is_err() {
            return Err(stopped());
        }
        answer.await.unwrap_or_else(|_| Err(stopped()))
    }

    /// Stops the task, which stops listening on every channel before it ends.
    async fn close(&self) {
        self.stop.cancel();
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// The error of a listen the listening task did not answer: it stopped.
fn stopped() -> sqlx::Error {
    sqlx::Error::Io(io::Error::other("the listening connection is closed"))
}

/// Listens on the channel of `table` for the subscription `subscription` of `row`, when the
/// broker listens, off a paused clock where the connection runs in process.
pub(crate) async fn listen<DB: Database>(
    shared: &Arc<Shared<DB>>,
    table: &'static TableWake,
    subscription: &str,
    row: &'static str,
) -> Result<(), SqlxBrokerError> {
    let Some(listening) = &shared.listening else {
        return Ok(());
    };
    // Why a startup refusal: the limit is Postgres's, and `pg_notify` refuses a longer channel
    // while `LISTEN` cuts it, so the subscription would never hear its table.
    if table.table().len() > CHANNEL_LIMIT {
        return Err(SqlxBrokerError::Declaration {
            subscription: subscription.to_owned(),
            table: table.table().to_owned(),
            row,
            reason: format!(
                "the table's qualified name is {} bytes, and a Postgres notification channel \
                 holds at most {CHANNEL_LIMIT}: shorten the name or drop `.listen_notify()`",
                table.table().len()
            ),
        });
    }
    #[cfg(feature = "testing")]
    let listened = if shared.harness.in_process() {
        let shared = Arc::clone(shared);
        off_clock(async move {
            match &shared.listening {
                Some(listening) => listening.listen(table).await,
                None => Ok(()),
            }
        })
        .await
        .unwrap_or_else(|| Err(cancelled()))
    } else {
        listening.listen(table).await
    };
    #[cfg(not(feature = "testing"))]
    let listened = listening.listen(table).await;
    listened.map_err(|source| SqlxBrokerError::Listen {
        subscription: subscription.to_owned(),
        table: table.table().to_owned(),
        row,
        source: Box::new(source),
    })
}

/// Closes the broker's listening connection, when it listens, off a paused clock where the
/// connection runs in process.
pub(crate) async fn close<DB: Database>(shared: &Arc<Shared<DB>>) {
    let Some(listening) = &shared.listening else {
        return;
    };
    #[cfg(feature = "testing")]
    if shared.harness.in_process() {
        let shared = Arc::clone(shared);
        let _ = off_clock(async move {
            if let Some(listening) = &shared.listening {
                listening.close().await;
            }
        })
        .await;
        return;
    }
    listening.close().await;
}

/// Announces a row written into `table`'s table for the group `name` on `conn`, the connection
/// that wrote it, with the statement `notify`.
///
/// The row is written whatever this answers, and the poll interval still reaches it, so a failed
/// notification is a warning, not a failed publish.
pub(crate) async fn announce<DB: QueueDatabase>(
    notify: &'static str,
    conn: &mut DB::Connection,
    table: &TableWake,
    name: &str,
) {
    let mut arguments = DB::Arguments::default();
    let announced = match DB::bind_str(&mut arguments, table.table())
        .and_then(|()| DB::bind_str(&mut arguments, name))
    {
        Ok(()) => DB::execute(conn, notify, arguments).await.map(drop),
        Err(error) => Err(error),
    };
    if let Err(error) = announced {
        tracing::warn!(
            target: "ruststream_sqlx",
            %error,
            name,
            table = table.table(),
            "the row is written, and its notification failed: subscriptions in other processes \
             reach it on their poll interval"
        );
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use std::ptr;
    use std::sync::Mutex;
    use std::time::Duration;

    use sqlx::postgres::{PgListener, PgNotification};
    use sqlx::{PgPool, Postgres};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::sealed::Listens;
    use super::{Listen, Listening, Starting};
    use crate::inbox::publish::TableWake;

    /// How long the task waits before it opens the listening connection again after an attempt
    /// failed, so a database out of reach cannot spin it.
    const RETRY: Duration = Duration::from_secs(1);

    impl Listens for Postgres {
        #[expect(
            private_interfaces,
            reason = "the trait is sealed: nothing outside the crate names or implements it"
        )]
        fn listen(pool: PgPool) -> Starting {
            Box::pin(async move {
                let listener = open(&pool, &[]).await?;
                let (commands, received) = mpsc::unbounded_channel();
                let stop = CancellationToken::new();
                let task = tokio::spawn(run(pool, listener, received, stop.clone()));
                Ok(Listening {
                    notify: "SELECT pg_notify($1, $2)",
                    commands,
                    stop,
                    task: Mutex::new(Some(task)),
                })
            })
        }
    }

    /// A listener on a connection of `pool`, listening on the channel of each of `tables`.
    async fn open(pool: &PgPool, tables: &[&'static TableWake]) -> Result<PgListener, sqlx::Error> {
        let mut listener = PgListener::connect_with(pool).await?;
        // A lost connection then comes back from `try_recv` as `Ok(None)` at once, and this task
        // opens a listener again itself, outside the `select!` that may drop a `try_recv`: a
        // reconnect dropped halfway would leave a pooled connection listening.
        listener.eager_reconnect(false);
        if !tables.is_empty() {
            listener
                .listen_all(tables.iter().map(|table| table.table()))
                .await?;
        }
        Ok(listener)
    }

    /// Drives `listener` until `stop`: listens for each subscription that asks, wakes the
    /// subscriptions each notification names, and listens again after a lost connection.
    async fn run(
        pool: PgPool,
        mut listener: PgListener,
        mut commands: mpsc::UnboundedReceiver<Listen>,
        stop: CancellationToken,
    ) {
        let mut tables: Vec<&'static TableWake> = Vec::new();
        loop {
            // `try_recv` is dropped only while it waits for the server's next message, which
            // sqlx reads cancel-safely; with `eager_reconnect` off it awaits nothing else.
            let lost = tokio::select! {
                biased;
                () = stop.cancelled() => break,
                command = commands.recv() => {
                    // Every handle of the broker dropped without a shutdown: nobody listens.
                    let Some(Listen { table, listened }) = command else { break };
                    let _ = listened.send(listen(&mut listener, &mut tables, table).await);
                    continue;
                }
                received = listener.try_recv() => match received {
                    Ok(Some(notification)) => {
                        wake(&tables, &notification);
                        continue;
                    }
                    Ok(None) => None,
                    Err(error) => Some(error),
                },
            };
            // FIXME(sqlx-postgres 0.9.0): `try_recv` answers `Ok(None)` for a lost connection only
            // when the socket reports an abort, an end of file, a timeout or a broken pipe; a
            // reset connection comes back as an error, with the dead connection kept. Every error
            // is taken as a lost connection here; once sqlx reports every loss as `Ok(None)`, an
            // error goes to the log alone.
            tracing::warn!(
                target: "ruststream_sqlx",
                error = lost.as_ref().map(tracing::field::display),
                tables = ?tables.iter().map(|table| table.table()).collect::<Vec<_>>(),
                "the listening connection was lost; listening again"
            );
            let Some(again) = reopen(&pool, &tables, &stop).await else {
                break;
            };
            listener = again;
            // A notification sent while no session listened is gone, so every subscription
            // claims once.
            for table in &tables {
                table.wake_all();
            }
        }
        // FIXME(sqlx-postgres 0.9.0): a `PgListener` hands its connection back only from its
        // `Drop`, which spawns a task that runs `UNLISTEN *` and returns the connection to the
        // pool, unawaited. The listener stops listening here first, awaited, so no channel is
        // listened once the task ends; once sqlx offers an awaited close, it replaces this.
        if let Err(error) = listener.unlisten_all().await {
            tracing::debug!(
                target: "ruststream_sqlx",
                %error,
                "the listening connection did not stop listening; it is closed"
            );
        }
        drop(listener);
    }

    /// Listens on the channel of `table`, unless the connection listens there already.
    async fn listen(
        listener: &mut PgListener,
        tables: &mut Vec<&'static TableWake>,
        table: &'static TableWake,
    ) -> Result<(), sqlx::Error> {
        if tables.iter().any(|listened| ptr::eq(*listened, table)) {
            return Ok(());
        }
        listener.listen(table.table()).await?;
        tables.push(table);
        Ok(())
    }

    /// Wakes the subscriptions `notification` names: its table's subscription of the group its
    /// payload names, or every subscription of the table for an empty payload.
    fn wake(tables: &[&'static TableWake], notification: &PgNotification) {
        let Some(table) = tables
            .iter()
            .find(|table| table.table() == notification.channel())
        else {
            return;
        };
        match notification.payload() {
            "" => table.wake_all(),
            group => table.wake(group),
        }
    }

    /// A listener on the channels of `tables` again, or `None` once `stop` is cancelled.
    async fn reopen(
        pool: &PgPool,
        tables: &[&'static TableWake],
        stop: &CancellationToken,
    ) -> Option<PgListener> {
        loop {
            if stop.is_cancelled() {
                return None;
            }
            // Not raced against `stop`: an open dropped halfway would leave its connection
            // listening; the pool's acquire timeout bounds it.
            match open(pool, tables).await {
                Ok(listener) => return Some(listener),
                Err(error) => {
                    tracing::warn!(
                        target: "ruststream_sqlx",
                        %error,
                        tables = ?tables.iter().map(|table| table.table()).collect::<Vec<_>>(),
                        "the listening connection did not open; trying again"
                    );
                    tokio::select! {
                        () = stop.cancelled() => return None,
                        () = tokio::time::sleep(RETRY) => {}
                    }
                }
            }
        }
    }
}
