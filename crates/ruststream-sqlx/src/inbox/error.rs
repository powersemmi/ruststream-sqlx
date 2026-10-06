//! The inbox broker's one error.

use ruststream_sqlx_dialect::StatementError;
use thiserror::Error;

/// What can go wrong in the inbox broker.
///
/// Every variant about a subscription names the subscription, the table and the row type, so one
/// line of a log says where to look.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::SqlxBrokerError;
///
/// let error = SqlxBrokerError::NoRoute { name: "orders".to_owned() };
/// assert_eq!(
///     error.to_string(),
///     "no route leads `orders` to a table: add `.route::<Row>(\"orders\")` to the broker"
/// );
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqlxBrokerError {
    /// `connect` could not take a connection from the pool.
    #[error("the inbox broker could not take a connection from the pool: {source}")]
    Connect {
        /// The pool's error.
        #[source]
        source: sqlx::Error,
    },
    /// `connect` found the pool on a database no built-in dialect serves: an `AnyPool`'s backend
    /// whose feature is off.
    #[error(
        "no built-in dialect serves the `{backend}` backend of this `AnyPool`: enable its feature, \
         or pass a dialect with `SqlxBroker::with_dialect`"
    )]
    Backend {
        /// The backend's name, as its sqlx driver reports it.
        backend: String,
    },
    /// A statement failed while the subscription ran.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}): `{statement}` failed: {source}"
    )]
    Sqlx {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The statement, or the event a service's own impl ran.
        statement: &'static str,
        /// The database's error.
        #[source]
        source: Box<sqlx::Error>,
    },
    /// A statement failed to prepare at startup: the table does not match the struct.
    ///
    /// Preparing checks the names of the table and its columns, not the column types; a claimed
    /// row whose columns do not decode goes to the subscription's decode-failure policy instead.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}): the database refused \
         `{statement}`: {source}"
    )]
    Schema {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The statement that failed to prepare.
        statement: &'static str,
        /// The database's error.
        #[source]
        source: Box<sqlx::Error>,
    },
    /// The dialect refused to build a statement for the table.
    #[error("subscription `{subscription}` on table `{table}` ({row}): {source}")]
    Dialect {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// What the dialect could not build.
        #[source]
        source: StatementError,
    },
    /// The server is older than the statements of the subscription's form need.
    ///
    /// A subscription reads the server's version when it opens, where its dialect asks for it:
    /// MySQL claims rows with `SKIP LOCKED` in both forms, which MySQL 8.0.1 and MariaDB 10.6
    /// added.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}): the server reports \
         `{server}`, and this form needs {required} or later"
    )]
    ServerTooOld {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The version the server reports.
        server: String,
        /// The oldest server the subscription's statements run on.
        required: &'static str,
    },
    /// The registration declared what the table cannot carry: a retry, or a mode its form does
    /// not run.
    #[error("subscription `{subscription}` on table `{table}` ({row}): {reason}")]
    Declaration {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// What to change.
        reason: String,
    },
    /// The connection already reads this queue: two subscriptions would compete for its rows.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}) is open already on this \
         connection: one queue has one subscription per process; scale it with `workers(n)`"
    )]
    AlreadySubscribed {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
    },
    /// A settlement of a batch whose transaction an earlier settlement's failure rolls back.
    ///
    /// A batch that holds a row whose statement always fails rolls back and returns all of its
    /// rows on every attempt, until the service's SQL or schema is fixed.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}): an earlier settlement of this \
         batch failed, so the batch rolls back and its rows return to the queue"
    )]
    BatchRolledBack {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
    },
    /// A settlement in the lease form found its row under another lease, so it took no effect.
    ///
    /// Ownership of a leased row ends with its lease, not with its holder. The subscription
    /// extends the lease of every delivery in work each half lease, so a lease runs out under a
    /// running handler only when its extensions fail or stop (the database out of reach, the
    /// subscription closed or the broker shut down). The handler then shares the row with the
    /// next claim, and the lease, the ownership token, only stops the late holder from settling.
    ///
    /// A settlement whose statement fails keeps the row under its lease until the lease runs
    /// out, as a crash does; a delivery dropped unsettled releases its row at once.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}): the lease on row {id} ran out \
         and another claim took it; this settlement did not take effect"
    )]
    LeaseLost {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The row's id, as logs name it.
        id: String,
    },
    /// A settlement in transactional mode found the delivery's transaction still lent to its
    /// handler's [`Tx`](crate::Tx), which the handler moved somewhere that outlived it.
    ///
    /// The settlement took no effect. The transaction ends when that `Tx` drops: its connection
    /// closes, the server rolls back what the handler wrote, and the row returns to the queue.
    #[error(
        "subscription `{subscription}` on table `{table}` ({row}): the handler still holds the \
         transaction of row {id}, so its settlement took no effect; the transaction rolls back \
         when the handler's `Tx` drops"
    )]
    TransactionHeld {
        /// The subscription.
        subscription: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The row's id, as logs name it.
        id: String,
    },
    /// The service's `Publish` failed.
    #[error("publishing to `{name}` into table `{table}` ({row}) failed: {source}")]
    Publish {
        /// The name published to.
        name: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The database's error.
        #[source]
        source: Box<sqlx::Error>,
    },
    /// The publish carries a header the table cannot hold byte for byte, so it was refused.
    #[error(
        "publishing to `{name}` into table `{table}` ({row}) was refused: the table cannot hold \
         header `{header}` byte for byte"
    )]
    Header {
        /// The name published to.
        name: String,
        /// The table, qualified with its schema.
        table: String,
        /// The row type.
        row: &'static str,
        /// The header.
        header: String,
    },
    /// No route leads the name to a table.
    #[error("no route leads `{name}` to a table: add `.route::<Row>(\"{name}\")` to the broker")]
    NoRoute {
        /// The name published to.
        name: String,
    },
    /// The broker was shut down; the handle outlived it.
    #[error("the inbox broker is shut down")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::SqlxBrokerError;

    #[test]
    fn every_message_names_where_to_look() {
        let error = SqlxBrokerError::AlreadySubscribed {
            subscription: "emails".to_owned(),
            table: "app.email_jobs".to_owned(),
            row: "SendEmail",
        };
        assert_eq!(
            error.to_string(),
            "subscription `emails` on table `app.email_jobs` (SendEmail) is open already on this \
             connection: one queue has one subscription per process; scale it with `workers(n)`"
        );
        assert_eq!(
            SqlxBrokerError::Closed.to_string(),
            "the inbox broker is shut down"
        );
        let backend = SqlxBrokerError::Backend {
            backend: "MySQL".to_owned(),
        };
        assert_eq!(
            backend.to_string(),
            "no built-in dialect serves the `MySQL` backend of this `AnyPool`: enable its feature, \
             or pass a dialect with `SqlxBroker::with_dialect`"
        );
        let lost = SqlxBrokerError::LeaseLost {
            subscription: "emails".to_owned(),
            table: "email_jobs".to_owned(),
            row: "SendEmail",
            id: "7".to_owned(),
        };
        assert_eq!(
            lost.to_string(),
            "subscription `emails` on table `email_jobs` (SendEmail): the lease on row 7 ran out \
             and another claim took it; this settlement did not take effect"
        );
        let held = SqlxBrokerError::TransactionHeld {
            subscription: "emails".to_owned(),
            table: "email_jobs".to_owned(),
            row: "SendEmail",
            id: "7".to_owned(),
        };
        assert_eq!(
            held.to_string(),
            "subscription `emails` on table `email_jobs` (SendEmail): the handler still holds the \
             transaction of row 7, so its settlement took no effect; the transaction rolls back \
             when the handler's `Tx` drops"
        );
        let old = SqlxBrokerError::ServerTooOld {
            subscription: "emails".to_owned(),
            table: "email_jobs".to_owned(),
            row: "SendEmail",
            server: "10.5.23-MariaDB".to_owned(),
            required: "MariaDB 10.6",
        };
        assert_eq!(
            old.to_string(),
            "subscription `emails` on table `email_jobs` (SendEmail): the server reports \
             `10.5.23-MariaDB`, and this form needs MariaDB 10.6 or later"
        );
    }
}
