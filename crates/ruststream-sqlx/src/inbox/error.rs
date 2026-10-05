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
    /// The registration declared a retry the table cannot carry.
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
    }
}
