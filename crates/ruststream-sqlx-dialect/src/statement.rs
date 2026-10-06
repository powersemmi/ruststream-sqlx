//! What a dialect produces: the text of a statement and the values its placeholders bind.

use std::fmt::{self, Display, Formatter};

use thiserror::Error;

use crate::role::Role;

/// A value a statement binds, named by its meaning; the broker supplies it when the statement runs.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Param, Statement};
///
/// // A delayed retry in the lease form names the row and the lease its delivery holds.
/// let retry = Statement::new(
///     r#"UPDATE "jobs" SET "retry_after" = $1, "locked_until" = NULL WHERE "job_id" = $2 AND "locked_until" = $3"#,
///     [Param::RetryAfter, Param::Id, Param::Held],
/// );
///
/// // The engine binds one value per parameter, in order.
/// let values: Vec<&str> = retry
///     .params()
///     .iter()
///     .map(|param| match param {
///         Param::RetryAfter => "now + 30s",
///         Param::Id => "42",
///         Param::Held => "the expiry its claim wrote",
///         _ => "unused here",
///     })
///     .collect();
/// assert_eq!(values, ["now + 30s", "42", "the expiry its claim wrote"]);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Param {
    /// The current time, in the type of the column it meets.
    Now,
    /// The group the subscription reads.
    Group,
    /// The most rows one claim takes.
    Limit,
    /// The id of the row being settled.
    Id,
    /// The ids a fetch assembles, as one list.
    Ids,
    /// The time before which a row is not claimed again.
    RetryAfter,
    /// Where a dead-lettered row goes: the name of its new group.
    Destination,
    /// The delay of a retry in microseconds, for a statement that adds it to the database's own
    /// time.
    Delay,
    /// The expiry a claim, a stamp or an extension writes into the lease column: the row stays
    /// with its delivery until then.
    Lease,
    /// The current time, in the type of the lease column: a row whose lease ended by then is
    /// claimable.
    LeaseNow,
    /// The expiry the delivery's claim or its last extension wrote: the ownership token that
    /// settlement and extension match.
    Held,
    /// The value of the column at this position of [`TableSpec::columns`](crate::TableSpec::columns),
    /// for an insert.
    Column(usize),
}

/// One SQL statement: its text and the parameters its placeholders bind, in placeholder order.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Param, Statement};
///
/// let ack = Statement::new(r#"DELETE FROM "jobs" WHERE "job_id" = $1"#, [Param::Id]);
/// assert_eq!(ack.params(), [Param::Id]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Statement {
    sql: String,
    params: Vec<Param>,
}

impl Statement {
    /// A statement with its text and the parameters its placeholders bind, in order.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Param, Statement};
    ///
    /// // A SQL Server dialect a service writes itself claims rows with `UPDLOCK, READPAST`.
    /// let claim = Statement::new(
    ///     "SELECT TOP (@p1) [job_id], [payload] FROM [jobs] WITH (UPDLOCK, READPAST) \
    ///      ORDER BY [job_id]",
    ///     [Param::Limit],
    /// );
    /// assert_eq!(claim.params(), [Param::Limit]);
    /// ```
    #[must_use]
    pub fn new(sql: impl Into<String>, params: impl IntoIterator<Item = Param>) -> Self {
        Self {
            sql: sql.into(),
            params: params.into_iter().collect(),
        }
    }

    /// The statement's text.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Param, Statement};
    ///
    /// let ack = Statement::new(r#"DELETE FROM "jobs" WHERE "job_id" = $1"#, [Param::Id]);
    ///
    /// // The startup check names the statement that failed to prepare.
    /// let context = format!("preparing `{}`", ack.sql());
    /// assert_eq!(context, r#"preparing `DELETE FROM "jobs" WHERE "job_id" = $1`"#);
    /// ```
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The parameters the placeholders bind, in placeholder order.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Param, Statement};
    ///
    /// let ack = Statement::new(
    ///     r#"UPDATE "jobs" SET "processed_at" = $1 WHERE "job_id" = $2"#,
    ///     [Param::Now, Param::Id],
    /// );
    ///
    /// // An acknowledgement that marks the row reads the clock.
    /// let reads_clock = ack.params().contains(&Param::Now);
    /// assert!(reads_clock);
    /// ```
    #[must_use]
    pub fn params(&self) -> &[Param] {
        &self.params
    }
}

/// What a claim selects: whole rows, only their ids for a service's own fetch, or the columns
/// that run the queue, each named by its role, for a reader that knows no struct.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::ClaimShape;
///
/// // A struct whose events include its own fetch claims ids and assembles the rows itself.
/// let custom_fetch = true;
/// let shape = if custom_fetch { ClaimShape::Ids } else { ClaimShape::Rows };
/// assert_eq!(shape, ClaimShape::Ids);
/// ```
///
/// A claim by role reads each column under the name of the role it plays:
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{ClaimShape, Column, Form, Postgres, RowLock, TableSpec};
///
/// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
///     .payload(Column::new("body"));
///
/// let claim = Postgres.lock_claim(&JOBS, ClaimShape::Roles)?;
/// assert!(claim.sql().starts_with(r#"SELECT "job_id" AS "id", "body" AS "payload" FROM"#));
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimShape {
    /// Every column of the claimed rows.
    Rows,
    /// Only the id of each claimed row.
    Ids,
    /// Only the columns that play `id`, `partition_key`, `attempt`, `headers` and `payload`,
    /// each under its role's attribute name as an alias, in [`Role::ALL`](crate::Role::ALL)
    /// order.
    Roles,
}

/// The longest name a database keeps, in the unit it measures names by.
///
/// Postgres measures a name in bytes, MySQL and MariaDB in characters, so a name of accented
/// letters can fit one database and not the other.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{NameLimit, StatementError};
///
/// // A SQL Server dialect of the service's own refuses a name over 128 characters.
/// fn checked(name: &str) -> Result<&str, StatementError> {
///     if name.chars().count() <= 128 {
///         return Ok(name);
///     }
///     Err(StatementError::IdentifierTooLong {
///         dialect: "mssql",
///         identifier: name.to_owned(),
///         limit: NameLimit::Characters(128),
///     })
/// }
///
/// let name = "n".repeat(129);
/// let refused = checked(&name).map_err(|refused| refused.to_string());
/// assert!(refused.is_err_and(|message| {
///     message.ends_with("is longer than the 128 characters the mssql dialect allows in a name")
/// }));
/// ```
// A `u16` holds every database's limit and keeps `StatementError` at 56 bytes: a `usize` would
// grow it, and every error that carries it, by eight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NameLimit {
    /// At most this many bytes of UTF-8.
    Bytes(u16),
    /// At most this many characters.
    Characters(u16),
}

impl Display for NameLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(limit) => write!(f, "{limit} bytes"),
            Self::Characters(limit) => write!(f, "{limit} characters"),
        }
    }
}

/// Why a dialect cannot build a statement for a table.
///
/// A broker builds a subscription's statements when the subscription starts, so a refusal stops
/// it before it claims a row, and the message names the change to make.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Role, StatementError};
///
/// let error = StatementError::MissingRole {
///     statement: "retry_after",
///     role: Role::RetryAfter,
/// };
/// assert_eq!(
///     error.to_string(),
///     "the retry_after statement needs a column playing `retry_after`: add \
///      `#[field(retry_after)]` to the struct",
/// );
/// ```
///
/// A lease table cannot read the database's clock, and every statement of its form says so:
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Column, Form, Lease, Postgres, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
///         .database_clock();
///
/// let refused = Postgres.lease_claim(&JOBS, ClaimShape::Rows);
/// assert_eq!(
///     refused,
///     Err(StatementError::LeaseOnDatabaseClock { dialect: "postgres" })
/// );
/// # }
/// ```
///
/// A dialect of the service's own that builds no claim for FIFO groups refuses a table with them:
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Form, Param, Statement, StatementError, TableSpec};
///
/// // The row lock claim of a SQL Server dialect the service writes itself.
/// fn lock_claim(spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///     if spec.is_fifo() {
///         return Err(StatementError::UnsupportedFifo { dialect: "mssql" });
///     }
///     Ok(Statement::new(
///         "SELECT TOP (@p1) [id], [payload] FROM [ledger] WITH (UPDLOCK, READPAST) ORDER BY [id]",
///         [Param::Limit],
///     ))
/// }
///
/// const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
///     .fifo_group(Column::new("account"));
///
/// let refused = lock_claim(&LEDGER).map_err(|refused| refused.to_string());
/// assert_eq!(refused, Err("the mssql dialect has no claim for FIFO groups".to_owned()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum StatementError {
    /// The statement needs a column that plays `role`, and the table has none.
    #[error(
        "the {statement} statement needs a column playing `{role}`: add `#[field({role})]` to \
         the struct"
    )]
    MissingRole {
        /// The statement being built.
        statement: &'static str,
        /// The role it needs.
        role: Role,
    },
    /// The dialect has no statements for the table's form.
    #[error("the {dialect} dialect has no statements for the {form} form")]
    UnsupportedForm {
        /// The dialect's name.
        dialect: &'static str,
        /// The name of the table's form.
        form: &'static str,
    },
    /// The statement belongs to one form, and the table takes rows in another: the row lock
    /// claim asked for a lease table, or a lease statement for a table without a lease.
    #[error("the {statement} statement does not serve a table in the {form} form")]
    FormMismatch {
        /// The statement being built.
        statement: &'static str,
        /// The name of the table's form.
        form: &'static str,
    },
    /// The dialect has no claim that keeps a group in order. A dialect of the service's own
    /// returns it from a claim of a table with FIFO groups when it builds no claim for them.
    #[error("the {dialect} dialect has no claim for FIFO groups")]
    UnsupportedFifo {
        /// The dialect's name.
        dialect: &'static str,
    },
    /// A table, schema or column name is longer than the database allows. Postgres would cut it
    /// short without a word and address another object; MySQL would refuse the statement.
    #[error("`{identifier}` is longer than the {limit} the {dialect} dialect allows in a name")]
    IdentifierTooLong {
        /// The dialect's name.
        dialect: &'static str,
        /// The name that is too long.
        identifier: String,
        /// The longest name the database keeps, in the unit it measures names by.
        limit: NameLimit,
    },
    /// The statement needs every column, and the struct flattens another whose columns the
    /// description cannot see.
    #[error(
        "the {statement} statement needs every column, and a `#[sqlx(flatten)]` field hides some: \
         write it in the service"
    )]
    Flattened {
        /// The statement being built.
        statement: &'static str,
    },
    /// The server is older than the dialect's statements need, as
    /// [`Dialect::check_server`](crate::Dialect::check_server) found when the subscription
    /// started.
    #[error(
        "the {dialect} dialect needs {required} or later for this form; the server reports \
         `{server}`"
    )]
    ServerTooOld {
        /// The dialect's name.
        dialect: &'static str,
        /// The version the server reports.
        server: String,
        /// The oldest server the dialect's statements run on.
        required: &'static str,
    },
    /// The dialect cannot read rows by a list of ids, so a claim of the service's own needs a
    /// fetch of its own beside it.
    #[error(
        "the {dialect} dialect cannot read rows by a list of ids: list `fetch` in `custom(..)` \
         beside `claim`"
    )]
    UnsupportedFetch {
        /// The dialect's name.
        dialect: &'static str,
    },
    /// The table declares a lease and reads the database's clock. Settlement matches the expiry
    /// the claim wrote, so the claim computes it from the crate's clock.
    #[error(
        "the lease form computes its expiry from the crate's clock: a table on `DatabaseClock` \
         cannot declare `locked_until`"
    )]
    LeaseOnDatabaseClock {
        /// The dialect's name.
        dialect: &'static str,
    },
    /// The dialect opens no transaction at the table's isolation level or mode, as
    /// [`Dialect::begin`](crate::Dialect::begin) found.
    #[error("the {dialect} dialect opens no transaction at {opening}")]
    UnsupportedOpening {
        /// The dialect's name.
        dialect: &'static str,
        /// The opening, as [`Opening::name`](crate::Opening::name) names it.
        opening: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::{NameLimit, Param, Statement, StatementError};
    use crate::role::Role;

    #[test]
    fn a_statement_keeps_its_text_and_parameters_in_order() {
        let statement = Statement::new(
            "UPDATE jobs SET processed_at = $1 WHERE job_id = $2",
            [Param::Now, Param::Id],
        );
        assert_eq!(
            statement.sql(),
            "UPDATE jobs SET processed_at = $1 WHERE job_id = $2"
        );
        assert_eq!(statement.params(), [Param::Now, Param::Id]);
    }

    #[test]
    fn errors_name_the_missing_piece() {
        assert_eq!(
            StatementError::UnsupportedForm {
                dialect: "postgres",
                form: "advisory lock",
            }
            .to_string(),
            "the postgres dialect has no statements for the advisory lock form"
        );
        assert_eq!(
            StatementError::UnsupportedFifo {
                dialect: "postgres"
            }
            .to_string(),
            "the postgres dialect has no claim for FIFO groups"
        );
        assert_eq!(
            StatementError::MissingRole {
                statement: "dead_letter_group",
                role: Role::Group,
            }
            .to_string(),
            "the dead_letter_group statement needs a column playing `group`: add `#[field(group)]` \
             to the struct"
        );
    }

    #[test]
    fn the_new_errors_name_what_to_fix() {
        assert_eq!(
            StatementError::IdentifierTooLong {
                dialect: "postgres",
                identifier: "jobs".to_owned(),
                limit: NameLimit::Bytes(63),
            }
            .to_string(),
            "`jobs` is longer than the 63 bytes the postgres dialect allows in a name"
        );
        assert_eq!(
            StatementError::IdentifierTooLong {
                dialect: "mysql",
                identifier: "jobs".to_owned(),
                limit: NameLimit::Characters(64),
            }
            .to_string(),
            "`jobs` is longer than the 64 characters the mysql dialect allows in a name"
        );
        assert_eq!(
            StatementError::Flattened {
                statement: "insert"
            }
            .to_string(),
            "the insert statement needs every column, and a `#[sqlx(flatten)]` field hides some: \
             write it in the service"
        );
        assert_eq!(
            StatementError::FormMismatch {
                statement: "extend",
                form: "row lock",
            }
            .to_string(),
            "the extend statement does not serve a table in the row lock form"
        );
        assert_eq!(
            StatementError::UnsupportedOpening {
                dialect: "postgres",
                opening: "isolation `read_uncommitted`",
            }
            .to_string(),
            "the postgres dialect opens no transaction at isolation `read_uncommitted`"
        );
    }
}
