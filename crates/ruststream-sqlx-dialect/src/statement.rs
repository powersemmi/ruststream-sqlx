//! What a dialect produces: the text of a statement and the values its placeholders bind.

use thiserror::Error;

use crate::role::Role;

/// A value a statement binds, named by its meaning; the broker supplies it when the statement runs.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Param, Statement};
///
/// let retry = Statement::new(
///     r#"UPDATE "jobs" SET "retry_after" = $1 WHERE "job_id" = $2"#,
///     [Param::RetryAfter, Param::Id],
/// );
///
/// // The engine binds one value per parameter, in order.
/// let values: Vec<&str> = retry
///     .params()
///     .iter()
///     .map(|param| match param {
///         Param::RetryAfter => "now + 30s",
///         Param::Id => "42",
///         _ => "unused here",
///     })
///     .collect();
/// assert_eq!(values, ["now + 30s", "42"]);
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

/// What a claim selects: whole rows, or only their ids for a service's own fetch.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimShape {
    /// Every column of the claimed rows.
    Rows,
    /// Only the id of each claimed row.
    Ids,
}

/// Why a dialect cannot build a statement for a table.
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
    /// The dialect has no claim that keeps a group in order.
    #[error("the {dialect} dialect has no claim for FIFO groups")]
    UnsupportedFifo {
        /// The dialect's name.
        dialect: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::{Param, Statement, StatementError};
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
                form: "lease",
            }
            .to_string(),
            "the postgres dialect has no statements for the lease form"
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
}
