//! What a dialect produces: the text of a statement and the values its placeholders bind.

mod error;

pub use error::{NameLimit, StatementError};

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
    /// The lock key of the row, as text: what the advisory claim selected as `__lock`, which the
    /// lock and the unlock name.
    Key,
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

#[cfg(test)]
mod tests {
    use super::{Param, Statement};

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
}
