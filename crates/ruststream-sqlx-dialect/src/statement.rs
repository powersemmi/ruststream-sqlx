//! What a dialect produces: the text of a statement and the values its placeholders bind.

mod error;

pub use error::{NameLimit, StatementError};

/// A value a statement binds, named by its meaning; the broker supplies it when the statement runs.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{Dialect, Param, Postgres, Statement, StatementError, TableSpec};
/// # use ruststream_sqlx_dialect::TableName;
///
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Dialect for Audited {
///     fn name(&self) -> &'static str {
///         "audited"
///     }
///
///     // A delayed retry of a leased email names the row and the lease its delivery holds; the
///     // broker binds the delay's end, the id and the lease, in this order.
///     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         if spec.table() == "email_jobs" {
///             return Ok(Statement::new(
///                 r#"UPDATE "email_jobs" SET "retry_after" = $1, "locked_until" = NULL WHERE "job_id" = $2 AND "locked_until" = $3"#,
///                 [Param::RetryAfter, Param::Id, Param::Held],
///             ));
///         }
///         Postgres.retry_after(spec)
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
/// }
/// # }
/// # fn main() {}
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
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{Dialect, Param, Postgres, Statement, StatementError, TableSpec};
/// # use ruststream_sqlx_dialect::TableName;
///
/// #[derive(Debug)]
/// pub struct Audited;
///
/// impl Dialect for Audited {
///     fn name(&self) -> &'static str {
///         "audited"
///     }
///
///     // A dropped email stays in its table, marked with the time it was dropped.
///     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         if spec.table() == "email_jobs" {
///             return Ok(Statement::new(
///                 r#"UPDATE "email_jobs" SET "dropped_at" = $1 WHERE "job_id" = $2"#,
///                 [Param::Now, Param::Id],
///             ));
///         }
///         Postgres.discard(spec)
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
/// }
/// # }
/// # fn main() {}
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
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Param, RowLock, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl RowLock for Mssql {
    ///     // The claim of the service's one queue table, which it reads whole.
    ///     fn lock_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         _shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         // This claim keeps no group in order.
    ///         if spec.is_fifo() {
    ///             return Err(StatementError::UnsupportedFifo { dialect: self.name() });
    ///         }
    ///         Ok(Statement::new(
    ///             "SELECT TOP (@p1) [job_id], [payload] FROM [email_jobs] \
    ///              WITH (UPDLOCK, READPAST) ORDER BY [job_id]",
    ///             [Param::Limit],
    ///         ))
    ///     }
    /// }
    /// # impl Dialect for Mssql {
    /// #     fn name(&self) -> &'static str { "mssql" }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// # }
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Postgres, RowLock, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// /// Postgres, with claims that let other rows reference a row in work by foreign key.
    /// #[derive(Debug)]
    /// pub struct NoKeyUpdate;
    ///
    /// impl RowLock for NoKeyUpdate {
    ///     fn lock_claim(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         shape: ClaimShape,
    ///     ) -> Result<Statement, StatementError> {
    ///         // `FOR NO KEY UPDATE` still keeps two claims apart, and lets an insert that
    ///         // references
    ///         // a claimed row go on.
    ///         let claim = Postgres.lock_claim(spec, shape)?;
    ///         let sql = claim
    ///             .sql()
    ///             .replace(" FOR UPDATE SKIP LOCKED", " FOR NO KEY UPDATE SKIP LOCKED");
    ///         Ok(Statement::new(sql, claim.params().iter().copied()))
    ///     }
    /// }
    /// # impl Dialect for NoKeyUpdate {
    /// #     fn name(&self) -> &'static str { "no_key_update" }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// # }
    /// # }
    /// # fn main() {}
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
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Postgres, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Idempotent;
    ///
    /// impl Dialect for Idempotent {
    ///     fn name(&self) -> &'static str {
    ///         "idempotent"
    ///     }
    ///
    ///     // Publishing a job twice keeps one row: the insert skips a row whose id is already
    ///     // there.
    ///     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let insert = Postgres.insert(spec)?;
    ///         Ok(Statement::new(
    ///             format!("{} ON CONFLICT DO NOTHING", insert.sql()),
    ///             insert.params().iter().copied(),
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { Postgres.quote_into(ident, out); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.dead_letter_group(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// }
    /// # }
    /// # fn main() {}
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
/// A claim of a dialect of the service's own selects what the shape asks for:
///
/// ```
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, Param, Role, RowLock, Statement, StatementError, TableSpec,
/// };
/// # use ruststream_sqlx_dialect::TableName;
///
/// /// SQL Server, a database without a built-in dialect.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// impl RowLock for Mssql {
///     // The service's queue tables have no groups and no claim order of their own.
///     fn lock_claim(
///         &self,
///         spec: &TableSpec<'_>,
///         shape: ClaimShape,
///     ) -> Result<Statement, StatementError> {
///         let mut selected = String::new();
///         match shape {
///             // Whole rows, for a struct the broker reads.
///             ClaimShape::Rows => {
///                 for column in spec.columns() {
///                     if !selected.is_empty() {
///                         selected.push_str(", ");
///                     }
///                     self.quote_into(column.name(), &mut selected);
///                 }
///             }
///             // Only the ids, for a struct that fetches its rows itself.
///             ClaimShape::Ids => self.quote_into(spec.id().name(), &mut selected),
///             // The columns that run the queue, each under the name of its role.
///             ClaimShape::Roles => {
///                 let roles = [
///                     Role::Id,
///                     Role::PartitionKey,
///                     Role::Attempt,
///                     Role::Headers,
///                     Role::Payload,
///                 ];
///                 for role in roles {
///                     let Some(column) = spec.column(role) else {
///                         continue;
///                     };
///                     if !selected.is_empty() {
///                         selected.push_str(", ");
///                     }
///                     self.quote_into(column.name(), &mut selected);
///                     selected.push_str(" AS ");
///                     self.quote_into(role.attribute(), &mut selected);
///                 }
///             }
///         }
///         let mut sql = format!("SELECT TOP (@p1) {selected} FROM ");
///         self.quote_into(spec.table(), &mut sql);
///         sql.push_str(" WITH (UPDLOCK, READPAST) ORDER BY ");
///         self.quote_into(spec.id().name(), &mut sql);
///         Ok(Statement::new(sql, [Param::Limit]))
///     }
/// }
/// # impl Dialect for Mssql {
/// #     fn name(&self) -> &'static str { "mssql" }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// # }
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
