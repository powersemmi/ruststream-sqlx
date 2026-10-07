//! One column of a queue table.

/// One column of a queue table: its name in the database, and whether the database fills it in.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # mod demo {
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Dialect, Form, Lease, Param, Postgres, Statement, StatementError, TableSpec,
/// };
/// # use ruststream_sqlx_dialect::TableName;
///
/// /// Postgres, recording when a handler last extended its lease: every lease table of the
/// /// service has a `seen_at` column.
/// #[derive(Debug)]
/// pub struct Watched;
///
/// impl Lease for Watched {
///     fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         // The lease form carries the column of its expiry.
///         let Form::Lease(expiry) = spec.form() else {
///             return Postgres.extend(spec);
///         };
///         let mut sql = String::from("UPDATE ");
///         self.quote_into(spec.table(), &mut sql);
///         sql.push_str(" SET ");
///         self.quote_into(expiry.name(), &mut sql);
///         sql.push_str(r#" = $1, "seen_at" = $2 WHERE "#);
///         self.quote_into(spec.id().name(), &mut sql);
///         sql.push_str(" = $3 AND ");
///         self.quote_into(expiry.name(), &mut sql);
///         sql.push_str(" = $4");
///         Ok(Statement::new(sql, [Param::Lease, Param::Now, Param::Id, Param::Held]))
///     }
/// #     fn lease_claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError> { Postgres.lease_claim(spec, shape) }
/// #     fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.stamp(spec) }
/// }
/// # impl Dialect for Watched {
/// #     fn name(&self) -> &'static str { "watched" }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Column<'a> {
    name: &'a str,
    generated: bool,
}

impl<'a> Column<'a> {
    /// A column the service writes.
    ///
    /// `#[derive(Inbox)]` names the column of each field with it.
    #[must_use]
    pub const fn new(name: &'a str) -> Self {
        Self {
            name,
            generated: false,
        }
    }

    /// The same column, filled in by the database, so an insert leaves it out.
    ///
    /// `#[field(generated)]` on a field sets it, as `#[field(id, generated)]` does on a
    /// sequence's id.
    #[must_use]
    pub const fn generated(self) -> Self {
        Self {
            generated: true,
            ..self
        }
    }

    /// The column's name in the database.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # mod demo {
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Form, Param, Postgres, Role, Statement, StatementError, TableSpec,
    /// };
    /// # use ruststream_sqlx_dialect::TableName;
    ///
    /// #[derive(Debug)]
    /// pub struct Restarted;
    ///
    /// impl Dialect for Restarted {
    ///     fn name(&self) -> &'static str {
    ///         "restarted"
    ///     }
    ///
    ///     // A dead letter starts its new group with the attempt count of a new row.
    ///     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let (Form::RowLock, Some(group), Some(attempt)) =
    ///             (spec.form(), spec.column(Role::Group), spec.column(Role::Attempt))
    ///         else {
    ///             return Postgres.dead_letter_group(spec);
    ///         };
    ///         let mut sql = String::from("UPDATE ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" SET ");
    ///         self.quote_into(group.name(), &mut sql);
    ///         sql.push_str(" = $1, ");
    ///         self.quote_into(attempt.name(), &mut sql);
    ///         sql.push_str(" = DEFAULT WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = $2");
    ///         Ok(Statement::new(sql, [Param::Destination, Param::Id]))
    ///     }
    ///
    ///     fn quote_into(&self, ident: &str, out: &mut String) {
    ///         Postgres.quote_into(ident, out);
    ///     }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { Postgres.placeholder_into(index, out); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.fetch(spec) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.ack(spec) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Postgres.retry(spec) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.retry_after(spec) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.discard(spec) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Postgres.dead_letter_table(spec, target) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Postgres.insert(spec) }
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn name(&self) -> &'a str {
        self.name
    }

    /// Whether the database fills the column in, so an insert leaves it out.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ruststream_sqlx_dialect::TableName;
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    ///
    /// /// SQL Server, a database without a built-in dialect.
    /// #[derive(Debug)]
    /// pub struct Mssql;
    ///
    /// impl Dialect for Mssql {
    ///     fn name(&self) -> &'static str {
    ///         "mssql"
    ///     }
    ///
    ///     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.selects_all() {
    ///             return Err(StatementError::Flattened { statement: "insert" });
    ///         }
    ///         let mut names = String::new();
    ///         let mut values = String::new();
    ///         let mut params = Vec::new();
    ///         // Every column the database does not fill, bound by its position in the
    ///         // description.
    ///         for (index, column) in spec.columns().enumerate() {
    ///             if column.is_generated() {
    ///                 continue;
    ///             }
    ///             if !params.is_empty() {
    ///                 names.push_str(", ");
    ///                 values.push_str(", ");
    ///             }
    ///             self.quote_into(column.name(), &mut names);
    ///             let position = NonZeroUsize::MIN.saturating_add(params.len());
    ///             self.placeholder_into(position, &mut values);
    ///             params.push(Param::Column(index));
    ///         }
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let sql = format!("INSERT INTO {table} ({names}) VALUES ({values})");
    ///         Ok(Statement::new(sql, params))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    #[must_use]
    pub const fn is_generated(&self) -> bool {
        self.generated
    }
}

#[cfg(test)]
mod tests {
    use super::Column;

    #[test]
    fn a_column_carries_its_name_and_generation() {
        let id = Column::new("job_id").generated();
        assert_eq!(id.name(), "job_id");
        assert!(id.is_generated());
        let subject = Column::new("subject");
        assert!(!subject.is_generated());
    }
}
