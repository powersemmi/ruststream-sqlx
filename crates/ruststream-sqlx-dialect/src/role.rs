//! The roles a column plays for the queue, as `#[field(..)]` names them.

use std::fmt::{self, Display, Formatter};

/// What a column does for the queue, named as `#[field(..)]` spells it.
///
/// A struct marks the columns that run its queue with roles; every other column is the
/// message's own data.
///
/// # Examples
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use ruststream_sqlx_dialect::TableName;
/// use ruststream_sqlx_dialect::{Dialect, Param, Role, Statement, StatementError, TableSpec};
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
///     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         // A delayed retry writes the column that plays `retry_after`.
///         let Some(until) = spec.column(Role::RetryAfter) else {
///             return Err(StatementError::MissingRole {
///                 statement: "retry_after",
///                 role: Role::RetryAfter,
///             });
///         };
///         let mut sql = String::from("UPDATE ");
///         self.quote_into(spec.table(), &mut sql);
///         sql.push_str(" SET ");
///         self.quote_into(until.name(), &mut sql);
///         sql.push_str(" = @p1 WHERE ");
///         self.quote_into(spec.id().name(), &mut sql);
///         sql.push_str(" = @p2");
///         Ok(Statement::new(sql, [Param::RetryAfter, Param::Id]))
///     }
/// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// The row's identity, for settlement and for matching fetched rows to claimed ids.
    Id,
    /// The group a subscription reads: the rows of one table split into groups.
    Group,
    /// The key of the delivery's lane under `workers(n, by_key)` or `threads(n, by_key)`.
    PartitionKey,
    /// The claim order: a smaller value is claimed first.
    Priority,
    /// The time before which a row is not claimed.
    RetryAfter,
    /// The attempt number; the first delivery reads 1.
    Attempt,
    /// The expiry of a lease; a column playing it selects the lease form.
    LockedUntil,
    /// The time a row was finished; acknowledgement sets it instead of deleting the row.
    ProcessedAt,
    /// The delivery's headers.
    Headers,
    /// The message bytes a handler decodes.
    Payload,
}

impl Role {
    /// Every role, in the order the documentation lists them.
    pub const ALL: &'static [Self] = &[
        Self::Id,
        Self::Group,
        Self::PartitionKey,
        Self::Priority,
        Self::RetryAfter,
        Self::Attempt,
        Self::LockedUntil,
        Self::ProcessedAt,
        Self::Headers,
        Self::Payload,
    ];

    /// The role's name inside `#[field(..)]`.
    ///
    /// # Examples
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
    #[must_use]
    pub const fn attribute(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Group => "group",
            Self::PartitionKey => "partition_key",
            Self::Priority => "priority",
            Self::RetryAfter => "retry_after",
            Self::Attempt => "attempt",
            Self::LockedUntil => "locked_until",
            Self::ProcessedAt => "processed_at",
            Self::Headers => "headers",
            Self::Payload => "payload",
        }
    }

    /// The role `#[field(..)]` names with `name`, or `None` when no role has that name.
    #[must_use]
    pub fn from_attribute(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|role| role.attribute() == name)
    }
}

impl Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.attribute())
    }
}

#[cfg(test)]
mod tests {
    use super::Role;

    #[test]
    fn every_role_reads_back_from_its_attribute() {
        for role in Role::ALL {
            assert_eq!(Role::from_attribute(role.attribute()), Some(*role));
            assert_eq!(role.to_string(), role.attribute());
        }
        assert_eq!(Role::ALL.len(), 10);
        assert_eq!(Role::from_attribute("deadline"), None);
    }
}
