//! How a subscription claims the rows of a table: the forms, and the pieces of an advisory
//! lock key.

use crate::column::Column;

/// One piece of an advisory lock key: literal text, or the value of a column of the row.
///
/// `#[inbox(advisory_lock = "jobs-{job_id}")]` becomes
/// `[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")]`.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::KeyPart;
///
/// // A dialect renders the key of one row from the row's values.
/// fn render_key(key: &[KeyPart<'_>], value_of: impl Fn(&str) -> String) -> String {
///     key.iter()
///         .map(|part| match part {
///             KeyPart::Literal(text) => (*text).to_owned(),
///             KeyPart::Column(column) => value_of(column),
///         })
///         .collect()
/// }
///
/// let key = [KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];
/// assert_eq!(render_key(&key, |_| "42".to_owned()), "jobs-42");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyPart<'a> {
    /// Text copied into the key as it is.
    Literal(&'a str),
    /// The value of the named column.
    Column(&'a str),
}

/// How a subscription claims the rows of a table; a table has exactly one form.
///
/// The lease form carries its `locked_until` column and the advisory form its key, so a table
/// cannot declare two forms. The row lock is the default: the form of a table that declares
/// neither.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")));
///
/// // A dialect picks the claim statement by the form.
/// let claim = match JOBS.form() {
///     Form::RowLock => "lock the rows for the handler".to_owned(),
///     Form::Lease(expiry) => format!("set {} and commit", expiry.name()),
///     Form::Advisory(_) => "take a session lock per row".to_owned(),
///     _ => "a form this dialect does not know".to_owned(),
/// };
/// assert_eq!(claim, "set locked_until and commit");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Form<'a> {
    /// Rows stay locked in a transaction held for the whole handler; the default.
    #[default]
    RowLock,
    /// A claim sets this column, the lease's expiry, and commits; the value holds the row.
    Lease(Column<'a>),
    /// A session lock on the key these parts build from the row holds the row.
    Advisory(&'a [KeyPart<'a>]),
}

impl Form<'_> {
    /// The form's name in messages: `row lock`, `lease` or `advisory lock`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // A startup log names the form a queue runs in.
    /// let line = format!("{} claims rows by {}", JOBS.table(), JOBS.form().name());
    /// assert_eq!(line, "jobs claims rows by row lock");
    /// ```
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::RowLock => "row lock",
            Self::Lease(_) => "lease",
            Self::Advisory(_) => "advisory lock",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, Form};

    #[test]
    fn form_names_read_in_messages() {
        assert_eq!(Form::RowLock.name(), "row lock");
        assert_eq!(Form::Lease(Column::new("locked_until")).name(), "lease");
        assert_eq!(Form::Advisory(&[]).name(), "advisory lock");
    }

    #[test]
    fn the_row_lock_is_the_default_form() {
        assert_eq!(Form::default(), Form::RowLock);
    }
}
