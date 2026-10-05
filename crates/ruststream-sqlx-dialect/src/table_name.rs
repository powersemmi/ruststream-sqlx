//! A table named the way a service writes it: `table`, or `schema.table`.

use thiserror::Error;

/// A table outside the queue's own description, such as the table dead-lettered rows move to.
///
/// It is read from `table` or `schema.table`, and checked when it is read: no segment is empty,
/// and there is at most one schema.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{ParseTableNameError, TableName};
///
/// let target = TableName::parse("archive.jobs_dead")?;
/// let qualified = format!("{}.{}", target.schema().unwrap_or("public"), target.table());
/// assert_eq!(qualified, "archive.jobs_dead");
/// # Ok::<(), ParseTableNameError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableName<'a> {
    schema: Option<&'a str>,
    table: &'a str,
}

impl<'a> TableName<'a> {
    /// Reads `table` or `schema.table`.
    ///
    /// # Errors
    ///
    /// [`ParseTableNameError::EmptySegment`] when the name or one of its segments is empty;
    /// [`ParseTableNameError::TooManySegments`] when the name has more than one dot.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{ParseTableNameError, TableName};
    ///
    /// // The table of the connection's default schema, and one inside a schema.
    /// assert_eq!(TableName::parse("jobs_dead")?.schema(), None);
    /// assert_eq!(TableName::parse("archive.jobs_dead")?.schema(), Some("archive"));
    ///
    /// // A name with an empty segment is refused when it is read.
    /// assert!(matches!(
    ///     TableName::parse("archive."),
    ///     Err(ParseTableNameError::EmptySegment { .. }),
    /// ));
    /// # Ok::<(), ParseTableNameError>(())
    /// ```
    pub fn parse(name: &'a str) -> Result<Self, ParseTableNameError> {
        let parsed = match name.split_once('.') {
            None => Self {
                schema: None,
                table: name,
            },
            Some((_, table)) if table.contains('.') => {
                return Err(ParseTableNameError::TooManySegments {
                    name: name.to_owned(),
                });
            }
            Some((schema, table)) => Self {
                schema: Some(schema),
                table,
            },
        };
        if parsed.table.is_empty() || parsed.schema.is_some_and(str::is_empty) {
            return Err(ParseTableNameError::EmptySegment {
                name: name.to_owned(),
            });
        }
        Ok(parsed)
    }

    /// The schema, or `None` for the connection's default.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{ParseTableNameError, TableName};
    ///
    /// // A dialect for a database without schemas refuses a table inside one.
    /// let target = TableName::parse("jobs_dead")?;
    /// let supported = target.schema().is_none();
    /// assert!(supported);
    /// # Ok::<(), ParseTableNameError>(())
    /// ```
    #[must_use]
    pub const fn schema(&self) -> Option<&'a str> {
        self.schema
    }

    /// The table's name, without its schema.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{ParseTableNameError, TableName};
    ///
    /// let target = TableName::parse("archive.jobs_dead")?;
    /// let context = format!("moving the row to `{}`", target.table());
    /// assert_eq!(context, "moving the row to `jobs_dead`");
    /// # Ok::<(), ParseTableNameError>(())
    /// ```
    #[must_use]
    pub const fn table(&self) -> &'a str {
        self.table
    }
}

/// Why a string does not name a table.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::TableName;
///
/// let message = TableName::parse("db.archive.jobs_dead").map_err(|error| error.to_string());
/// assert_eq!(
///     message,
///     Err(
///         "table name `db.archive.jobs_dead` has more than two segments: write `table` or \
///          `schema.table`"
///             .to_owned()
///     ),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ParseTableNameError {
    /// The name, or one of its segments, is empty.
    #[error("table name `{name}` has an empty segment: write `table` or `schema.table`")]
    EmptySegment {
        /// The name as given.
        name: String,
    },
    /// The name has more than one dot.
    #[error("table name `{name}` has more than two segments: write `table` or `schema.table`")]
    TooManySegments {
        /// The name as given.
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{ParseTableNameError, TableName};

    #[test]
    fn a_name_reads_with_or_without_a_schema() -> Result<(), ParseTableNameError> {
        let bare = TableName::parse("jobs_dead")?;
        assert_eq!((bare.schema(), bare.table()), (None, "jobs_dead"));
        let qualified = TableName::parse("Archive.Jobs Dead")?;
        assert_eq!(
            (qualified.schema(), qualified.table()),
            (Some("Archive"), "Jobs Dead")
        );
        Ok(())
    }

    #[test]
    fn an_empty_segment_is_refused() {
        for name in ["", ".jobs_dead", "archive.", "."] {
            assert_eq!(
                TableName::parse(name),
                Err(ParseTableNameError::EmptySegment {
                    name: name.to_owned()
                })
            );
        }
    }

    #[test]
    fn more_than_one_schema_is_refused() {
        for name in ["db.archive.jobs_dead", "archive..jobs_dead"] {
            assert_eq!(
                TableName::parse(name),
                Err(ParseTableNameError::TooManySegments {
                    name: name.to_owned()
                })
            );
        }
    }
}
