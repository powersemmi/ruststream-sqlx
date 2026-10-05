//! One column of a queue table.

/// One column of a queue table: its name in the database, and whether the database fills it in.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// // `#[field(id, generated)] job_id: i64` and a column of the message's data.
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id").generated(), Form::RowLock)
///         .data(&[Column::new("subject")]);
///
/// // An insert writes every column the database does not fill in.
/// let written: Vec<&str> = JOBS
///     .columns()
///     .filter(|column| !column.is_generated())
///     .map(|column| column.name())
///     .collect();
/// assert_eq!(written, ["subject"]);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Column<'a> {
    name: &'a str,
    generated: bool,
}

impl<'a> Column<'a> {
    /// A column the service writes.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const EMAILS: TableSpec<'static> = TableSpec::new("emails", Column::new("job_id"), Form::RowLock)
    ///     .data(&[Column::new("subject"), Column::new("body")]);
    ///
    /// let selected: Vec<&str> = EMAILS.columns().map(|column| column.name()).collect();
    /// assert_eq!(selected, ["job_id", "subject", "body"]);
    /// ```
    #[must_use]
    pub const fn new(name: &'a str) -> Self {
        Self {
            name,
            generated: false,
        }
    }

    /// The same column, filled in by the database, so an insert leaves it out.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// // The id comes from a sequence.
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id").generated(), Form::RowLock)
    ///         .payload(Column::new("payload"));
    ///
    /// let written = JOBS.columns().filter(|column| !column.is_generated()).count();
    /// assert_eq!(written, 1);
    /// ```
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
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .payload(Column::new("payload"));
    ///
    /// // A select names every column of the table.
    /// let selected: Vec<&str> = JOBS.columns().map(|column| column.name()).collect();
    /// assert_eq!(selected.join(", "), "job_id, payload");
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
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id").generated(), Form::RowLock)
    ///         .payload(Column::new("payload"));
    ///
    /// // What an insert returns: the values the database chose.
    /// let returned: Vec<&str> = JOBS
    ///     .columns()
    ///     .filter(|column| column.is_generated())
    ///     .map(|column| column.name())
    ///     .collect();
    /// assert_eq!(returned, ["job_id"]);
    /// ```
    #[must_use]
    pub const fn is_generated(&self) -> bool {
        self.generated
    }
}
