//! The description of a queue table: its columns, the role each one plays, and how rows are
//! claimed.

use crate::column::Column;
use crate::form::Form;
use crate::role::Role;

/// Whether the rows of a table split into groups, and whether each group keeps its order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Grouping<'a> {
    None,
    Groups(Column<'a>),
    Fifo(Column<'a>),
}

/// The description of a queue table: its name, the column that identifies a row, one slot per
/// role, the message's data columns, and the form its rows are claimed in.
///
/// `#[derive(Inbox)]` builds one as a constant, and a dialect reads it to build statements. The id
/// column is part of the constructor and every role has one slot, so a table without an id or
/// with a role played twice cannot be described. Column names are strings, so the types do not
/// catch a name used twice or a lock key reading a column the table lacks: `#[derive(Inbox)]`
/// refuses both at compile time, and a description written by hand meets the database's own
/// checks when the statements are prepared.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
///
/// const EMAILS: TableSpec<'static> =
///     TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
///         .within("app")
///         .group(Column::new("name"))
///         .payload(Column::new("payload"));
///
/// // A subscription to `emails` reads the group column for its name.
/// let group = EMAILS.column(Role::Group).map(|column| column.name());
/// assert_eq!(group, Some("name"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableSpec<'a> {
    schema: Option<&'a str>,
    table: &'a str,
    id: Column<'a>,
    grouping: Grouping<'a>,
    partition_key: Option<Column<'a>>,
    priority: Option<Column<'a>>,
    retry_after: Option<Column<'a>>,
    attempt: Option<Column<'a>>,
    processed_at: Option<Column<'a>>,
    headers: Option<Column<'a>>,
    payload: Option<Column<'a>>,
    data: &'a [Column<'a>],
    form: Form<'a>,
    select_all: bool,
}

impl<'a> TableSpec<'a> {
    /// A table in the connection's default schema, with the column that identifies a row and the
    /// form its rows are claimed in.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// // The description `#[derive(Inbox)]` emits for a struct with an id and a payload.
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .payload(Column::new("payload"));
    ///
    /// // Every statement settles a row by its id.
    /// let settles_by = JOBS.id().name();
    /// assert_eq!(settles_by, "job_id");
    /// assert_eq!(JOBS.column(Role::Id), Some(JOBS.id()));
    /// ```
    #[must_use]
    pub const fn new(table: &'a str, id: Column<'a>, form: Form<'a>) -> Self {
        Self {
            schema: None,
            table,
            id,
            grouping: Grouping::None,
            partition_key: None,
            priority: None,
            retry_after: None,
            attempt: None,
            processed_at: None,
            headers: None,
            payload: None,
            data: &[],
            form,
            select_all: false,
        }
    }

    /// The same table, inside `schema`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).within("app");
    ///
    /// // A dialect qualifies the table with its schema.
    /// let qualified = format!("{}.{}", JOBS.schema().unwrap_or("public"), JOBS.table());
    /// assert_eq!(qualified, "app.jobs");
    /// ```
    #[must_use]
    pub const fn within(self, schema: &'a str) -> Self {
        Self {
            schema: Some(schema),
            ..self
        }
    }

    /// The same table, split into groups by `column`; a subscription reads one group.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// // `#[field(group)] name: String`.
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).group(Column::new("name"));
    ///
    /// let group = JOBS.column(Role::Group).map(|column| column.name());
    /// assert_eq!(group, Some("name"));
    /// assert!(!JOBS.is_fifo());
    /// ```
    #[must_use]
    pub const fn group(self, column: Column<'a>) -> Self {
        Self {
            grouping: Grouping::Groups(column),
            ..self
        }
    }

    /// The same table, split into groups by `column`, with each group in order: at most one row of
    /// a group is in work, taken in claim order.
    ///
    /// The advisory lock form keeps a group in order through its lock key instead, and a dialect
    /// refuses a FIFO group in that form when it builds the statements.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// // `#[field(group, fifo = true)] account: String`.
    /// const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
    ///     .fifo_group(Column::new("account"));
    ///
    /// // A FIFO group gives no parallelism inside the group.
    /// let batch = if LEDGER.is_fifo() { 1 } else { 100 };
    /// assert_eq!(batch, 1);
    /// assert_eq!(LEDGER.column(Role::Group).map(|column| column.name()), Some("account"));
    /// ```
    #[must_use]
    pub const fn fifo_group(self, column: Column<'a>) -> Self {
        Self {
            grouping: Grouping::Fifo(column),
            ..self
        }
    }

    /// The same table, with the delivery's partition key read from `column`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const ORDERS: TableSpec<'static> = TableSpec::new("orders", Column::new("id"), Form::RowLock)
    ///     .partition_key(Column::new("customer_id"));
    ///
    /// // `workers(n, by_key)` keys its lanes by this column.
    /// let key = ORDERS.column(Role::PartitionKey).map(|column| column.name());
    /// assert_eq!(key, Some("customer_id"));
    /// ```
    #[must_use]
    pub const fn partition_key(self, column: Column<'a>) -> Self {
        Self {
            partition_key: Some(column),
            ..self
        }
    }

    /// The same table, with rows claimed in the order of `column`, a smaller value first.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("id"), Form::RowLock).priority(Column::new("rank"));
    ///
    /// let order = JOBS.column(Role::Priority).map(|column| column.name());
    /// assert_eq!(order, Some("rank"));
    /// ```
    #[must_use]
    pub const fn priority(self, column: Column<'a>) -> Self {
        Self {
            priority: Some(column),
            ..self
        }
    }

    /// The same table, where `column` holds the time before which a row is not claimed.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("id"), Form::RowLock).retry_after(Column::new("run_at"));
    ///
    /// // `retry_after(d)` is native for this table.
    /// let native = JOBS.column(Role::RetryAfter).is_some();
    /// assert!(native);
    /// ```
    #[must_use]
    pub const fn retry_after(self, column: Column<'a>) -> Self {
        Self {
            retry_after: Some(column),
            ..self
        }
    }

    /// The same table, where `column` counts the attempts.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("id"), Form::RowLock).attempt(Column::new("attempt"));
    ///
    /// // `max_attempts(n)` needs the count.
    /// let can_limit = JOBS.column(Role::Attempt).is_some();
    /// assert!(can_limit);
    /// ```
    #[must_use]
    pub const fn attempt(self, column: Column<'a>) -> Self {
        Self {
            attempt: Some(column),
            ..self
        }
    }

    /// The same table, where acknowledgement sets `column` instead of deleting the row.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("id"), Form::RowLock)
    ///     .processed_at(Column::new("done_at"));
    ///
    /// let ack = match JOBS.column(Role::ProcessedAt) {
    ///     Some(column) => format!("set {}", column.name()),
    ///     None => "delete the row".to_owned(),
    /// };
    /// assert_eq!(ack, "set done_at");
    /// ```
    #[must_use]
    pub const fn processed_at(self, column: Column<'a>) -> Self {
        Self {
            processed_at: Some(column),
            ..self
        }
    }

    /// The same table, where `column` holds the delivery's headers.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("id"), Form::RowLock).headers(Column::new("meta"));
    ///
    /// let headers = JOBS.column(Role::Headers).map(|column| column.name());
    /// assert_eq!(headers, Some("meta"));
    /// ```
    #[must_use]
    pub const fn headers(self, column: Column<'a>) -> Self {
        Self {
            headers: Some(column),
            ..self
        }
    }

    /// The same table, where `column` holds the message bytes a handler decodes.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("id"), Form::RowLock).payload(Column::new("body"));
    ///
    /// // A table with a payload column delivers bytes; without one, the row itself.
    /// let mode = if JOBS.column(Role::Payload).is_some() { "payload" } else { "row" };
    /// assert_eq!(mode, "payload");
    /// ```
    #[must_use]
    pub const fn payload(self, column: Column<'a>) -> Self {
        Self {
            payload: Some(column),
            ..self
        }
    }

    /// The same table, with the columns of the message's own data: the columns without a role.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const EMAILS: TableSpec<'static> = TableSpec::new("emails", Column::new("id"), Form::RowLock)
    ///     .data(&[Column::new("subject"), Column::new("created_at").generated()]);
    ///
    /// let selected: Vec<&str> = EMAILS.columns().map(|column| column.name()).collect();
    /// assert_eq!(selected, ["id", "subject", "created_at"]);
    /// ```
    #[must_use]
    pub const fn data(self, columns: &'a [Column<'a>]) -> Self {
        Self {
            data: columns,
            ..self
        }
    }

    /// The same table, read with `*`: the struct flattens another, so the columns are not all
    /// known.
    ///
    /// A dead-letter move to another table then copies the row by position, so that table has the
    /// same columns in the same order.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// // A struct with a `#[sqlx(flatten)]` field: its own columns, and more the database knows.
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).selecting_all();
    ///
    /// let selected = if JOBS.selects_all() {
    ///     "*".to_owned()
    /// } else {
    ///     JOBS.columns().map(|column| column.name()).collect::<Vec<_>>().join(", ")
    /// };
    /// assert_eq!(selected, "*");
    /// ```
    #[must_use]
    pub const fn selecting_all(self) -> Self {
        Self {
            select_all: true,
            ..self
        }
    }

    /// The table's name, without its schema.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // Every error of a queue names its table.
    /// let context = format!("claiming from `{}`", JOBS.table());
    /// assert_eq!(context, "claiming from `jobs`");
    /// ```
    #[must_use]
    pub const fn table(&self) -> &'a str {
        self.table
    }

    /// The schema the table lives in, or `None` for the connection's default.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // A dialect for a database without schemas refuses a table inside one.
    /// fn check(spec: &TableSpec<'_>) -> Result<(), String> {
    ///     match spec.schema() {
    ///         Some(schema) => Err(format!("`{}` lives in schema `{schema}`", spec.table())),
    ///         None => Ok(()),
    ///     }
    /// }
    ///
    /// check(&JOBS)?;
    /// # Ok::<(), String>(())
    /// ```
    #[must_use]
    pub const fn schema(&self) -> Option<&'a str> {
        self.schema
    }

    /// The column that identifies a row.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // An acknowledgement deletes the row it names by id.
    /// let ack = format!("DELETE FROM {} WHERE {} = $1", JOBS.table(), JOBS.id().name());
    /// assert_eq!(ack, "DELETE FROM jobs WHERE job_id = $1");
    /// ```
    #[must_use]
    pub const fn id(&self) -> Column<'a> {
        self.id
    }

    /// The form the table's rows are claimed in.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, KeyPart, TableSpec};
    ///
    /// const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY));
    ///
    /// // An advisory dialect locks the key the form carries.
    /// let parts = match JOBS.form() {
    ///     Form::Advisory(key) => key.len(),
    ///     _ => 0,
    /// };
    /// assert_eq!(parts, 2);
    /// ```
    #[must_use]
    pub const fn form(&self) -> Form<'a> {
        self.form
    }

    /// Whether each group of the table keeps its order: at most one row of a group is in work.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const LEDGER: TableSpec<'static> = TableSpec::new("ledger", Column::new("id"), Form::RowLock)
    ///     .fifo_group(Column::new("account"));
    ///
    /// // A claim of a FIFO group takes the group's head, or nothing.
    /// let limit = if LEDGER.is_fifo() { 1 } else { 100 };
    /// assert_eq!(limit, 1);
    /// ```
    #[must_use]
    pub const fn is_fifo(&self) -> bool {
        matches!(self.grouping, Grouping::Fifo(_))
    }

    /// Whether statements select `*` instead of listing the columns.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // A table whose struct flattens nothing lists its columns.
    /// let selected = if JOBS.selects_all() {
    ///     "*".to_owned()
    /// } else {
    ///     JOBS.columns().map(|column| column.name()).collect::<Vec<_>>().join(", ")
    /// };
    /// assert_eq!(selected, "job_id");
    /// ```
    #[must_use]
    pub const fn selects_all(&self) -> bool {
        self.select_all
    }

    /// The column that plays `role`, or `None` when the table has none.
    ///
    /// The `locked_until` column is the one the lease form carries.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, Role, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .processed_at(Column::new("processed_at"));
    ///
    /// // Acknowledgement marks the row when the table keeps finished rows.
    /// let ack = match JOBS.column(Role::ProcessedAt) {
    ///     Some(column) => format!("set {}", column.name()),
    ///     None => "delete the row".to_owned(),
    /// };
    /// assert_eq!(ack, "set processed_at");
    /// ```
    #[must_use]
    pub const fn column(&self, role: Role) -> Option<Column<'a>> {
        match role {
            Role::Id => Some(self.id),
            Role::Group => match self.grouping {
                Grouping::None => None,
                Grouping::Groups(column) | Grouping::Fifo(column) => Some(column),
            },
            Role::PartitionKey => self.partition_key,
            Role::Priority => self.priority,
            Role::RetryAfter => self.retry_after,
            Role::Attempt => self.attempt,
            Role::LockedUntil => match self.form {
                Form::Lease(column) => Some(column),
                Form::RowLock | Form::Advisory(_) => None,
            },
            Role::ProcessedAt => self.processed_at,
            Role::Headers => self.headers,
            Role::Payload => self.payload,
        }
    }

    /// Every column: the one of each role in [`Role::ALL`] order, then the message's data.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .payload(Column::new("payload"))
    ///     .data(&[Column::new("subject")]);
    ///
    /// let selected: Vec<&str> = JOBS.columns().map(|column| column.name()).collect();
    /// assert_eq!(selected, ["job_id", "payload", "subject"]);
    /// ```
    pub fn columns(&self) -> impl Iterator<Item = Column<'a>> {
        Role::ALL
            .iter()
            .filter_map(|role| self.column(*role))
            .chain(self.data.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::{Column, Form, Role, TableSpec};
    use crate::form::KeyPart;

    const EMAILS: TableSpec<'static> = TableSpec::new(
        "email_jobs",
        Column::new("job_id").generated(),
        Form::RowLock,
    )
    .within("app")
    .group(Column::new("name"))
    .partition_key(Column::new("customer"))
    .priority(Column::new("priority"))
    .retry_after(Column::new("retry_after"))
    .attempt(Column::new("attempt"))
    .processed_at(Column::new("processed_at"))
    .headers(Column::new("headers"))
    .payload(Column::new("payload"))
    .data(&[Column::new("subject")]);

    const ID_KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("id")];

    fn names(spec: &TableSpec<'_>) -> Vec<String> {
        spec.columns()
            .map(|column| column.name().to_owned())
            .collect()
    }

    #[test]
    fn a_column_carries_its_name_and_generation() {
        let id = EMAILS.id();
        assert_eq!(id.name(), "job_id");
        assert!(id.is_generated());
        let subject = Column::new("subject");
        assert!(!subject.is_generated());
    }

    #[test]
    fn every_role_has_its_slot() {
        let expected = [
            (Role::Id, Some("job_id")),
            (Role::Group, Some("name")),
            (Role::PartitionKey, Some("customer")),
            (Role::Priority, Some("priority")),
            (Role::RetryAfter, Some("retry_after")),
            (Role::Attempt, Some("attempt")),
            (Role::LockedUntil, None),
            (Role::ProcessedAt, Some("processed_at")),
            (Role::Headers, Some("headers")),
            (Role::Payload, Some("payload")),
        ];
        for (role, column) in expected {
            assert_eq!(EMAILS.column(role).map(|column| column.name()), column);
        }
    }

    #[test]
    fn columns_list_the_roles_in_order_then_the_data() {
        assert_eq!(
            names(&EMAILS),
            [
                "job_id",
                "name",
                "customer",
                "priority",
                "retry_after",
                "attempt",
                "processed_at",
                "headers",
                "payload",
                "subject",
            ]
        );
    }

    #[test]
    fn a_spec_carries_what_its_builders_set() {
        assert_eq!(EMAILS.table(), "email_jobs");
        assert_eq!(EMAILS.schema(), Some("app"));
        assert_eq!(EMAILS.form(), Form::RowLock);
        assert!(!EMAILS.is_fifo());
        assert!(!EMAILS.selects_all());
        assert!(EMAILS.selecting_all().selects_all());
    }

    #[test]
    fn a_slot_holds_one_column() {
        let regrouped = EMAILS.group(Column::new("queue"));
        assert_eq!(
            regrouped.column(Role::Group).map(|column| column.name()),
            Some("queue")
        );
        assert_eq!(names(&regrouped).len(), names(&EMAILS).len());
    }

    #[test]
    fn a_fifo_group_is_a_group_that_keeps_its_order() {
        let fifo = EMAILS.fifo_group(Column::new("account"));
        assert!(fifo.is_fifo());
        assert_eq!(
            fifo.column(Role::Group).map(|column| column.name()),
            Some("account")
        );
        assert!(!fifo.group(Column::new("account")).is_fifo());
    }

    #[test]
    fn the_lease_form_carries_the_locked_until_column() {
        let leased = TableSpec::new(
            "jobs",
            Column::new("id"),
            Form::Lease(Column::new("locked_until")),
        );
        assert_eq!(
            leased.column(Role::LockedUntil).map(|column| column.name()),
            Some("locked_until")
        );
        assert_eq!(names(&leased), ["id", "locked_until"]);
        let advisory = TableSpec::new("jobs", Column::new("id"), Form::Advisory(ID_KEY));
        assert_eq!(advisory.column(Role::LockedUntil), None);
    }
}
