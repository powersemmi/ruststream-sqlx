//! The description of a queue table: its columns, the role each one plays, and how rows are
//! claimed.

use crate::column::Column;
use crate::form::Form;
use crate::opening::{Isolation, Mode, Opening};
use crate::role::Role;

/// Whether the rows of a table split into groups, and whether each group keeps its order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Grouping<'a> {
    None,
    Groups(Column<'a>),
    Fifo(Column<'a>),
}

/// The description of a queue table: its name, the column that identifies a row, one slot per
/// role, the message's data columns, the form its rows are claimed in, and what its transactions
/// open at.
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
    opening: Opening,
    select_all: bool,
    database_clock: bool,
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
            opening: Opening::Default,
            select_all: false,
            database_clock: false,
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
    /// A claim takes the group's head, its first unfinished row in claim order, and takes nothing
    /// while a row of the group is in work or the head is not yet due: the claim's transaction
    /// first takes the group ([`Dialect::fifo_guard`](crate::Dialect::fifo_guard)), and a lease
    /// claim waits while a row of the group holds a lease. A delayed retry gives its row a later
    /// `retry_after`, so the row moves behind the rows of its group due earlier.
    ///
    /// The advisory lock form keeps a group in order through its lock key instead, so its claim
    /// and its take refuse a FIFO group
    /// ([`StatementError::AdvisoryFifo`](crate::StatementError::AdvisoryFifo)).
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

    /// Makes the statements read the database's own clock instead of a time the service binds.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// // `#[inbox(clock = DatabaseClock)]` sets it: hosts whose clocks drift read one clock.
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .retry_after(Column::new("retry_after"))
    ///     .database_clock();
    /// assert!(JOBS.uses_database_clock());
    /// ```
    #[must_use]
    pub const fn database_clock(self) -> Self {
        Self {
            database_clock: true,
            ..self
        }
    }

    /// The same table, with its transactions opened at the isolation level `isolation`.
    ///
    /// The row lock claim's transaction opens at it, and so does the transaction a broker opens
    /// for a handler's writes. A table opens at one level or in one mode: the last of `isolation`
    /// and [`mode`](Self::mode) given is the table's.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Isolation, MySql, TableSpec};
    ///
    /// // `#[inbox(isolation = repeatable_read)]`: on MySQL the claims open at REPEATABLE READ
    /// // instead of READ COMMITTED.
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .isolation(Isolation::RepeatableRead);
    ///
    /// let begin = MySql.begin(JOBS.opening())?;
    /// assert_eq!(
    ///     begin,
    ///     Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION"),
    /// );
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    #[must_use]
    pub const fn isolation(self, isolation: Isolation) -> Self {
        Self {
            opening: Opening::Isolation(isolation),
            ..self
        }
    }

    /// The same table, with its SQLite transactions opened in `mode`.
    ///
    /// A table opens in one mode or at one level: the last of `mode` and
    /// [`isolation`](Self::isolation) given is the table's.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "sqlite")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Mode, Sqlite, TableSpec};
    ///
    /// // `#[inbox(mode = exclusive)]` on a SQLite table.
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
    ///         .mode(Mode::Exclusive);
    ///
    /// let begin = Sqlite.begin(JOBS.opening())?;
    /// assert_eq!(begin, Some("BEGIN EXCLUSIVE"));
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    #[must_use]
    pub const fn mode(self, mode: Mode) -> Self {
        Self {
            opening: Opening::Mode(mode),
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

    /// What the table's transactions open at: [`Opening::Default`] unless the table names an
    /// isolation level or a mode.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Isolation, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .isolation(Isolation::Serializable);
    ///
    /// // What a broker sends to open a claim's transaction.
    /// let begin = Postgres.begin(JOBS.opening())?.unwrap_or("BEGIN");
    /// assert_eq!(begin, "BEGIN ISOLATION LEVEL SERIALIZABLE");
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    #[must_use]
    pub const fn opening(&self) -> Opening {
        self.opening
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

    /// Whether the statements read the database's own clock.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // A service binds "now" itself unless the table reads the database's clock.
    /// let binds_now = !JOBS.uses_database_clock();
    /// assert!(binds_now);
    /// ```
    #[must_use]
    pub const fn uses_database_clock(&self) -> bool {
        self.database_clock
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
mod tests;
