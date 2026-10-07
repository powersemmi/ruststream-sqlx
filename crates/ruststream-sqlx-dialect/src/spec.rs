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
/// column is part of the constructor and every role has one slot, which its setter fills once (a
/// second call panics), so a table without an id or with a role played twice cannot be described.
/// Column names are strings, so the types do not catch a name used twice or a lock key reading a
/// column the table lacks: `#[derive(Inbox)]` refuses both at compile time, and a description
/// written by hand meets the database's own checks when the statements are prepared.
///
/// # Examples
///
/// A dialect of the service's own reads the description to build a statement:
///
/// ```
/// # use std::num::NonZeroUsize;
/// use ruststream_sqlx_dialect::{
///     Dialect, Form, Param, Role, Statement, StatementError, TableSpec,
/// };
/// # use ruststream_sqlx_dialect::TableName;
///
/// /// SQL Server, a database without a built-in dialect, keeping the finished rows of a table with
/// /// groups in its `done` group.
/// #[derive(Debug)]
/// pub struct Mssql;
///
/// impl Dialect for Mssql {
///     fn name(&self) -> &'static str {
///         "mssql"
///     }
///
///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
///         // The service's tables take their rows by row lock.
///         if spec.form() != Form::RowLock {
///             return Err(StatementError::UnsupportedForm {
///                 dialect: self.name(),
///                 form: spec.form().name(),
///             });
///         }
///         // A table that names no schema lives in SQL Server's default one, `dbo`.
///         let mut table = String::new();
///         self.quote_into(spec.schema().unwrap_or("dbo"), &mut table);
///         table.push('.');
///         self.quote_into(spec.table(), &mut table);
///         let mut id = String::new();
///         self.quote_into(spec.id().name(), &mut id);
///         let Some(group) = spec.column(Role::Group) else {
///             return Ok(Statement::new(
///                 format!("DELETE FROM {table} WHERE {id} = @p1"),
///                 [Param::Id],
///             ));
///         };
///         let mut done = String::new();
///         self.quote_into(group.name(), &mut done);
///         Ok(Statement::new(
///             format!("UPDATE {table} SET {done} = 'done' WHERE {id} = @p1"),
///             [Param::Id],
///         ))
///     }
///
///     fn quote_into(&self, ident: &str, out: &mut String) {
///         out.push('[');
///         out.push_str(&ident.replace(']', "]]"));
///         out.push(']');
///     }
/// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
/// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
/// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
/// }
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
    fetched: &'a [Column<'a>],
    form: Form<'a>,
    opening: Opening,
    select_all: bool,
    database_clock: bool,
}

impl<'a> TableSpec<'a> {
    /// A table in the connection's default schema, with the column that identifies a row and the
    /// form its rows are claimed in.
    ///
    /// `#[derive(Inbox)]` builds the description from `#[inbox(table = "..")]`, the `#[field(id)]`
    /// field and the form the struct declares.
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
            fetched: &[],
            form,
            opening: Opening::Default,
            select_all: false,
            database_clock: false,
        }
    }

    /// The same table, inside `schema`.
    ///
    /// `#[inbox(schema = "app")]` sets it.
    ///
    /// # Panics
    ///
    /// When the description already names a schema. In a `const` the panic is a build error.
    #[must_use]
    pub const fn within(self, schema: &'a str) -> Self {
        assert!(
            self.schema.is_none(),
            "a table description is given the schema twice: name it once"
        );
        Self {
            schema: Some(schema),
            ..self
        }
    }

    /// The same table, split into groups by `column`; a subscription reads one group.
    ///
    /// `#[field(group)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already splits into groups, by this setter or by
    /// [`fifo_group`](Self::fifo_group). In a `const` the panic is a build error.
    #[must_use]
    pub const fn group(self, column: Column<'a>) -> Self {
        assert!(
            matches!(self.grouping, Grouping::None),
            "a table description sets its `group` column twice (`group` or `fifo_group`): a role \
             belongs to one column"
        );
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
    /// `#[field(group, fifo = true)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already splits into groups, by this setter or by
    /// [`group`](Self::group). In a `const` the panic is a build error.
    #[must_use]
    pub const fn fifo_group(self, column: Column<'a>) -> Self {
        assert!(
            matches!(self.grouping, Grouping::None),
            "a table description sets its `group` column twice (`group` or `fifo_group`): a role \
             belongs to one column"
        );
        Self {
            grouping: Grouping::Fifo(column),
            ..self
        }
    }

    /// The same table, with the delivery's partition key read from `column`.
    ///
    /// `#[field(partition_key)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `partition_key` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    #[must_use]
    pub const fn partition_key(self, column: Column<'a>) -> Self {
        assert!(
            self.partition_key.is_none(),
            "a table description sets its `partition_key` column twice: a role belongs to one column"
        );
        Self {
            partition_key: Some(column),
            ..self
        }
    }

    /// The same table, with rows claimed in the order of `column`, a smaller value first.
    ///
    /// `#[field(priority)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `priority` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    #[must_use]
    pub const fn priority(self, column: Column<'a>) -> Self {
        assert!(
            self.priority.is_none(),
            "a table description sets its `priority` column twice: a role belongs to one column"
        );
        Self {
            priority: Some(column),
            ..self
        }
    }

    /// The same table, where `column` holds the time before which a row is not claimed.
    ///
    /// `#[field(retry_after)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `retry_after` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    #[must_use]
    pub const fn retry_after(self, column: Column<'a>) -> Self {
        assert!(
            self.retry_after.is_none(),
            "a table description sets its `retry_after` column twice: a role belongs to one column"
        );
        Self {
            retry_after: Some(column),
            ..self
        }
    }

    /// The same table, where `column` counts the attempts.
    ///
    /// `#[field(attempt)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `attempt` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    #[must_use]
    pub const fn attempt(self, column: Column<'a>) -> Self {
        assert!(
            self.attempt.is_none(),
            "a table description sets its `attempt` column twice: a role belongs to one column"
        );
        Self {
            attempt: Some(column),
            ..self
        }
    }

    /// The same table, where acknowledgement sets `column` instead of deleting the row.
    ///
    /// `#[field(processed_at)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `processed_at` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    #[must_use]
    pub const fn processed_at(self, column: Column<'a>) -> Self {
        assert!(
            self.processed_at.is_none(),
            "a table description sets its `processed_at` column twice: a role belongs to one column"
        );
        Self {
            processed_at: Some(column),
            ..self
        }
    }

    /// The same table, where `column` holds the delivery's headers.
    ///
    /// `#[field(headers)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `headers` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    #[must_use]
    pub const fn headers(self, column: Column<'a>) -> Self {
        assert!(
            self.headers.is_none(),
            "a table description sets its `headers` column twice: a role belongs to one column"
        );
        Self {
            headers: Some(column),
            ..self
        }
    }

    /// The same table, where `column` holds the message bytes a handler decodes.
    ///
    /// `#[field(payload)]` on a field sets it.
    ///
    /// # Panics
    ///
    /// When the description already has a `payload` column: a role belongs to one column. In a
    /// `const` the panic is a build error.
    ///
    /// ```compile_fail,E0080
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// // Two columns for one role do not build.
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("id"), Form::RowLock)
    ///     .payload(Column::new("payload"))
    ///     .payload(Column::new("body"));
    /// # fn main() { let _ = JOBS; }
    /// ```
    #[must_use]
    pub const fn payload(self, column: Column<'a>) -> Self {
        assert!(
            self.payload.is_none(),
            "a table description sets its `payload` column twice: a role belongs to one column"
        );
        Self {
            payload: Some(column),
            ..self
        }
    }

    /// The same table, with the columns of the message's own data: the columns without a role.
    ///
    /// `#[derive(Inbox)]` passes every field without a role here.
    ///
    /// # Panics
    ///
    /// When the description already lists data columns: the list is given once. In a `const` the
    /// panic is a build error.
    #[must_use]
    pub const fn data(self, columns: &'a [Column<'a>]) -> Self {
        assert!(
            self.data.is_empty(),
            "a table description lists its data columns twice: give the list once"
        );
        Self {
            data: columns,
            ..self
        }
    }

    /// The same table, with the columns a message assembled from it reads beside the headers
    /// struct's: listed after the data, wherever the statements list the table's columns.
    ///
    /// `#[derive(Inbox)]` passes the message struct's own fields here when the struct flattens a
    /// headers struct into its `#[field(headers)]` field, which describes the rest of the table.
    /// The default fetch then names every column it reads, so a column the table lacks stops the
    /// subscription when its statements are prepared.
    ///
    /// # Panics
    ///
    /// When the description already lists fetched columns: the list is given once. In a `const`
    /// the panic is a build error.
    #[must_use]
    pub const fn fetching(self, columns: &'a [Column<'a>]) -> Self {
        assert!(
            self.fetched.is_empty(),
            "a table description lists its fetched columns twice: give the list once"
        );
        Self {
            fetched: columns,
            ..self
        }
    }

    /// The same table, read with `*`: the struct flattens another, so the columns are not all
    /// known.
    ///
    /// A dead-letter move to another table then copies the row by position, so that table has the
    /// same columns in the same order.
    ///
    /// A struct with a `#[sqlx(flatten)]` field sets it.
    #[must_use]
    pub const fn selecting_all(self) -> Self {
        Self {
            select_all: true,
            ..self
        }
    }

    /// Makes the statements read the database's own clock instead of a time the service binds.
    ///
    /// `#[inbox(clock = DatabaseClock)]` sets it.
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
    /// for a handler's writes. A table opens at one level or in one mode.
    ///
    /// `#[inbox(isolation = repeatable_read)]` sets it.
    ///
    /// # Panics
    ///
    /// When the description already opens at a level or in a mode, by this setter or by
    /// [`mode`](Self::mode). In a `const` the panic is a build error.
    #[must_use]
    pub const fn isolation(self, isolation: Isolation) -> Self {
        assert!(
            matches!(self.opening, Opening::Default),
            "a table description sets its opening twice (`isolation` or `mode`): a table opens at \
             one level or in one mode"
        );
        Self {
            opening: Opening::Isolation(isolation),
            ..self
        }
    }

    /// The same table, with its SQLite transactions opened in `mode`.
    ///
    /// A table opens in one mode or at one level.
    ///
    /// `#[inbox(mode = immediate)]` sets it.
    ///
    /// # Panics
    ///
    /// When the description already opens at a level or in a mode, by this setter or by
    /// [`isolation`](Self::isolation). In a `const` the panic is a build error.
    #[must_use]
    pub const fn mode(self, mode: Mode) -> Self {
        assert!(
            matches!(self.opening, Opening::Default),
            "a table description sets its opening twice (`isolation` or `mode`): a table opens at \
             one level or in one mode"
        );
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
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{Dialect, Param, Statement, StatementError, TableSpec};
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     // The service keeps its finished emails for an audit, in the `sent` group; every other
    ///     // table deletes its finished rows.
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         if spec.table() == "email_jobs" {
    ///             return Ok(Statement::new(
    ///                 "UPDATE [email_jobs] SET [name] = 'sent' WHERE [job_id] = @p1",
    ///                 [Param::Id],
    ///             ));
    ///         }
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         Ok(Statement::new(sql, [Param::Id]))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
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
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         // A table that names no schema lives in SQL Server's default one, `dbo`.
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.schema().unwrap_or("dbo"), &mut sql);
    ///         sql.push('.');
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         Ok(Statement::new(sql, [Param::Id]))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
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
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         // Every settlement names its row by the id column.
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         Ok(Statement::new(sql, [Param::Id]))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
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
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
    /// use ruststream_sqlx_dialect::{Dialect, Form, Param, Statement, StatementError, TableSpec};
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
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let mut sql = String::from("DELETE FROM ");
    ///         self.quote_into(spec.table(), &mut sql);
    ///         sql.push_str(" WHERE ");
    ///         self.quote_into(spec.id().name(), &mut sql);
    ///         sql.push_str(" = @p1");
    ///         match spec.form() {
    ///             // In the lease form the row goes only while it holds the delivery's lease.
    ///             Form::Lease(expiry) => {
    ///                 sql.push_str(" AND ");
    ///                 self.quote_into(expiry.name(), &mut sql);
    ///                 sql.push_str(" = @p2");
    ///                 Ok(Statement::new(sql, [Param::Id, Param::Held]))
    ///             }
    ///             _ => Ok(Statement::new(sql, [Param::Id])),
    ///         }
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
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
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Dialect, Isolation, Opening, Param, RowLock, Statement, StatementError,
    ///     TableSpec,
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
    ///         // READPAST, which skips the rows another claim holds, refuses SERIALIZABLE.
    ///         let opening = spec.opening();
    ///         if opening == Opening::Isolation(Isolation::Serializable) {
    ///             return Err(StatementError::UnsupportedOpening {
    ///                 dialect: self.name(),
    ///                 opening: opening.name(),
    ///             });
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
    pub const fn opening(&self) -> Opening {
        self.opening
    }

    /// Whether each group of the table keeps its order: at most one row of a group is in work.
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
    pub const fn is_fifo(&self) -> bool {
        matches!(self.grouping, Grouping::Fifo(_))
    }

    /// Whether statements select `*` instead of listing the columns.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::num::NonZeroUsize;
    /// use ruststream_sqlx_dialect::{
    ///     Dialect, Param, Statement, StatementError, TableName, TableSpec,
    /// };
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
    ///     fn dead_letter_table(
    ///         &self,
    ///         spec: &TableSpec<'_>,
    ///         target: TableName<'_>,
    ///     ) -> Result<Vec<Statement>, StatementError> {
    ///         // A struct that flattens another hides columns, so its row moves by position.
    ///         let mut columns = String::new();
    ///         if spec.selects_all() {
    ///             columns.push('*');
    ///         } else {
    ///             for column in spec.columns() {
    ///                 if !columns.is_empty() {
    ///                     columns.push_str(", ");
    ///                 }
    ///                 self.quote_into(column.name(), &mut columns);
    ///             }
    ///         }
    ///         // A target that names no schema lives in SQL Server's default one, `dbo`.
    ///         let mut into = String::new();
    ///         self.quote_into(target.schema().unwrap_or("dbo"), &mut into);
    ///         into.push('.');
    ///         self.quote_into(target.table(), &mut into);
    ///         let mut from = String::new();
    ///         self.quote_into(spec.table(), &mut from);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         let into_columns = if spec.selects_all() {
    ///             String::new()
    ///         } else {
    ///             format!(" ({columns})")
    ///         };
    ///         Ok(vec![
    ///             Statement::new(
    ///                 format!(
    ///                     "INSERT INTO {into}{into_columns} SELECT {columns} FROM {from} \
    ///                      WHERE {id} = @p1"
    ///                 ),
    ///                 [Param::Id],
    ///             ),
    ///             Statement::new(format!("DELETE FROM {from} WHERE {id} = @p1"), [Param::Id]),
    ///         ])
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
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
    /// # use std::num::NonZeroUsize;
    /// # use ruststream_sqlx_dialect::TableName;
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
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         // A table on the database's clock reads the server's time; otherwise the service
    ///         // binds it.
    ///         if spec.uses_database_clock() {
    ///             return Ok(Statement::new(
    ///                 "UPDATE [email_jobs] SET [processed_at] = SYSUTCDATETIME() \
    ///                  WHERE [job_id] = @p1",
    ///                 [Param::Id],
    ///             ));
    ///         }
    ///         Ok(Statement::new(
    ///             "UPDATE [email_jobs] SET [processed_at] = @p1 WHERE [job_id] = @p2",
    ///             [Param::Now, Param::Id],
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
    /// ```
    #[must_use]
    pub const fn uses_database_clock(&self) -> bool {
        self.database_clock
    }

    /// The message's data columns, in the order the description lists them.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const EMAILS: TableSpec<'static> =
    ///     TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
    ///         .data(&[Column::new("recipient"), Column::new("subject")]);
    ///
    /// let names: Vec<&str> = EMAILS.data_columns().iter().map(|column| column.name()).collect();
    /// assert_eq!(names, ["recipient", "subject"]);
    /// ```
    #[must_use]
    pub const fn data_columns(&self) -> &'a [Column<'a>] {
        self.data
    }

    /// The columns only a message assembled from the table reads, in the order the description
    /// lists them ([`fetching`](Self::fetching)).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const ORDERS: TableSpec<'static> =
    ///     TableSpec::new("order_jobs", Column::new("job_id"), Form::RowLock)
    ///         .data(&[Column::new("tenant")])
    ///         .fetching(&[Column::new("note")]);
    ///
    /// let names: Vec<&str> = ORDERS.fetched_columns().iter().map(|column| column.name()).collect();
    /// assert_eq!(names, ["note"]);
    /// ```
    #[must_use]
    pub const fn fetched_columns(&self) -> &'a [Column<'a>] {
        self.fetched
    }

    /// The column that plays `role`, or `None` when the table has none.
    ///
    /// The `locked_until` column is the one the lease form carries.
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
    ///     fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
    ///         let mut table = String::new();
    ///         self.quote_into(spec.table(), &mut table);
    ///         let mut id = String::new();
    ///         self.quote_into(spec.id().name(), &mut id);
    ///         // A table with a `processed_at` column keeps its finished rows and marks them.
    ///         let Some(done) = spec.column(Role::ProcessedAt) else {
    ///             return Ok(Statement::new(
    ///                 format!("DELETE FROM {table} WHERE {id} = @p1"),
    ///                 [Param::Id],
    ///             ));
    ///         };
    ///         let mut done_at = String::new();
    ///         self.quote_into(done.name(), &mut done_at);
    ///         Ok(Statement::new(
    ///             format!("UPDATE {table} SET {done_at} = @p1 WHERE {id} = @p2"),
    ///             [Param::Now, Param::Id],
    ///         ))
    ///     }
    /// #     fn quote_into(&self, ident: &str, out: &mut String) { out.push('['); out.push_str(&ident.replace(']', "]]")); out.push(']'); }
    /// #     fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) { out.push_str("@p"); out.push_str(&index.to_string()); }
    /// #     fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedFetch { dialect: self.name() }) }
    /// #     fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn dead_letter_table(&self, spec: &TableSpec<'_>, target: TableName<'_>) -> Result<Vec<Statement>, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// #     fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> { Err(StatementError::UnsupportedForm { dialect: self.name(), form: spec.form().name() }) }
    /// }
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

    /// Every column: the one of each role in [`Role::ALL`] order, then the message's data, then
    /// the columns a message assembled from the table reads ([`fetching`](Self::fetching)).
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
    pub fn columns(&self) -> impl Iterator<Item = Column<'a>> {
        Columns::new(self)
    }
}

/// Every column of a description in [`TableSpec::columns`] order, walked by a `const fn` as well
/// as by an iterator, so a `const` insert and a statement built at run time list the same columns.
#[derive(Debug, Clone)]
pub(crate) struct Columns<'s, 'a> {
    spec: &'s TableSpec<'a>,
    role: usize,
    data: usize,
    fetched: usize,
}

impl<'s, 'a> Columns<'s, 'a> {
    pub(crate) const fn new(spec: &'s TableSpec<'a>) -> Self {
        Self {
            spec,
            role: 0,
            data: 0,
            fetched: 0,
        }
    }

    /// The next column, or `None` past the last.
    pub(crate) const fn next_column(&mut self) -> Option<Column<'a>> {
        while self.role < Role::ALL.len() {
            let role = Role::ALL[self.role];
            self.role += 1;
            if let Some(column) = self.spec.column(role) {
                return Some(column);
            }
        }
        if self.data < self.spec.data.len() {
            self.data += 1;
            return Some(self.spec.data[self.data - 1]);
        }
        if self.fetched < self.spec.fetched.len() {
            self.fetched += 1;
            return Some(self.spec.fetched[self.fetched - 1]);
        }
        None
    }
}

impl<'a> Iterator for Columns<'_, 'a> {
    type Item = Column<'a>;

    fn next(&mut self) -> Option<Column<'a>> {
        self.next_column()
    }
}

#[cfg(test)]
mod tests;
