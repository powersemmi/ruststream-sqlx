//! How a description is built: the constructor and one setter per role and setting, each filling
//! its slot once.

use super::{Grouping, TableSpec};
use crate::column::Column;
use crate::form::Form;
use crate::opening::{Isolation, Mode, Opening};

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
}
