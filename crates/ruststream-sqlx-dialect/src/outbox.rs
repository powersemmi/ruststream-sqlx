//! The statements of a transactional outbox table: the take of a record by its id, the mark of a
//! processed one, and the recovery of the unprocessed records of a name.

use crate::dialect::Dialect;
use crate::spec::TableSpec;
use crate::statement::{Statement, StatementError};

/// The statements a dialect builds for an outbox table, which records what a service publishes
/// until a consumer has processed it.
///
/// An outbox table is described by a [`TableSpec`] whose id identifies a record, whose
/// [`group`](TableSpec::group) column holds the name the record was published under, and whose
/// optional [`processed_at`](TableSpec::processed_at) column marks a processed record. A table
/// without it deletes a processed record instead. The table's [`Form`](crate::Form) does not
/// apply: a record is taken by its id, never claimed. `#[derive(Outbox)]` in `ruststream-sqlx`
/// builds these statements at compile time with each built-in dialect, and on the database's own
/// clock ([`TableSpec::database_clock`]). [`Postgres`](crate::Postgres), [`MySql`](crate::MySql)
/// and [`Sqlite`](crate::Sqlite) implement it.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")]
/// # fn main() -> Result<(), ruststream_sqlx_dialect::StatementError> {
/// use ruststream_sqlx_dialect::{Column, Form, OutboxDialect, Param, Postgres, TableSpec};
///
/// // outbox: id BIGSERIAL PRIMARY KEY, name TEXT, payload BYTEA, processed_at TIMESTAMPTZ
/// const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
///     .group(Column::new("name"))
///     .processed_at(Column::new("processed_at"))
///     .payload(Column::new("payload"))
///     .database_clock();
///
/// // What a consumer of a tracked message runs: the take of its record, then the mark once the
/// // handler acknowledged it.
/// let fetch = Postgres.outbox_fetch(&OUTBOX)?;
/// assert_eq!(
///     fetch.sql(),
///     r#"SELECT "id", "name", "processed_at", "payload" FROM "outbox" WHERE "id" = $1 AND "processed_at" IS NULL"#
/// );
/// let mark = Postgres.outbox_mark(&OUTBOX)?;
/// assert_eq!(
///     mark.sql(),
///     r#"UPDATE "outbox" SET "processed_at" = statement_timestamp() WHERE "id" = $1"#
/// );
///
/// // What the republish at startup runs for each registered name.
/// let recover = Postgres.outbox_recover(&OUTBOX)?;
/// assert_eq!(recover.params(), [Param::Group]);
/// # Ok(())
/// # }
/// # #[cfg(not(feature = "postgres"))]
/// # fn main() {}
/// ```
pub trait OutboxDialect: Dialect {
    /// The statement that reads the record [`Param::Id`](crate::Param::Id) names while it is
    /// unprocessed, with every column: what the consumer of a tracked message takes into work.
    ///
    /// # Errors
    ///
    /// [`StatementError::IdentifierTooLong`] for a name longer than the database keeps.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")]
    /// # fn main() -> Result<(), ruststream_sqlx_dialect::StatementError> {
    /// use ruststream_sqlx_dialect::{Column, Form, MySql, OutboxDialect, Param, TableSpec};
    ///
    /// // A table without `processed_at` deletes a processed record, so every record it holds is
    /// // unprocessed.
    /// const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    ///     .group(Column::new("name"))
    ///     .payload(Column::new("payload"));
    ///
    /// let fetch = MySql.outbox_fetch(&OUTBOX)?;
    /// assert_eq!(fetch.sql(), "SELECT `id`, `name`, `payload` FROM `outbox` WHERE `id` = ?");
    /// assert_eq!(fetch.params(), [Param::Id]);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "mysql"))]
    /// # fn main() {}
    /// ```
    fn outbox_fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that marks the record [`Param::Id`](crate::Param::Id) names processed: it
    /// sets `processed_at` to now, or deletes the record in a table without that column.
    ///
    /// Where the table runs in the row lock form, this is the dialect's
    /// [`ack`](Dialect::ack) of it.
    ///
    /// # Errors
    ///
    /// [`StatementError::IdentifierTooLong`] for a name longer than the database keeps.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "sqlite")]
    /// # fn main() -> Result<(), ruststream_sqlx_dialect::StatementError> {
    /// use ruststream_sqlx_dialect::{Column, Form, OutboxDialect, Sqlite, TableSpec};
    ///
    /// const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    ///     .group(Column::new("name"))
    ///     .processed_at(Column::new("processed_at"))
    ///     .payload(Column::new("payload"))
    ///     .database_clock();
    ///
    /// // The mark reads SQLite's own clock, as text that sorts as the times do.
    /// let mark = Sqlite.outbox_mark(&OUTBOX)?;
    /// assert_eq!(
    ///     mark.sql(),
    ///     "UPDATE `outbox` SET `processed_at` = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now') \
    ///      WHERE `id` = ?"
    /// );
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "sqlite"))]
    /// # fn main() {}
    /// ```
    fn outbox_mark(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that reads the unprocessed records published under the name
    /// [`Param::Group`](crate::Param::Group) binds, with every column: what the republish at
    /// startup sends again.
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] for a table without a [`group`](TableSpec::group) column,
    /// and [`StatementError::IdentifierTooLong`] for a name longer than the database keeps.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")]
    /// # fn main() -> Result<(), ruststream_sqlx_dialect::StatementError> {
    /// use ruststream_sqlx_dialect::{Column, Form, OutboxDialect, Param, Postgres, TableSpec};
    ///
    /// const OUTBOX: TableSpec<'static> = TableSpec::new("outbox", Column::new("id"), Form::RowLock)
    ///     .within("app")
    ///     .group(Column::new("name"))
    ///     .processed_at(Column::new("processed_at"))
    ///     .payload(Column::new("payload"));
    ///
    /// let recover = Postgres.outbox_recover(&OUTBOX)?;
    /// assert_eq!(
    ///     recover.sql(),
    ///     r#"SELECT "id", "name", "processed_at", "payload" FROM "app"."outbox" WHERE "name" = $1 AND "processed_at" IS NULL"#
    /// );
    /// assert_eq!(recover.params(), [Param::Group]);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(feature = "postgres"))]
    /// # fn main() {}
    /// ```
    fn outbox_recover(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;
}
