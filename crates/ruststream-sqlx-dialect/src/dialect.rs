//! The trait a database's SQL implements.

use std::fmt::Debug;
use std::num::NonZeroUsize;

use crate::spec::TableSpec;
use crate::statement::{ClaimShape, Statement, StatementError};
use crate::table_name::TableName;

/// A database's SQL: how it quotes names and numbers placeholders, and the statement each queue
/// event runs.
///
/// A dialect reads a [`TableSpec`] and answers with [`Statement`]s whose
/// [`Param`](crate::Param)s are bound in order. Statements are built while a subscription starts,
/// never per message, so a dialect of the service's own travels as `&dyn Dialect`. A dialect
/// refuses with a [`StatementError`] what it does not build, and never hands out a statement with
/// other semantics instead.
///
/// In the lease form a claim writes the lease's expiry into the row and commits. The expiry it
/// wrote is the delivery's ownership token ([`Param::Held`](crate::Param::Held)): every settlement
/// and the extension name the row and that token, so a delivery whose lease ran out, and whose
/// row another claim took, changes nothing. Beside its statements a dialect tells whether its
/// claim writes the lease ([`claim_writes_lease`](Self::claim_writes_lease)), which servers its
/// statements run on ([`check_server`](Self::check_server)), and how a claim's transaction opens
/// ([`begin_claim`](Self::begin_claim)).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "postgres")] {
/// use ruststream_sqlx_dialect::{
///     ClaimShape, Column, Dialect, Form, Postgres, Statement, StatementError, TableSpec,
/// };
///
/// const JOBS: TableSpec<'static> =
///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
///
/// // What a subscription prepares when it starts, whatever the database.
/// fn prepare(dialect: &dyn Dialect, spec: &TableSpec<'_>) -> Result<Vec<Statement>, StatementError> {
///     Ok(vec![dialect.claim(spec, ClaimShape::Rows)?, dialect.ack(spec)?])
/// }
///
/// let statements = prepare(&Postgres, &JOBS)?;
/// assert_eq!(statements[1].sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
/// # }
/// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
/// ```
pub trait Dialect: Debug + Send + Sync {
    /// The dialect's name, for messages.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// let dialect: &dyn Dialect = &Postgres;
    /// let context = format!("building the claim with the {} dialect", dialect.name());
    /// assert_eq!(context, "building the claim with the postgres dialect");
    /// # }
    /// ```
    fn name(&self) -> &'static str;

    /// Appends `ident` to `out`, quoted as a name of this database.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// // A name keeps its case and its spaces; an embedded quote doubles.
    /// let mut sql = String::from("SELECT * FROM ");
    /// Postgres.quote_into(r#"Email "Jobs""#, &mut sql);
    /// assert_eq!(sql, r#"SELECT * FROM "Email ""Jobs""""#);
    /// # }
    /// ```
    fn quote_into(&self, ident: &str, out: &mut String);

    /// Appends the placeholder of parameter number `index` (counted from 1) to `out`.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use std::num::NonZeroUsize;
    ///
    /// use ruststream_sqlx_dialect::{Dialect, Postgres};
    ///
    /// let mut sql = String::from(r#"DELETE FROM "jobs" WHERE "job_id" = "#);
    /// Postgres.placeholder_into(NonZeroUsize::MIN, &mut sql);
    /// assert_eq!(sql, r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
    /// # }
    /// ```
    fn placeholder_into(&self, index: NonZeroUsize, out: &mut String);

    /// The statement that claims up to [`Param::Limit`](crate::Param::Limit) rows of the
    /// subscription's group, in claim order.
    ///
    /// In the lease form it skips every row whose lease has not ended by
    /// [`Param::LeaseNow`](crate::Param::LeaseNow). Where
    /// [`claim_writes_lease`](Self::claim_writes_lease) answers `true`, it also writes the new
    /// lease ([`Param::Lease`](crate::Param::Lease)), counts the attempt, and returns the rows as
    /// they were before.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::UnsupportedFifo`] when the table has FIFO groups and the dialect
    /// has no claim that keeps them in order; [`StatementError::LeaseOnDatabaseClock`] for a
    /// lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{ClaimShape, Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .group(Column::new("name"))
    ///     .retry_after(Column::new("retry_after"))
    ///     .payload(Column::new("payload"));
    ///
    /// let claim = Postgres.claim(&JOBS, ClaimShape::Ids)?;
    /// assert_eq!(
    ///     claim.sql(),
    ///     r#"SELECT "job_id" FROM "jobs" WHERE "name" = $1 AND "retry_after" <= $2 ORDER BY "retry_after", "job_id" LIMIT $3 FOR UPDATE SKIP LOCKED"#,
    /// );
    /// assert_eq!(claim.params(), [Param::Group, Param::Now, Param::Limit]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn claim(&self, spec: &TableSpec<'_>, shape: ClaimShape) -> Result<Statement, StatementError>;

    /// The statement that reads the rows of claimed ids, bound as one list.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedFetch`] when the dialect cannot read rows by a list of ids, so
    /// a claim of the service's own needs a fetch of its own too. Postgres builds it for every
    /// table; MySQL refuses it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
    ///
    /// let fetch = Postgres.fetch(&JOBS)?;
    /// assert_eq!(
    ///     fetch.sql(),
    ///     r#"SELECT "job_id", "payload" FROM "jobs" WHERE "job_id" = ANY($1)"#,
    /// );
    /// assert_eq!(fetch.params(), [Param::Ids]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that acknowledges a row: it deletes the row, or sets `processed_at` when the
    /// table has that column.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .processed_at(Column::new("processed_at"));
    ///
    /// let ack = Postgres.ack(&JOBS)?;
    /// assert_eq!(ack.sql(), r#"UPDATE "jobs" SET "processed_at" = $1 WHERE "job_id" = $2"#);
    /// assert_eq!(ack.params(), [Param::Now, Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that releases a row for another attempt at once, or `None` when releasing
    /// the row needs no statement.
    ///
    /// In the lease form it clears the lease, and the attempt stays as the claim counted it.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).attempt(Column::new("attempt"));
    ///
    /// // In the row lock form a retry counts the attempt; the rollback releases the row.
    /// let retry = Postgres.retry(&JOBS)?.map(|statement| statement.sql().to_owned());
    /// assert_eq!(
    ///     retry.as_deref(),
    ///     Some(r#"UPDATE "jobs" SET "attempt" = "attempt" + 1 WHERE "job_id" = $1"#),
    /// );
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError>;

    /// The statement that releases a row for another attempt after a delay, bound as
    /// [`Param::RetryAfter`](crate::Param::RetryAfter).
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] when the table has no `retry_after` column;
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock)
    ///     .retry_after(Column::new("retry_after"));
    ///
    /// let retry_after = Postgres.retry_after(&JOBS)?;
    /// assert_eq!(
    ///     retry_after.sql(),
    ///     r#"UPDATE "jobs" SET "retry_after" = $1 WHERE "job_id" = $2"#,
    /// );
    /// assert_eq!(retry_after.params(), [Param::RetryAfter, Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that drops a row: it deletes the row, or sets `processed_at` when the table
    /// has that column.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
    ///
    /// let discard = Postgres.discard(&JOBS)?;
    /// assert_eq!(discard.sql(), r#"DELETE FROM "jobs" WHERE "job_id" = $1"#);
    /// assert_eq!(discard.params(), [Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statement that moves a row whose attempts are spent to another group, bound as
    /// [`Param::Destination`](crate::Param::Destination).
    ///
    /// # Errors
    ///
    /// [`StatementError::MissingRole`] when the table has no `group` column;
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).group(Column::new("name"));
    ///
    /// let dead_letter = Postgres.dead_letter_group(&JOBS)?;
    /// assert_eq!(dead_letter.sql(), r#"UPDATE "jobs" SET "name" = $1 WHERE "job_id" = $2"#);
    /// assert_eq!(dead_letter.params(), [Param::Destination, Param::Id]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// The statements that move a row whose attempts are spent to `target`, a table with the same
    /// columns; they run in one transaction.
    ///
    /// A table read with `*` ([`TableSpec::selects_all`]) has columns the description does not
    /// name, so its row moves by position: `target` has the same columns in the same order. In
    /// the lease form the row arrives without a lease, so whatever reads `target` can claim it at
    /// once.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect has no statements for the table's
    /// form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the database's clock;
    /// [`StatementError::Flattened`] for a lease table read with `*`, whose lease column the move
    /// cannot name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::error::Error;
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, TableName, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("payload"));
    ///
    /// let target = TableName::parse("archive.jobs_dead")?;
    /// let moves = Postgres.dead_letter_table(&JOBS, target)?;
    /// assert_eq!(
    ///     moves[0].sql(),
    ///     r#"WITH moved AS (DELETE FROM "jobs" WHERE "job_id" = $1 RETURNING "job_id", "payload") INSERT INTO "archive"."jobs_dead" ("job_id", "payload") SELECT "job_id", "payload" FROM moved"#,
    /// );
    /// # }
    /// # Ok::<(), Box<dyn Error>>(())
    /// ```
    fn dead_letter_table(
        &self,
        spec: &TableSpec<'_>,
        target: TableName<'_>,
    ) -> Result<Vec<Statement>, StatementError>;

    /// The statement that extends a delivery's lease: it writes the new expiry
    /// ([`Param::Lease`](crate::Param::Lease)) while the row still holds the delivery's token
    /// ([`Param::Held`](crate::Param::Held)), so a handler that runs longer than one lease keeps
    /// its row.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect builds no lease statements, and for a
    /// table in another form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the
    /// database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")));
    ///
    /// let extend = Postgres.extend(&JOBS)?;
    /// assert_eq!(
    ///     extend.sql(),
    ///     r#"UPDATE "jobs" SET "locked_until" = $1 WHERE "job_id" = $2 AND "locked_until" = $3"#,
    /// );
    /// // The new expiry, the row, and the expiry the delivery holds until this statement runs.
    /// assert_eq!(extend.params(), [Param::Lease, Param::Id, Param::Held]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Err(StatementError::UnsupportedForm {
            dialect: self.name(),
            form: spec.form().name(),
        })
    }

    /// The statement that leases one claimed row: it writes the expiry
    /// ([`Param::Lease`](crate::Param::Lease)) and counts the attempt, while no lease holds the
    /// row at [`Param::LeaseNow`](crate::Param::LeaseNow).
    ///
    /// A claim that only selects its rows runs it for each of them inside its transaction: the
    /// claim of a dialect whose [`claim_writes_lease`](Self::claim_writes_lease) answers `false`,
    /// or a claim the service writes itself.
    ///
    /// # Errors
    ///
    /// [`StatementError::UnsupportedForm`] when the dialect builds no lease statements, and for a
    /// table in another form; [`StatementError::LeaseOnDatabaseClock`] for a lease table on the
    /// database's clock.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")))
    ///         .attempt(Column::new("attempt"));
    ///
    /// let stamp = Postgres.stamp(&JOBS)?;
    /// assert_eq!(
    ///     stamp.sql(),
    ///     r#"UPDATE "jobs" SET "locked_until" = $1, "attempt" = "attempt" + 1 WHERE "job_id" = $2 AND ("locked_until" IS NULL OR "locked_until" <= $3)"#,
    /// );
    /// assert_eq!(stamp.params(), [Param::Lease, Param::Id, Param::LeaseNow]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
        Err(StatementError::UnsupportedForm {
            dialect: self.name(),
            form: spec.form().name(),
        })
    }

    /// The statement that inserts a row: every column the database does not fill, in the order of
    /// [`TableSpec::columns`], each bound as [`Param::Column`](crate::Param::Column).
    ///
    /// # Errors
    ///
    /// [`StatementError::Flattened`] when the table is read with `*`
    /// ([`TableSpec::selects_all`]), whose columns the description cannot see.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Param, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id").generated(), Form::RowLock)
    ///         .payload(Column::new("payload"));
    ///
    /// // The database fills the id; the service writes the payload.
    /// let insert = Postgres.insert(&JOBS)?;
    /// assert_eq!(insert.sql(), r#"INSERT INTO "jobs" ("payload") VALUES ($1)"#);
    /// assert_eq!(insert.params(), [Param::Column(1)]);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError>;

    /// Whether the lease claim writes the lease itself. A dialect whose lease claim only selects
    /// the rows answers `false`, and the claim's transaction then runs [`stamp`](Self::stamp) for
    /// each claimed row before it commits.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{
    ///     ClaimShape, Column, Dialect, Form, Postgres, Statement, StatementError, TableSpec,
    /// };
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("job_id"), Form::Lease(Column::new("locked_until")));
    ///
    /// // What a broker prepares to claim leased rows.
    /// fn claiming(
    ///     dialect: &dyn Dialect,
    ///     spec: &TableSpec<'_>,
    /// ) -> Result<Vec<Statement>, StatementError> {
    ///     let mut statements = vec![dialect.claim(spec, ClaimShape::Rows)?];
    ///     if !dialect.claim_writes_lease() {
    ///         // The claim only selects: each claimed row is stamped before the commit.
    ///         statements.push(dialect.stamp(spec)?);
    ///     }
    ///     Ok(statements)
    /// }
    ///
    /// // Postgres locks, stamps and returns the rows in one statement.
    /// assert_eq!(claiming(&Postgres, &JOBS)?.len(), 1);
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn claim_writes_lease(&self) -> bool {
        true
    }

    /// The query that reads the server's version as one text column, or `None` when the
    /// dialect's statements run on every version of its server.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, StatementError, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // What a broker runs once per subscription, before it prepares the statements.
    /// fn check(
    ///     dialect: &dyn Dialect,
    ///     spec: &TableSpec<'_>,
    ///     mut query: impl FnMut(&str) -> String,
    /// ) -> Result<(), StatementError> {
    ///     match dialect.server_version() {
    ///         Some(sql) => dialect.check_server(spec, &query(sql)),
    ///         None => Ok(()),
    ///     }
    /// }
    ///
    /// // Postgres asks its server nothing.
    /// let mut asked = Vec::new();
    /// check(&Postgres, &JOBS, |sql| {
    ///     asked.push(sql.to_owned());
    ///     "17.2".to_owned()
    /// })?;
    /// assert!(asked.is_empty());
    /// # }
    /// # Ok::<(), ruststream_sqlx_dialect::StatementError>(())
    /// ```
    fn server_version(&self) -> Option<&'static str> {
        None
    }

    /// Refuses a server older than the statements of `spec` need; `version` is what the
    /// [`server_version`](Self::server_version) query returned.
    ///
    /// # Errors
    ///
    /// [`StatementError::ServerTooOld`] when the server predates the dialect's statements for the
    /// table's form.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "postgres")] {
    /// use ruststream_sqlx_dialect::{Column, Dialect, Form, Postgres, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> = TableSpec::new("jobs", Column::new("job_id"), Form::RowLock);
    ///
    /// // A startup check of the version the server reported; a refusal stops the subscription
    /// // and names it.
    /// let checked = Postgres
    ///     .check_server(&JOBS, "17.2")
    ///     .map_err(|refused| format!("subscription `emails`: {refused}"));
    /// assert_eq!(checked, Ok(()));
    /// # }
    /// ```
    fn check_server(&self, spec: &TableSpec<'_>, version: &str) -> Result<(), StatementError> {
        let _ = (spec, version);
        Ok(())
    }

    /// The statement that opens a claim's transaction in place of `BEGIN`, or `None` when `BEGIN`
    /// opens it.
    ///
    /// A claim that runs in a transaction (the row lock form, and a lease claim that only selects
    /// its rows) opens it with this statement, which leaves the connection inside a transaction
    /// as `BEGIN` does. MySQL opens it at READ COMMITTED, so a claim locks no gaps between rows
    /// and holds back no insert into the table.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "postgres", feature = "mysql"))] {
    /// use ruststream_sqlx_dialect::{Dialect, MySql, Postgres};
    ///
    /// // What a broker sends to open a claim's transaction.
    /// fn opening(dialect: &dyn Dialect) -> &'static str {
    ///     dialect.begin_claim().unwrap_or("BEGIN")
    /// }
    ///
    /// assert_eq!(opening(&Postgres), "BEGIN");
    /// assert_eq!(
    ///     opening(&MySql),
    ///     "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
    /// );
    /// # }
    /// ```
    fn begin_claim(&self) -> Option<&'static str> {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::Dialect;
    use crate::column::Column;
    use crate::form::Form;
    use crate::spec::TableSpec;
    use crate::statement::{ClaimShape, Statement, StatementError};
    use crate::table_name::TableName;

    /// A dialect of a service's own that builds no statement and leaves every hook to its
    /// default.
    #[derive(Debug)]
    struct Refusing;

    impl Refusing {
        fn refuse<Built>(&self, spec: &TableSpec<'_>) -> Result<Built, StatementError> {
            Err(StatementError::UnsupportedForm {
                dialect: self.name(),
                form: spec.form().name(),
            })
        }
    }

    impl Dialect for Refusing {
        fn name(&self) -> &'static str {
            "refusing"
        }

        fn quote_into(&self, ident: &str, out: &mut String) {
            out.push_str(ident);
        }

        fn placeholder_into(&self, _: NonZeroUsize, out: &mut String) {
            out.push('?');
        }

        fn claim(&self, spec: &TableSpec<'_>, _: ClaimShape) -> Result<Statement, StatementError> {
            self.refuse(spec)
        }

        fn fetch(&self, _: &TableSpec<'_>) -> Result<Statement, StatementError> {
            Err(StatementError::UnsupportedFetch {
                dialect: self.name(),
            })
        }

        fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
            self.refuse(spec)
        }

        fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
            self.refuse(spec)
        }

        fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
            self.refuse(spec)
        }

        fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
            self.refuse(spec)
        }

        fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
            self.refuse(spec)
        }

        fn dead_letter_table(
            &self,
            spec: &TableSpec<'_>,
            _: TableName<'_>,
        ) -> Result<Vec<Statement>, StatementError> {
            self.refuse(spec)
        }

        fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
            self.refuse(spec)
        }
    }

    #[test]
    fn a_dialect_without_lease_statements_refuses_their_hooks() {
        const JOBS: TableSpec<'static> = TableSpec::new(
            "jobs",
            Column::new("job_id"),
            Form::Lease(Column::new("locked_until")),
        );
        let refused = StatementError::UnsupportedForm {
            dialect: "refusing",
            form: "lease",
        };
        assert_eq!(Refusing.extend(&JOBS), Err(refused.clone()));
        assert_eq!(Refusing.stamp(&JOBS), Err(refused));
    }
}
