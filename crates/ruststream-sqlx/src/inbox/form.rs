//! The forms of claiming as types: what a table's form asks of its dialect, checked where a
//! subscription mounts, and the dialect seen through that form's trait once it holds.

pub(crate) mod advisory;
pub(crate) mod lease;
pub(crate) mod row_lock;

use std::sync::Arc;

use ruststream_sqlx_dialect::{
    Advisory, ClaimShape, Dialect, Lease, RowLock, Statement, StatementError, TableSpec,
};

/// A form of claiming rows that the dialect `D` serves. Machinery: a subscription requires it of
/// its table's [`InboxRow::Form`](super::InboxRow::Form), so a table in a form its dialect lacks
/// does not compile.
#[doc(hidden)]
pub trait FormOn<D> {
    /// `dialect`, seen through the trait of this form.
    fn erase(dialect: &Arc<D>) -> FormDialect;
}

/// The row lock form: the claim's transaction holds the row. Machinery; the derive names it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct RowLockForm;

/// The lease form: the expiry in `locked_until` holds the row. Machinery; the derive names it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseForm;

/// The advisory lock form: a lock on the row's key holds the row. Machinery; the derive names it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct AdvisoryForm;

impl<D: RowLock + 'static> FormOn<D> for RowLockForm {
    fn erase(dialect: &Arc<D>) -> FormDialect {
        FormDialect::RowLock(Arc::clone(dialect) as Arc<dyn RowLock>)
    }
}

impl<D: Lease + 'static> FormOn<D> for LeaseForm {
    fn erase(dialect: &Arc<D>) -> FormDialect {
        FormDialect::Lease(Arc::clone(dialect) as Arc<dyn Lease>)
    }
}

impl<D: Advisory + 'static> FormOn<D> for AdvisoryForm {
    fn erase(dialect: &Arc<D>) -> FormDialect {
        FormDialect::Advisory(Arc::clone(dialect) as Arc<dyn Advisory>)
    }
}

/// A dialect seen through the trait of a table's form: what a subscription builds that form's
/// statements with when it starts. Machinery.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum FormDialect {
    /// A dialect with the row lock form.
    RowLock(Arc<dyn RowLock>),
    /// A dialect with the lease form.
    Lease(Arc<dyn Lease>),
    /// A dialect with the advisory lock form.
    Advisory(Arc<dyn Advisory>),
}

impl FormDialect {
    /// The dialect itself: the statements every form runs.
    pub(crate) fn dialect(&self) -> &dyn Dialect {
        match self {
            Self::RowLock(dialect) => dialect.as_ref(),
            Self::Lease(dialect) => dialect.as_ref(),
            Self::Advisory(dialect) => dialect.as_ref(),
        }
    }

    /// The dialect's lease form, where the table takes rows by lease.
    pub(crate) fn lease(&self) -> Option<&dyn Lease> {
        match self {
            Self::Lease(dialect) => Some(dialect.as_ref()),
            Self::RowLock(_) | Self::Advisory(_) => None,
        }
    }

    /// The dialect's advisory lock form, where the table takes rows by advisory lock.
    pub(crate) fn advisory(&self) -> Option<&dyn Advisory> {
        match self {
            Self::Advisory(dialect) => Some(dialect.as_ref()),
            Self::RowLock(_) | Self::Lease(_) => None,
        }
    }

    /// The claim of `spec` in `shape`, built by the trait of the table's form: in the advisory
    /// lock form, the candidates with their keys, whatever the shape.
    ///
    /// # Errors
    ///
    /// The dialect's refusal.
    pub(crate) fn claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        match self {
            Self::RowLock(dialect) => dialect.lock_claim(spec, shape),
            Self::Lease(dialect) => dialect.lease_claim(spec, shape),
            Self::Advisory(dialect) => dialect.advisory_claim(spec),
        }
    }

    /// The statement that opens a claim's transaction of `spec`'s table in place of `BEGIN`,
    /// where the dialect names one: the row lock claim's opens at the table's opening, the lease
    /// claim's as the lease form opens it. The advisory claim selects its candidates outside any
    /// transaction of the crate's.
    ///
    /// The dialect answers for the table's opening in every form, so a table at a level or in a
    /// mode its dialect does not open stops its subscription when it starts. Under `BuiltIn<Any>`
    /// that is the first moment the database is known.
    ///
    /// # Errors
    ///
    /// The dialect's refusal of the table's opening.
    pub(crate) fn begin_claim(
        &self,
        spec: &TableSpec<'_>,
    ) -> Result<Option<&'static str>, StatementError> {
        let opening = self.dialect().begin(spec.opening())?;
        Ok(match self {
            Self::RowLock(_) => opening,
            Self::Lease(dialect) => dialect.begin_lease_claim(),
            Self::Advisory(_) => None,
        })
    }
}

#[cfg(test)]
pub(in crate::inbox) mod tests {
    //! The claim's transaction as each form opens it, on a dialect of a service's own and on the
    //! built-in ones.

    use ruststream_sqlx_dialect::{Column, Form, TableSpec};

    /// The jobs in the row lock form.
    pub(in crate::inbox) const JOBS: TableSpec<'static> =
        TableSpec::new("jobs", Column::new("job_id"), Form::RowLock).payload(Column::new("body"));

    #[cfg(feature = "postgres")]
    pub(in crate::inbox) mod reshaped {
        //! A dialect of a service's own: Postgres's statements, each form's claim and transaction
        //! opening marked with the trait that built it, its dead-letter move and its take cut into
        //! `parts` statements, and its lease claim writing the lease or only selecting the rows.

        use std::num::NonZeroUsize;
        use std::sync::Arc;

        use ruststream_sqlx_dialect::{
            Advisory, ClaimShape, Column, Dialect, Form, Isolation, KeyPart, Lease, Opening,
            Postgres, RowLock, Statement, StatementError, TableName, TableSpec,
        };

        use crate::inbox::FormDialect;

        /// The Postgres dialect, its dead-letter move and its take cut into `parts` statements, its
        /// lease claim writing the lease or only selecting the rows, and each form's claim and
        /// transaction opening marked with the trait that built it. The row lock claim opens with
        /// the dialect's own `begin`, which opens SERIALIZABLE beside the default and refuses every
        /// other level.
        #[derive(Debug)]
        pub(in crate::inbox) struct Reshaped {
            pub(in crate::inbox) parts: usize,
            pub(in crate::inbox) writes_lease: bool,
        }

        /// The claim `trait_name` built for `spec`: Postgres's text, marked with the trait's name.
        fn marked(
            trait_name: &str,
            claim: Result<Statement, StatementError>,
        ) -> Result<Statement, StatementError> {
            let claim = claim?;
            Ok(Statement::new(
                format!("/* {trait_name} */ {}", claim.sql()),
                claim.params().iter().copied(),
            ))
        }

        impl Dialect for Reshaped {
            fn name(&self) -> &'static str {
                "reshaped"
            }

            fn quote_into(&self, ident: &str, out: &mut String) {
                Postgres.quote_into(ident, out);
            }

            fn placeholder_into(&self, index: NonZeroUsize, out: &mut String) {
                Postgres.placeholder_into(index, out);
            }

            fn fetch(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.fetch(spec)
            }

            fn ack(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.ack(spec)
            }

            fn retry(&self, spec: &TableSpec<'_>) -> Result<Option<Statement>, StatementError> {
                Postgres.retry(spec)
            }

            fn retry_after(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.retry_after(spec)
            }

            fn discard(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.discard(spec)
            }

            fn dead_letter_group(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.dead_letter_group(spec)
            }

            fn dead_letter_table(
                &self,
                spec: &TableSpec<'_>,
                target: TableName<'_>,
            ) -> Result<Vec<Statement>, StatementError> {
                let mut moves = Postgres.dead_letter_table(spec, target)?;
                moves.resize(self.parts, moves[0].clone());
                Ok(moves)
            }

            fn insert(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.insert(spec)
            }

            fn begin(&self, opening: Opening) -> Result<Option<&'static str>, StatementError> {
                match opening {
                    Opening::Default => Ok(Some("BEGIN /* Dialect */")),
                    Opening::Isolation(Isolation::Serializable) => {
                        Ok(Some("BEGIN ISOLATION LEVEL SERIALIZABLE /* Dialect */"))
                    }
                    _ => Err(StatementError::UnsupportedOpening {
                        dialect: self.name(),
                        opening: opening.name(),
                    }),
                }
            }
        }

        impl RowLock for Reshaped {
            fn lock_claim(
                &self,
                spec: &TableSpec<'_>,
                shape: ClaimShape,
            ) -> Result<Statement, StatementError> {
                marked("RowLock", Postgres.lock_claim(spec, shape))
            }
        }

        impl Lease for Reshaped {
            fn lease_claim(
                &self,
                spec: &TableSpec<'_>,
                shape: ClaimShape,
            ) -> Result<Statement, StatementError> {
                marked("Lease", Postgres.lease_claim(spec, shape))
            }

            fn extend(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.extend(spec)
            }

            fn stamp(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                Postgres.stamp(spec)
            }

            fn claim_writes_lease(&self) -> bool {
                self.writes_lease
            }

            fn begin_lease_claim(&self) -> Option<&'static str> {
                Some("BEGIN /* Lease */")
            }
        }

        impl Advisory for Reshaped {
            fn advisory_claim(&self, spec: &TableSpec<'_>) -> Result<Statement, StatementError> {
                marked("Advisory", Postgres.advisory_claim(spec))
            }

            fn lock(&self) -> Option<Statement> {
                Postgres.lock()
            }

            fn unlock(&self) -> Option<Statement> {
                Postgres.unlock()
            }

            fn take(
                &self,
                spec: &TableSpec<'_>,
                shape: ClaimShape,
            ) -> Result<Vec<Statement>, StatementError> {
                let mut takes = Postgres.take(spec, shape)?;
                takes.resize(self.parts, takes[0].clone());
                Ok(takes)
            }
        }

        pub(in crate::inbox) const LEASED: TableSpec<'static> = TableSpec::new(
            "jobs",
            Column::new("job_id"),
            Form::Lease(Column::new("locked_until")),
        )
        .payload(Column::new("body"));

        /// The lock key of every job: its id.
        pub(in crate::inbox) const KEY: &[KeyPart<'static>] =
            &[KeyPart::Literal("jobs-"), KeyPart::Column("job_id")];

        /// The jobs in the advisory lock form, with an attempt the take counts.
        pub(in crate::inbox) const ADVISED: TableSpec<'static> =
            TableSpec::new("jobs", Column::new("job_id"), Form::Advisory(KEY))
                .attempt(Column::new("attempt"))
                .payload(Column::new("body"));

        pub(in crate::inbox) const RESHAPED: Reshaped = Reshaped {
            parts: 1,
            writes_lease: true,
        };

        /// `dialect` seen through the trait of `spec`'s form, as a subscription to it sees it.
        pub(in crate::inbox) fn form_of(dialect: Reshaped, spec: &TableSpec<'_>) -> FormDialect {
            match spec.form() {
                Form::RowLock => FormDialect::RowLock(Arc::new(dialect)),
                Form::Lease(_) => FormDialect::Lease(Arc::new(dialect)),
                _ => FormDialect::Advisory(Arc::new(dialect)),
            }
        }

        #[test]
        fn each_form_opens_its_claim_with_the_statement_of_its_trait() -> Result<(), StatementError>
        {
            // The row lock claim's transaction opens at the table's opening, as the dialect opens
            // it.
            assert_eq!(
                form_of(RESHAPED, &super::JOBS).begin_claim(&super::JOBS)?,
                Some("BEGIN /* Dialect */")
            );
            let serializable = super::JOBS.isolation(Isolation::Serializable);
            assert_eq!(
                form_of(RESHAPED, &serializable).begin_claim(&serializable)?,
                Some("BEGIN ISOLATION LEVEL SERIALIZABLE /* Dialect */")
            );
            // The lease claim's own short transaction opens as the lease form opens it, whatever
            // the table's opening.
            let leased = LEASED.isolation(Isolation::Serializable);
            assert_eq!(
                form_of(RESHAPED, &leased).begin_claim(&leased)?,
                Some("BEGIN /* Lease */")
            );
            // The candidates of an advisory claim are selected outside any transaction of the
            // crate's.
            let advised = ADVISED.isolation(Isolation::Serializable);
            assert_eq!(form_of(RESHAPED, &advised).begin_claim(&advised)?, None);
            Ok(())
        }

        #[test]
        fn a_table_at_an_opening_its_dialect_lacks_is_refused_whatever_its_form() {
            // The lease claim does not open at the table's opening, and still the subscription
            // stops when it starts: a level the dialect lacks is no level its transactions run at.
            for spec in [super::JOBS, LEASED, ADVISED] {
                let uncommitted = spec.isolation(Isolation::ReadUncommitted);
                assert_eq!(
                    form_of(RESHAPED, &uncommitted).begin_claim(&uncommitted),
                    Err(StatementError::UnsupportedOpening {
                        dialect: "reshaped",
                        opening: "isolation `read_uncommitted`",
                    }),
                    "{}",
                    spec.form().name()
                );
            }
        }
    }

    #[cfg(feature = "mysql")]
    mod on_mysql {
        //! What a subscription to a table opens its claims with on MySQL and MariaDB, through the
        //! built-in dialect the broker builds its statements with. The servers do not tell a
        //! transaction its own level reliably, so the begin statement is what pins it.

        use std::sync::Arc;

        use ruststream_sqlx_dialect::{
            Column, Form, Isolation, Mode, MySql, StatementError, TableSpec,
        };

        use crate::inbox::BuiltIn;
        use crate::inbox::form::{FormOn, LeaseForm, RowLockForm};

        const LEASED: TableSpec<'static> = TableSpec::new(
            "jobs",
            Column::new("job_id"),
            Form::Lease(Column::new("locked_until")),
        )
        .payload(Column::new("body"));

        fn built_in() -> Arc<BuiltIn<sqlx::MySql>> {
            Arc::new(BuiltIn::new(MySql))
        }

        #[test]
        fn a_row_lock_claim_opens_at_the_tables_isolation() -> Result<(), StatementError> {
            let form = <RowLockForm as FormOn<BuiltIn<sqlx::MySql>>>::erase(&built_in());
            let cases = [
                (
                    super::JOBS,
                    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
                ),
                (
                    super::JOBS.isolation(Isolation::RepeatableRead),
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION",
                ),
                (
                    super::JOBS.isolation(Isolation::Serializable),
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION",
                ),
            ];
            for (spec, begin) in cases {
                assert_eq!(
                    form.begin_claim(&spec)?,
                    Some(begin),
                    "{:?}",
                    spec.opening()
                );
            }
            Ok(())
        }

        #[test]
        fn a_lease_claim_keeps_read_committed_and_still_refuses_a_mode()
        -> Result<(), StatementError> {
            let form = <LeaseForm as FormOn<BuiltIn<sqlx::MySql>>>::erase(&built_in());
            let serializable = LEASED.isolation(Isolation::Serializable);
            assert_eq!(
                form.begin_claim(&serializable)?,
                Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION")
            );
            let immediate = LEASED.mode(Mode::Immediate);
            assert_eq!(
                form.begin_claim(&immediate),
                Err(StatementError::UnsupportedOpening {
                    dialect: "mysql",
                    opening: "mode `immediate`",
                })
            );
            Ok(())
        }
    }

    #[cfg(all(feature = "any", feature = "sqlite"))]
    mod on_any {
        //! What an `AnyPool` refuses when a subscription starts: its database is known only then,
        //! so a table may name a level the picked backend lacks.

        use std::sync::Arc;

        use ruststream_sqlx_dialect::{Column, Form, Isolation, Mode, StatementError, TableSpec};
        use sqlx::Any;

        use crate::inbox::database::built_in::SQLITE_BACKEND;
        use crate::inbox::form::{FormOn, LeaseForm};
        use crate::inbox::{AnyDialect, BuiltIn};

        const LEASED: TableSpec<'static> = TableSpec::new(
            "jobs",
            Column::new("job_id"),
            Form::Lease(Column::new("locked_until")),
        )
        .payload(Column::new("body"));

        #[test]
        fn a_sqlite_backend_refuses_an_isolation_level_whatever_the_form()
        -> Result<(), StatementError> {
            let picked = AnyDialect::of(SQLITE_BACKEND).expect("the sqlite feature is on");
            let form = <LeaseForm as FormOn<BuiltIn<Any>>>::erase(&Arc::new(BuiltIn::new(picked)));
            let serializable = LEASED.isolation(Isolation::Serializable);
            assert_eq!(
                form.begin_claim(&serializable),
                Err(StatementError::UnsupportedOpening {
                    dialect: "sqlite",
                    opening: "isolation `serializable`",
                })
            );
            // A mode it opens passes, and the lease claim opens as the lease form opens it.
            assert_eq!(
                form.begin_claim(&LEASED.mode(Mode::Exclusive))?,
                Some("BEGIN IMMEDIATE")
            );
            Ok(())
        }
    }
}
