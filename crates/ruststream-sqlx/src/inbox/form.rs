//! The forms of claiming as types: what a table's form asks of its dialect, checked where a
//! subscription mounts, and the dialect seen through that form's trait once it holds.

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
