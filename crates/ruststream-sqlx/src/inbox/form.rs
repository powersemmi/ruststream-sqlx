//! The forms of claiming as types: what a table's form asks of its dialect, checked where a
//! subscription mounts, and the dialect seen through that form's trait once it holds.

use std::sync::Arc;

use ruststream_sqlx_dialect::{
    ClaimShape, Dialect, Lease, RowLock, Statement, StatementError, TableSpec,
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

// Why the advisory form compiles on every dialect: no trait of the dialect crate builds its
// statements, so its subscription refuses it when it starts, naming the dialect and the form.
impl<D: Dialect + 'static> FormOn<D> for AdvisoryForm {
    fn erase(dialect: &Arc<D>) -> FormDialect {
        FormDialect::Unbuilt(Arc::clone(dialect) as Arc<dyn Dialect>)
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
    /// A dialect whose traits build no statements of the table's form.
    Unbuilt(Arc<dyn Dialect>),
}

impl FormDialect {
    /// The dialect itself: the statements every form runs.
    pub(crate) fn dialect(&self) -> &dyn Dialect {
        match self {
            Self::RowLock(dialect) => dialect.as_ref(),
            Self::Lease(dialect) => dialect.as_ref(),
            Self::Unbuilt(dialect) => dialect.as_ref(),
        }
    }

    /// The dialect's lease form, where the table takes rows by lease.
    pub(crate) fn lease(&self) -> Option<&dyn Lease> {
        match self {
            Self::Lease(dialect) => Some(dialect.as_ref()),
            Self::RowLock(_) | Self::Unbuilt(_) => None,
        }
    }

    /// The claim of `spec` in `shape`, built by the trait of the table's form.
    ///
    /// # Errors
    ///
    /// The dialect's refusal; [`StatementError::UnsupportedForm`] for a form no trait builds.
    pub(crate) fn claim(
        &self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
    ) -> Result<Statement, StatementError> {
        match self {
            Self::RowLock(dialect) => dialect.lock_claim(spec, shape),
            Self::Lease(dialect) => dialect.lease_claim(spec, shape),
            Self::Unbuilt(dialect) => Err(StatementError::UnsupportedForm {
                dialect: dialect.name(),
                form: spec.form().name(),
            }),
        }
    }

    /// The statement that opens a claim's transaction in place of `BEGIN`, where the dialect
    /// names one for the table's form.
    pub(crate) fn begin_claim(&self) -> Option<&'static str> {
        match self {
            Self::RowLock(dialect) => dialect.begin_lock_claim(),
            Self::Lease(dialect) => dialect.begin_lease_claim(),
            Self::Unbuilt(_) => None,
        }
    }
}
