//! The claim's select every form builds on: the conditions a row meets, the claim order and
//! the claimed rows; and the row lock form's claim.

use super::{BuiltIn, SqlWriter};
use crate::role::Role;
use crate::spec::TableSpec;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use crate::statement::ClaimShape;
use crate::statement::Param;

/// The keys a claim orders by before the id, the most significant first.
pub(super) const ORDER_KEYS: [Role; 2] = [Role::Priority, Role::RetryAfter];

/// A condition a claim puts on a row, written only where the table has the column it reads.
#[derive(Debug, Clone, Copy)]
pub(super) enum Condition {
    /// The row is in the subscription's group.
    InGroup,
    /// The row's time has come.
    Due,
    /// The row carries no finish mark.
    Unfinished,
    /// No lease holds the row.
    Free,
}

impl Condition {
    /// The role of the column the condition reads.
    fn role(self) -> Role {
        match self {
            Self::InGroup => Role::Group,
            Self::Due => Role::RetryAfter,
            Self::Unfinished => Role::ProcessedAt,
            Self::Free => Role::LockedUntil,
        }
    }
}

/// What a row meets to be claimed, in the order a claim of many rows writes it.
pub(super) const CLAIMABLE: [Condition; 4] = [
    Condition::InGroup,
    Condition::Due,
    Condition::Unfinished,
    Condition::Free,
];

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// The conditions a row meets to be claimed, each present only with its column: the
    /// subscription's group, a time that has come, no finish mark, no lease in force.
    pub(crate) fn claimable(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.conditions(spec, &CLAIMABLE, " WHERE ")
    }

    /// Each of `conditions` the table has a column for: the first after `keyword`, the others
    /// after ` AND `.
    pub(super) fn conditions(
        &mut self,
        spec: &TableSpec<'_>,
        conditions: &[Condition],
        keyword: &'static str,
    ) -> &mut Self {
        self.conditions_then(spec, conditions, keyword);
        self
    }

    /// Each of `conditions` the table has a column for, as [`conditions`](Self::conditions)
    /// writes them; returns what a condition after them opens with: `keyword` when the table has
    /// none of them, ` AND ` otherwise.
    pub(super) fn conditions_then(
        &mut self,
        spec: &TableSpec<'_>,
        conditions: &[Condition],
        keyword: &'static str,
    ) -> &'static str {
        let mut keyword = keyword;
        for &condition in conditions {
            let Some(column) = spec.column(condition.role()) else {
                continue;
            };
            self.push(keyword);
            keyword = " AND ";
            match condition {
                Condition::InGroup => self.ident(column.name()).push(" = ").param(Param::Group),
                Condition::Due => self.ident(column.name()).push(" <= ").now(spec),
                Condition::Unfinished => self.ident(column.name()).push(" IS NULL"),
                Condition::Free => self.lease_free(column.name()),
            };
        }
        keyword
    }

    /// The claim order: `priority`, then `retry_after`, then the id, each key only with its
    /// column.
    pub(crate) fn claim_order(&mut self, spec: &TableSpec<'_>, id: &str) -> &mut Self {
        self.push(" ORDER BY ");
        for column in ORDER_KEYS.iter().filter_map(|&role| spec.column(role)) {
            self.ident(column.name()).push(", ");
        }
        self.ident(id)
    }

    /// A claim's select after its columns, locked by `lock`: the claimable rows of the table in
    /// claim order, at most [`Param::Limit`] of them; in a table with FIFO groups, the group's
    /// head alone, while it is due and free, and in the lease form while no row of the group holds
    /// a lease.
    pub(super) fn claimed_rows(&mut self, spec: &TableSpec<'_>, lock: &str) -> &mut Self {
        let id = spec.id().name();
        self.push(" FROM ").table(spec);
        if spec.is_fifo() {
            // The select of the head takes no lock, so a head another claim holds stays the head:
            // `lock` falls on the head row alone, and a held head leaves the claim empty instead
            // of handing out the row behind it.
            self.push(" WHERE ")
                .ident(id)
                .push(" = (SELECT ")
                .ident(id)
                .push(" FROM ")
                .table(spec)
                .head_conditions(spec)
                .claim_order(spec, id)
                .push(" LIMIT 1)")
                .taking_conditions(spec)
                .group_free(spec);
        } else {
            self.claimable(spec)
                .claim_order(spec, id)
                .push(" LIMIT ")
                .param(Param::Limit);
        }
        self.push(lock)
    }

    /// The claim that selects rows and locks them with `lock`: the columns of `shape`, of the
    /// claimable rows in claim order.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    pub(crate) fn claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        lock: &str,
    ) -> &mut Self {
        self.push("SELECT ")
            .claimed_columns(spec, shape)
            .claimed_rows(spec, lock)
    }
}
