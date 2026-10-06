//! FIFO groups: the head of a group, what it meets to be taken, and the lease form's wait
//! for the group's row in work.

use super::claim::Condition;
use super::{BuiltIn, SqlWriter};
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::Param;

/// What makes a row a candidate for its group's head: the head is the first of these rows in
/// claim order.
const HEAD: [Condition; 2] = [Condition::InGroup, Condition::Unfinished];

/// What the head of a group meets for a claim to take it.
const TAKING: [Condition; 2] = [Condition::Due, Condition::Free];

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// The conditions of a row that may be its group's head, each present only with its column:
    /// the subscription's group, no finish mark.
    pub(super) fn head_conditions(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.conditions(spec, &HEAD, " WHERE ")
    }

    /// The conditions the head of a group meets for a claim to take it, each present only with
    /// its column and each after ` AND `, as they follow the condition that names the head: a
    /// time that has come, no lease in force.
    pub(super) fn taking_conditions(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.conditions(spec, &TAKING, " AND ")
    }

    /// The count of the subscription's group's unfinished rows, read under `lock`.
    #[cfg(feature = "mysql")]
    pub(crate) fn group_count(&mut self, spec: &TableSpec<'_>, lock: &str) -> &mut Self {
        self.push("SELECT COUNT(*) FROM ")
            .table(spec)
            .head_conditions(spec)
            .push(lock)
    }

    /// In the lease form, the condition that no row of the subscription's group holds a lease in
    /// force at [`Param::LeaseNow`], after ` AND `; nothing in the other forms. A row that enters
    /// the group ahead of a head in work becomes the head, and this keeps it waiting until the
    /// row in work settles or its lease ends.
    pub(super) fn group_free(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        let (Some(group), Some(expiry)) =
            (spec.column(Role::Group), spec.column(Role::LockedUntil))
        else {
            return self;
        };
        self.push(" AND NOT EXISTS (SELECT 1 FROM ")
            .table(spec)
            .push(" AS __work WHERE __work.")
            .ident(group.name())
            .push(" = ")
            .param(Param::Group)
            .push(" AND __work.")
            .ident(expiry.name())
            .push(" > ")
            .param(Param::LeaseNow)
            .push(")")
    }
}
