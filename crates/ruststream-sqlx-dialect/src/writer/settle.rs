//! The row a settlement names, and what a settlement adds to it in the lease form.

use super::{BuiltIn, SqlWriter};
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::Param;

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// The row a settlement names: its id, and in the lease form the delivery's token.
    pub(crate) fn settled_row(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.push(" WHERE ")
            .ident(spec.id().name())
            .push(" = ")
            .param(Param::Id)
            .held(spec)
    }

    /// In the lease form, the condition that the row still holds the delivery's lease
    /// ([`Param::Held`]), following a condition on the row's id; nothing in the other forms.
    pub(crate) fn held(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if let Some(expiry) = spec.column(Role::LockedUntil) {
            self.push(" AND ")
                .ident(expiry.name())
                .push(" = ")
                .param(Param::Held);
        }
        self
    }

    /// In the lease form, one more assignment that clears the lease; nothing in the other forms.
    pub(crate) fn release(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if let Some(expiry) = spec.column(Role::LockedUntil) {
            self.push(", ").ident(expiry.name()).push(" = NULL");
        }
        self
    }
}
