//! The lease form's text: the condition that no lease holds a row, and the claims of the
//! dialects whose claim writes the lease.

#[cfg(feature = "postgres")]
use super::claim::ORDER_KEYS;
use super::{BuiltIn, SqlWriter};
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use crate::role::Role;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use crate::spec::TableSpec;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use crate::statement::ClaimShape;
use crate::statement::Param;

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// No lease holds the row: it has none, or its lease ended by [`Param::LeaseNow`].
    pub(crate) fn lease_free(&mut self, expiry: &str) -> &mut Self {
        self.push("(")
            .ident(expiry)
            .push(" IS NULL OR ")
            .ident(expiry)
            .push(" <= ")
            .param(Param::LeaseNow)
            .push(")")
    }

    /// The lease claim in one statement: the claim's select under `lock`, an update that writes
    /// the claimed rows' lease ([`Param::Lease`]) into `expiry` and counts their attempt, and the
    /// rows as the select read them, in claim order.
    #[cfg(feature = "postgres")]
    pub(crate) fn lease_claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        expiry: &str,
        lock: &str,
    ) -> &mut Self {
        let id = spec.id().name();
        // A claim by role names the id by its role.
        let claimed_id = match shape {
            ClaimShape::Roles => Role::Id.attribute(),
            ClaimShape::Rows | ClaimShape::Ids => id,
        };
        self.push("WITH __claimed AS (SELECT ")
            .claimed_columns(spec, shape);
        if matches!(shape, ClaimShape::Ids | ClaimShape::Roles) {
            // The rows return in claim order, so the select keeps its keys; whole rows have them.
            for column in ORDER_KEYS.iter().filter_map(|&role| spec.column(role)) {
                self.push(", ").ident(column.name());
            }
        }
        self.claimed_rows(spec, lock)
            .push("), __stamped AS (UPDATE ")
            .table(spec)
            .push(" AS __row SET ")
            .ident(expiry)
            .push(" = ")
            .param(Param::Lease);
        if let Some(attempt) = spec.column(Role::Attempt) {
            // Qualified: the claimed rows may carry an attempt of their own.
            self.push(", ")
                .ident(attempt.name())
                .push(" = __row.")
                .ident(attempt.name())
                .push(" + 1");
        }
        self.push(" FROM __claimed WHERE __row.")
            .ident(id)
            .push(" = __claimed.")
            .ident(claimed_id)
            .push(") SELECT ");
        match shape {
            ClaimShape::Rows => self.push("*"),
            ClaimShape::Ids => self.ident(id),
            ClaimShape::Roles => self.role_names(spec),
        };
        self.push(" FROM __claimed").claim_order(spec, claimed_id)
    }

    /// The lease claim as one update that returns the rows it took: it writes the lease
    /// ([`Param::Lease`]) into `expiry` and counts the attempt of the claimable rows a subquery
    /// picks in claim order, and returns the columns of `shape` as the rows were before.
    #[cfg(feature = "sqlite")]
    pub(crate) fn returning_claim(
        &mut self,
        spec: &TableSpec<'_>,
        shape: ClaimShape,
        expiry: &str,
    ) -> &mut Self {
        let id = spec.id().name();
        self.push("UPDATE ")
            .table(spec)
            .push(" SET ")
            .ident(expiry)
            .push(" = ")
            .param(Param::Lease);
        if let Some(attempt) = spec.column(Role::Attempt) {
            self.push(", ").increment(attempt.name());
        }
        // The subquery takes no lock: the update holds the database's one write lock.
        self.push(" WHERE ")
            .ident(id)
            .push(" IN (SELECT ")
            .ident(id)
            .claimed_rows(spec, "")
            .push(") RETURNING ")
            .counted_columns(spec, shape)
    }
}
