//! The columns a statement lists, by claim shape.

use super::{BuiltIn, SqlWriter};
use crate::column::Column;
use crate::role::Role;
use crate::spec::TableSpec;
use crate::statement::ClaimShape;

/// The columns a reader that knows no struct reads, with the roles they play: `id`,
/// `partition_key`, `attempt`, `headers` and `payload`, in [`Role::ALL`] order and only where the
/// table has them.
fn read_by_role<'a>(spec: &TableSpec<'a>) -> impl Iterator<Item = (Role, Column<'a>)> {
    Role::ALL
        .iter()
        .filter(|role| {
            matches!(
                role,
                Role::Id | Role::PartitionKey | Role::Attempt | Role::Headers | Role::Payload
            )
        })
        .filter_map(|&role| spec.column(role).map(|column| (role, column)))
}

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// Every column, or `*` when the struct flattens another.
    pub(crate) fn columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.selects_all() {
            return self.push("*");
        }
        for (index, column) in spec.columns().enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name());
        }
        self
    }

    /// Every column as a moved row carries it into another table, `NULL` in place of the lease's
    /// expiry so the row arrives without a lease; `*` when the struct flattens another.
    pub(crate) fn moved_columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.selects_all() {
            return self.push("*");
        }
        let expiry = spec.column(Role::LockedUntil).map(|column| column.name());
        for (index, column) in spec.columns().enumerate() {
            if index > 0 {
                self.push(", ");
            }
            if expiry == Some(column.name()) {
                self.push("NULL");
            } else {
                self.ident(column.name());
            }
        }
        self
    }

    /// The columns a reader that knows no struct reads, each under its role's attribute name:
    /// `id`, `partition_key`, `attempt`, `headers` and `payload`, in [`Role::ALL`] order and only
    /// where the table has them.
    pub(crate) fn role_columns(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.roles_read(spec, false)
    }

    /// The columns a reader that knows no struct reads, under their roles' names, with the attempt
    /// one less than the row holds where `counted`: the rows of an update that counted it, read
    /// as they were before.
    fn roles_read(&mut self, spec: &TableSpec<'_>, counted: bool) -> &mut Self {
        for (index, (role, column)) in read_by_role(spec).enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name());
            if counted && role == Role::Attempt {
                self.push(D::UNCOUNT);
            }
            self.push(" AS ").ident(role.attribute());
        }
        self
    }

    /// Every column of a row an update that counted its attempt returns, the attempt one less
    /// under its own name, so the row reads as it was before; `*` when the struct flattens
    /// another, whose columns the description cannot see.
    fn columns_before_count(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        if spec.selects_all() {
            return self.push("*");
        }
        let attempt = spec.column(Role::Attempt).map(|column| column.name());
        for (index, column) in spec.columns().enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(column.name());
            if attempt == Some(column.name()) {
                self.push(D::UNCOUNT).push(" AS ").ident(column.name());
            }
        }
        self
    }

    /// The names [`role_columns`](Self::role_columns) gives its columns, as a select over its
    /// rows reads them.
    #[cfg(feature = "postgres")]
    pub(super) fn role_names(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        for (index, (role, _)) in read_by_role(spec).enumerate() {
            if index > 0 {
                self.push(", ");
            }
            self.ident(role.attribute());
        }
        self
    }

    /// The columns a claim of `shape` selects.
    pub(super) fn claimed_columns(&mut self, spec: &TableSpec<'_>, shape: ClaimShape) -> &mut Self {
        match shape {
            ClaimShape::Rows => self.columns(spec),
            ClaimShape::Ids => self.ident(spec.id().name()),
            ClaimShape::Roles => self.role_columns(spec),
        }
    }

    /// The columns of `shape` of a row whose attempt an update counted, read as the row was
    /// before the count.
    pub(super) fn counted_columns(&mut self, spec: &TableSpec<'_>, shape: ClaimShape) -> &mut Self {
        match shape {
            ClaimShape::Rows => self.columns_before_count(spec),
            ClaimShape::Ids => self.ident(spec.id().name()),
            ClaimShape::Roles => self.roles_read(spec, true),
        }
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use crate::column::Column;
    use crate::dialect::Dialect;
    use crate::form::Form;
    use crate::postgres::Postgres;
    use crate::row_lock::RowLock;
    use crate::spec::TableSpec;
    use crate::statement::{ClaimShape, StatementError};

    /// The queue table of a message assembled from it: the headers struct's columns, then the
    /// message's own.
    const ASSEMBLED: TableSpec<'static> =
        TableSpec::new("order_jobs", Column::new("job_id"), Form::RowLock)
            .group(Column::new("name"))
            .data(&[Column::new("tenant")])
            .fetching(&[Column::new("note")]);

    #[test]
    fn a_claim_and_a_fetch_name_the_messages_columns_after_the_headers()
    -> Result<(), StatementError> {
        let claim = Postgres.lock_claim(&ASSEMBLED, ClaimShape::Rows)?;
        assert!(
            claim
                .sql()
                .starts_with(r#"SELECT "job_id", "name", "tenant", "note" FROM "order_jobs""#),
            "{}",
            claim.sql()
        );
        let fetch = Postgres.fetch(&ASSEMBLED)?;
        assert!(
            fetch
                .sql()
                .starts_with(r#"SELECT "job_id", "name", "tenant", "note" FROM "order_jobs""#),
            "{}",
            fetch.sql()
        );
        Ok(())
    }
}
