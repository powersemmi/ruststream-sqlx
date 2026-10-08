//! The rule of a description the types do not hold: each column is named once. It runs where
//! `InboxRow::SPEC` is evaluated, which every subscription of the table does, so a broken
//! description stops the build.

use ruststream_sqlx_dialect::{Role, TableSpec};

/// `spec`, after the check that it names each column once.
///
/// # Panics
///
/// Panics, at build time, when the description names a column twice.
pub(super) const fn checked(spec: &TableSpec<'static>) -> TableSpec<'static> {
    assert!(
        !names_a_column_twice(spec),
        "a queue table's description names a column twice: a column plays one role or holds one \
         field, so name each column once across the id, the roles, `data` and `fetching`"
    );
    *spec
}

const fn names_a_column_twice(spec: &TableSpec<'_>) -> bool {
    let count = Role::ALL.len() + spec.data_columns().len() + spec.fetched_columns().len();
    let mut first = 0;
    while first < count {
        if let Some(name) = nth(spec, first) {
            let mut second = first + 1;
            while second < count {
                if let Some(other) = nth(spec, second)
                    && same(name, other)
                {
                    return true;
                }
                second += 1;
            }
        }
        first += 1;
    }
    false
}

/// The name at `index` of the description's columns read as one list: a slot per role (empty
/// where no column plays it), then `data`, then `fetching`.
const fn nth<'spec>(spec: &TableSpec<'spec>, index: usize) -> Option<&'spec str> {
    if index < Role::ALL.len() {
        return match spec.column(Role::ALL[index]) {
            Some(column) => Some(column.name()),
            None => None,
        };
    }
    let index = index - Role::ALL.len();
    let data = spec.data_columns();
    if index < data.len() {
        return Some(data[index].name());
    }
    let fetched = spec.fetched_columns();
    let index = index - data.len();
    if index < fetched.len() {
        Some(fetched[index].name())
    } else {
        None
    }
}

const fn same(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use ruststream_sqlx_dialect::{Column, Form, TableSpec};

    use super::names_a_column_twice;

    const TENANT: &[Column<'static>] = &[Column::new("tenant")];
    const NOTE: &[Column<'static>] = &[Column::new("note")];
    const LEASE: &[Column<'static>] = &[Column::new("locked_until")];
    const PAYLOAD: &[Column<'static>] = &[Column::new("payload")];
    const PAIR: &[Column<'static>] = &[Column::new("a"), Column::new("a")];

    const fn jobs() -> TableSpec<'static> {
        TableSpec::new(
            "jobs",
            Column::new("id"),
            Form::Lease(Column::new("locked_until")),
        )
        .group(Column::new("name"))
        .payload(Column::new("payload"))
    }

    #[test]
    fn a_description_of_distinct_columns_passes() {
        assert!(!names_a_column_twice(&jobs().data(TENANT).fetching(NOTE)));
    }

    #[test]
    fn a_column_named_twice_anywhere_is_found() {
        let twice = [
            ("two roles", jobs().attempt(Column::new("name"))),
            ("the id and a role", jobs().priority(Column::new("id"))),
            ("the lease and data", jobs().data(LEASE)),
            ("a role and fetched", jobs().fetching(PAYLOAD)),
            ("data twice", jobs().data(PAIR)),
            ("data and fetched", jobs().data(NOTE).fetching(NOTE)),
        ];
        for (case, spec) in twice {
            assert!(names_a_column_twice(&spec), "{case}");
        }
    }
}
