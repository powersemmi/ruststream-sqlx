//! What a subscription knows of its table when it opens, and the retry declarations a table
//! refuses.

use std::any::type_name;
use std::time::Duration;

use ruststream::RetryDeclaration;
use ruststream_sqlx_dialect::{ClaimShape, Form, Role, TableName, TableSpec};

use crate::inbox::InboxRow;
use crate::inbox::database::QueueDatabase;
use crate::inbox::engine::{Events, IdAt, Shape};
use crate::inbox::error::SqlxBrokerError;
use crate::inbox::named::kinds::Kinds;
use crate::inbox::publish::table_of;
use crate::inbox::threads::InboxThreads;

/// What a subscription names of its timing; the broker's where it names nothing.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Timing {
    /// How long the claim loop waits after a claim that found the queue short.
    pub(crate) poll_interval: Option<Duration>,
    /// How long a claim leases a row, in the lease form.
    pub(crate) lease: Option<Duration>,
    /// The dedicated threads the subscription runs its handlers on, with their pools' size.
    pub(crate) threads: Option<InboxThreads>,
}

/// `lease` in whole seconds, rounded up, and at least one.
pub(super) fn whole_seconds(lease: Duration) -> Duration {
    let seconds = lease
        .as_secs()
        .saturating_add(u64::from(lease.subsec_nanos() > 0));
    Duration::from_secs(seconds.max(1))
}

/// What a subscription knows of its table when it opens: the description, the events the
/// service implements itself, how the claim selects, and the struct that reads the rows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Description {
    /// The table.
    pub(crate) spec: TableSpec<'static>,
    /// The events the service implements itself.
    pub(crate) shape: Shape,
    /// What the claim selects.
    pub(crate) claim: ClaimShape,
    /// The struct's type, for messages.
    pub(crate) row: &'static str,
    /// The kinds a by-name subscription reads the rows by, where the struct's types allow it.
    pub(crate) kinds: Option<Kinds>,
}

impl Description {
    /// The description `Row`'s derive gives: its table, its events, and whole rows to claim (ids
    /// alone where the service fetches the rows itself).
    pub(crate) fn of<DB, Row>() -> Self
    where
        DB: QueueDatabase,
        Row: InboxRow + Events<DB>,
    {
        let shape = Row::SHAPE;
        Self {
            spec: Row::SPEC,
            shape,
            claim: if shape.custom_fetch {
                ClaimShape::Ids
            } else {
                ClaimShape::Rows
            },
            row: type_name::<Row>(),
            kinds: Row::kinds(),
        }
    }

    /// The description a by-name subscription reads the table by: the crate's own events, and
    /// the columns that run the queue under their roles' names.
    pub(crate) fn by_role(self) -> Self {
        Self {
            shape: Shape::default(),
            claim: ClaimShape::Roles,
            ..self
        }
    }

    /// Whether a delayed retry is the database's own: the table holds `retry_after`, or the
    /// service implements the event.
    pub(crate) const fn native_retry_after(&self) -> bool {
        self.shape.custom_retry_after || self.spec.column(Role::RetryAfter).is_some()
    }

    /// Whether the table is claimed by lease: it has a `locked_until` column.
    pub(crate) const fn leased(&self) -> bool {
        self.spec.column(Role::LockedUntil).is_some()
    }

    /// Whether the table is claimed by advisory lock: its struct names a lock key.
    pub(crate) const fn advisory(&self) -> bool {
        matches!(self.spec.form(), Form::Advisory(_))
    }

    /// Where the claim's select carries the id of a row read alone.
    pub(crate) const fn id_at(&self) -> IdAt {
        match self.claim {
            // Role aliases list the id first, whatever the struct flattens.
            ClaimShape::Roles => IdAt::First,
            ClaimShape::Rows | ClaimShape::Ids => IdAt::of(&self.spec),
        }
    }
}

/// Why `declaration` cannot apply to the table `description` reads, if it cannot.
pub(crate) fn refused_declaration(
    name: &str,
    declaration: &RetryDeclaration,
    description: &Description,
) -> Option<SqlxBrokerError> {
    let spec = description.spec;
    let refuse = |reason: String| SqlxBrokerError::Declaration {
        subscription: name.to_owned(),
        table: table_of(&spec),
        row: description.row,
        reason,
    };
    // The table moves a spent row itself, so the cap and the destination come together: a half
    // would leave the row nowhere to go, or nothing to count before it goes.
    match (declaration.max_attempts(), declaration.dead_letter()) {
        (Some(_), None) => {
            return Some(refuse(
                "the registration declares `max_attempts(..)` without `dead_letter(..)`: name \
                 where a row whose attempts are spent goes"
                    .to_owned(),
            ));
        }
        (None, Some(_)) => {
            return Some(refuse(
                "the registration declares `dead_letter(..)` without `max_attempts(..)`: name the \
                 cap, `max_attempts(1)` to move a row at its first failure"
                    .to_owned(),
            ));
        }
        _ => {}
    }
    if declaration.max_attempts().is_some() && spec.column(Role::Attempt).is_none() {
        return Some(refuse(
            "`max_attempts(..)` counts deliveries in the `attempt` column: add \
             `#[field(attempt)]` to the struct"
                .to_owned(),
        ));
    }
    match declaration.dead_letter() {
        Some(target) if spec.column(Role::Group).is_none() => TableName::parse(target)
            .err()
            .map(|err| refuse(format!("the dead-letter table {err}"))),
        _ => None,
    }
}

#[cfg(test)]
pub(super) mod tests {
    //! What opening a subscription builds: its description and its lease.

    use std::time::Duration;

    use ruststream_sqlx_dialect::{ClaimShape, Column, TableSpec};

    use super::{Description, whole_seconds};
    use crate::inbox::engine::{IdAt, Shape};
    use crate::inbox::form::tests::JOBS;

    pub(in crate::inbox) fn described(
        spec: &TableSpec<'static>,
        shape: Shape,
        claim: ClaimShape,
    ) -> Description {
        Description {
            spec: *spec,
            shape,
            claim,
            row: "Job",
            kinds: None,
        }
    }

    #[test]
    fn a_delayed_retry_is_native_with_its_column_or_the_services_event() {
        let bare = described(&JOBS, Shape::default(), ClaimShape::Rows);
        assert!(!bare.native_retry_after());
        let column = described(
            &JOBS.retry_after(Column::new("retry_after")),
            Shape::default(),
            ClaimShape::Rows,
        );
        assert!(column.native_retry_after());
        let custom = Shape {
            custom_retry_after: true,
            ..Shape::default()
        };
        assert!(described(&JOBS, custom, ClaimShape::Rows).native_retry_after());
    }

    #[test]
    fn a_lease_is_whole_seconds_and_at_least_one() {
        assert_eq!(
            whole_seconds(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            whole_seconds(Duration::from_millis(1500)),
            Duration::from_secs(2)
        );
        assert_eq!(whole_seconds(Duration::ZERO), Duration::from_secs(1));
        assert_eq!(
            whole_seconds(Duration::MAX),
            Duration::from_secs(u64::MAX),
            "the longest lease saturates instead of wrapping"
        );
    }

    #[test]
    fn a_claim_by_role_carries_the_id_first_whatever_the_struct_flattens() {
        let flat = JOBS.selecting_all();
        assert_eq!(
            described(&flat, Shape::default(), ClaimShape::Rows).id_at(),
            IdAt::Named("job_id")
        );
        assert_eq!(
            described(&flat, Shape::default(), ClaimShape::Roles).id_at(),
            IdAt::First
        );
        assert_eq!(
            described(&JOBS, Shape::default(), ClaimShape::Rows).id_at(),
            IdAt::First
        );
    }
}
