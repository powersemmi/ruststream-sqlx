//! The roles a column plays for the queue, as `#[field(..)]` names them.

use std::fmt::{self, Display, Formatter};

/// What a column does for the queue, named as `#[field(..)]` spells it.
///
/// A struct marks the columns that run its queue with roles; every other column is the
/// message's own data.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::Role;
///
/// // An attribute parser reads `#[field(retry_after)]` this way.
/// assert_eq!(Role::from_attribute("retry_after"), Some(Role::RetryAfter));
/// assert_eq!(Role::from_attribute("deadline"), None);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// The row's identity, for settlement and for matching fetched rows to claimed ids.
    Id,
    /// The group a subscription reads: the rows of one table split into groups.
    Group,
    /// The key of the delivery's lane under `workers(n, by_key)` or `threads(n, by_key)`.
    PartitionKey,
    /// The claim order: a smaller value is claimed first.
    Priority,
    /// The time before which a row is not claimed.
    RetryAfter,
    /// The attempt number; the first delivery reads 1.
    Attempt,
    /// The expiry of a lease; a column playing it selects the lease form.
    LockedUntil,
    /// The time a row was finished; acknowledgement sets it instead of deleting the row.
    ProcessedAt,
    /// The delivery's headers.
    Headers,
    /// The message bytes a handler decodes.
    Payload,
}

impl Role {
    /// Every role, in the order the documentation lists them.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Role;
    ///
    /// // The hint an attribute parser gives for a name it does not know.
    /// let known: Vec<&str> = Role::ALL.iter().copied().map(Role::attribute).collect();
    /// let hint = format!("expected one of: {}", known.join(", "));
    /// assert!(hint.starts_with("expected one of: id, group, partition_key"));
    /// ```
    pub const ALL: &'static [Self] = &[
        Self::Id,
        Self::Group,
        Self::PartitionKey,
        Self::Priority,
        Self::RetryAfter,
        Self::Attempt,
        Self::LockedUntil,
        Self::ProcessedAt,
        Self::Headers,
        Self::Payload,
    ];

    /// The role's name inside `#[field(..)]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Role;
    ///
    /// // A startup check names the attribute a struct lacks.
    /// let missing = Role::RetryAfter;
    /// let hint = format!("add `#[field({})]` to the struct", missing.attribute());
    /// assert_eq!(hint, "add `#[field(retry_after)]` to the struct");
    /// ```
    #[must_use]
    pub const fn attribute(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Group => "group",
            Self::PartitionKey => "partition_key",
            Self::Priority => "priority",
            Self::RetryAfter => "retry_after",
            Self::Attempt => "attempt",
            Self::LockedUntil => "locked_until",
            Self::ProcessedAt => "processed_at",
            Self::Headers => "headers",
            Self::Payload => "payload",
        }
    }

    /// The role `#[field(..)]` names with `name`, or `None` when no role has that name.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_sqlx_dialect::Role;
    ///
    /// // An attribute parser tells a role from the other words `#[field(..)]` takes.
    /// let words = ["group", "generated"];
    /// let roles: Vec<Role> = words.iter().filter_map(|word| Role::from_attribute(word)).collect();
    /// assert_eq!(roles, [Role::Group]);
    /// ```
    #[must_use]
    pub fn from_attribute(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|role| role.attribute() == name)
    }
}

impl Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.attribute())
    }
}

#[cfg(test)]
mod tests {
    use super::Role;

    #[test]
    fn every_role_reads_back_from_its_attribute() {
        for role in Role::ALL {
            assert_eq!(Role::from_attribute(role.attribute()), Some(*role));
            assert_eq!(role.to_string(), role.attribute());
        }
        assert_eq!(Role::ALL.len(), 10);
        assert_eq!(Role::from_attribute("deadline"), None);
    }
}
