//! The wake-up: a publish from the same process wakes the subscriptions of the table and the group
//! it wrote, so they claim the row before their poll interval runs out.
//!
//! A connection keeps one [`TableWake`] per table its publishers write or its subscriptions read.
//! Each holds one [`Notify`] per group a subscription of the connection reads (one for a table
//! without groups), in a list that only grows: a publisher walks it after each write that
//! succeeded, with one acquire load per entry and no lock, and calls `notify_one` on its group's
//! entry. A claim loop waiting its interval wakes at once; one busy claiming finds the wake-up
//! kept as a permit, so its next wait returns at once.

use std::sync::{Mutex, OnceLock, PoisonError};

use ruststream_sqlx_dialect::{Role, TableSpec};
use tokio::sync::Notify;

use super::table_of;

/// The subscriptions of one table on one connection, woken by its publishers.
#[derive(Debug)]
pub(crate) struct TableWake {
    /// The table, qualified with its schema.
    table: Box<str>,
    /// Whether the table keeps groups: a publish then wakes only its own group's subscription.
    grouped: bool,
    /// The first entry of the list.
    first: OnceLock<&'static Entry>,
}

/// The wake-up of one group's subscription.
#[derive(Debug)]
struct Entry {
    /// The group; empty for a table without groups.
    group: Box<str>,
    notify: Notify,
    next: OnceLock<&'static Self>,
}

impl Entry {
    fn leak(group: &str) -> &'static Self {
        Box::leak(Box::new(Self {
            group: group.into(),
            notify: Notify::new(),
            next: OnceLock::new(),
        }))
    }
}

impl TableWake {
    /// The wake-up a subscription to `name` of the table waits on, kept for every later
    /// subscription to the same group on the connection. Cold: run as a subscription opens.
    pub(crate) fn subscribe(&self, name: &str) -> &'static Notify {
        // A table without groups is one queue, whatever name its subscription gives it.
        let group = if self.grouped { name } else { "" };
        // `get_or_init` lets one caller append at the tail, so two subscriptions opening at once
        // neither lose an entry nor add a group twice.
        let mut entry = *self.first.get_or_init(|| Entry::leak(group));
        loop {
            if &*entry.group == group {
                return &entry.notify;
            }
            entry = *entry.next.get_or_init(|| Entry::leak(group));
        }
    }

    /// Wakes the subscription of the group `name` after a write into the table; every subscription
    /// of a table without groups.
    pub(crate) fn wake(&self, name: &str) {
        let mut next = self.first.get();
        while let Some(entry) = next {
            if !self.grouped || &*entry.group == name {
                entry.notify.notify_one();
                return;
            }
            next = entry.next.get();
        }
    }

    /// Wakes every subscription of the table: for a write whose group is not known.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the Postgres listener wakes every table")
    )]
    pub(crate) fn wake_all(&self) {
        let mut next = self.first.get();
        while let Some(entry) = next {
            entry.notify.notify_one();
            next = entry.next.get();
        }
    }

    /// The table, qualified with its schema.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the Postgres listener names its channels")
    )]
    pub(crate) fn table(&self) -> &str {
        &self.table
    }
}

/// A connection's wake-ups, one per table. Cold: read when a publisher pairs and when a
/// subscription opens.
#[derive(Debug, Default)]
pub(crate) struct Wakes {
    tables: Mutex<Vec<&'static TableWake>>,
}

impl Wakes {
    /// The wake-up of the table `spec` describes, by its qualified name: every struct over one
    /// table shares it.
    pub(crate) fn table(&self, spec: &TableSpec<'_>) -> &'static TableWake {
        let table = table_of(spec);
        let mut tables = self.tables.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(found) = tables.iter().find(|wake| *wake.table == *table) {
            return found;
        }
        let wake: &'static TableWake = Box::leak(Box::new(TableWake {
            table: table.into(),
            grouped: spec.column(Role::Group).is_some(),
            first: OnceLock::new(),
        }));
        tables.push(wake);
        wake
    }

    /// The wake-up of the qualified `table`, if a publisher or a subscription of the connection
    /// reached it.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the Postgres listener wakes by channel")
    )]
    pub(crate) fn by_table(&self, table: &str) -> Option<&'static TableWake> {
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|wake| *wake.table == *table)
            .copied()
    }

    /// Every table's wake-up.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the Postgres listener wakes every table")
    )]
    pub(crate) fn tables(&self) -> Vec<&'static TableWake> {
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use futures::FutureExt;
    use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    use tokio::sync::Notify;

    use super::Wakes;

    const GROUPED: TableSpec<'static> =
        TableSpec::new("jobs", Column::new("id"), Form::RowLock).group(Column::new("name"));
    const SINGLE: TableSpec<'static> =
        TableSpec::new("plain", Column::new("id"), Form::RowLock).within("ops");

    /// Whether `notify` holds a wake-up, which this read takes.
    fn woken(notify: &Notify) -> bool {
        notify.notified().now_or_never().is_some()
    }

    #[test]
    fn a_publish_wakes_only_its_groups_subscription() {
        let wakes = Wakes::default();
        let table = wakes.table(&GROUPED);
        let (a, b) = (table.subscribe("a"), table.subscribe("b"));
        table.wake("b");
        assert!(!woken(a), "`a` sleeps on");
        assert!(woken(b), "`b` claims");
        assert!(!woken(b), "one write, one wake-up");
        table.wake("c");
        assert!(!woken(a) && !woken(b), "a group nobody reads wakes nobody");
    }

    #[test]
    fn a_table_without_groups_is_one_queue_whatever_the_name() {
        let wakes = Wakes::default();
        let table = wakes.table(&SINGLE);
        let queue = table.subscribe("plain");
        assert!(ptr::eq(queue, table.subscribe("another")));
        table.wake("anything");
        assert!(woken(queue));
    }

    #[test]
    fn a_group_reopened_keeps_its_wake_up_and_every_struct_of_a_table_shares_one() {
        let wakes = Wakes::default();
        let table = wakes.table(&GROUPED);
        assert!(ptr::eq(
            table,
            wakes.table(&GROUPED.attempt(Column::new("attempt")))
        ));
        assert!(ptr::eq(table.subscribe("a"), table.subscribe("a")));
        assert!(!ptr::eq(table, wakes.table(&SINGLE)));
    }

    #[test]
    fn the_listener_finds_a_table_by_name_and_wakes_all_its_groups() {
        let wakes = Wakes::default();
        let grouped = wakes.table(&GROUPED);
        let single = wakes.table(&SINGLE);
        assert!(
            wakes
                .by_table("ops.plain")
                .is_some_and(|found| ptr::eq(found, single))
        );
        assert!(
            wakes.by_table("plain").is_none(),
            "the name is qualified with its schema"
        );
        assert_eq!(grouped.table(), "jobs");
        assert_eq!(wakes.tables().len(), 2);
        let (a, b) = (grouped.subscribe("a"), grouped.subscribe("b"));
        grouped.wake_all();
        assert!(woken(a) && woken(b));
    }
}
