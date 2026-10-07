//! The rules across a table's settings, one trait each: a table whose settings break one does
//! not implement it, and its `type Table` fails with that rule's message.

use super::builder::InboxSpec;
use super::declaration::{Declaration, Set, Unset};
use super::{Advisory, Clock, Headers, Lease, Payload, own};
use crate::{Clock as ServiceClock, DatabaseClock, SystemClock};

/// A table's description whose settings keep every rule: what `InboxTable::Table` requires.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe a queue table's settings",
    label = "not a table's description",
    note = "write `type Table = InboxSpec<(..)>`, listing the markers the chain of `TABLE` sets"
)]
pub trait Valid {
    /// The table's settings.
    type Settings: Declaration;
}

impl<Settings> Valid for InboxSpec<Settings>
where
    Settings: Declaration,
    Settings::Clock: ClockSlot,
    (Settings::Form, <Settings::Clock as ClockSlot>::Reads): LeaseOnServiceClock,
    (Settings::Form, Settings::Fifo): FifoOutsideAdvisory,
    (Settings::Form, Settings::OwnClaim): ClaimOutsideAdvisory,
    (Settings::Form, Settings::OwnExtend): ExtendInLease,
    (Settings::Form, Settings::OwnLock): LockInAdvisory,
    (Settings::Form, Settings::OwnUnlock): LockInAdvisory,
    (Settings::OwnLock, Settings::OwnUnlock): LockWithUnlock,
    (Settings::Message, Settings::Headers): PayloadOutsideHeaderFields,
{
    type Settings = Settings;
}

/// Where the clock a table sets reads now: [`SystemClock`] for a clock on the host,
/// [`DatabaseClock`] for the database's.
#[diagnostic::on_unimplemented(
    message = "`{Self}` reads now from neither a `Clock` on the host nor `DatabaseClock`",
    label = "an unknown clock",
    note = "set the clock with `.clock::<Source>()`, where `Source` implements `Clock` or is \
            `DatabaseClock`"
)]
pub trait ClockSlot {
    /// [`SystemClock`] or [`DatabaseClock`].
    type Reads;
}

impl ClockSlot for Unset {
    type Reads = SystemClock;
}

impl<Source: ServiceClock> ClockSlot for Set<Clock<Source>> {
    type Reads = SystemClock;
}

impl ClockSlot for Set<Clock<DatabaseClock>> {
    type Reads = DatabaseClock;
}

/// The lease form computes its expiry from a clock on the host: the form and where the clock
/// reads now.
#[diagnostic::on_unimplemented(
    message = "the lease form computes its expiry from the crate's clock, and this table reads \
               the database's",
    label = "the lease form on `DatabaseClock`",
    note = "drop `.clock::<DatabaseClock>()` and `Clock<DatabaseClock>`, or `.lease(..)` and \
            `Lease<..>` (with the derive: `clock = DatabaseClock`, or the `locked_until` field)"
)]
pub trait LeaseOnServiceClock {}

impl<Reads> LeaseOnServiceClock for (Unset, Reads) {}
impl<Reads> LeaseOnServiceClock for (Set<Advisory>, Reads) {}
impl<Time> LeaseOnServiceClock for (Set<Lease<Time>>, SystemClock) {}

/// The advisory lock form keeps a group in order through its lock key: the form and the FIFO
/// group.
#[diagnostic::on_unimplemented(
    message = "a FIFO group does not combine with the advisory lock form, which keeps a group in \
               order through its lock key",
    label = "a FIFO group in the advisory lock form",
    note = "drop `.fifo_group(..)` and `Fifo`, and put the group's column into the lock key"
)]
pub trait FifoOutsideAdvisory {}

impl<FifoSlot> FifoOutsideAdvisory for (Unset, FifoSlot) {}
impl<Time, FifoSlot> FifoOutsideAdvisory for (Set<Lease<Time>>, FifoSlot) {}
impl FifoOutsideAdvisory for (Set<Advisory>, Unset) {}

/// The advisory lock form selects its candidates with their keys itself: the form and the
/// service's own claim.
#[diagnostic::on_unimplemented(
    message = "the advisory lock form selects its candidates with their keys itself, so it takes \
               no claim of the service's own",
    label = "`own::Claim` in the advisory lock form",
    note = "drop `.own::<own::Claim>()` and `own::Claim` (with the derive: `claim` from \
            `custom(..)`; a message's form is its headers struct's)"
)]
pub trait ClaimOutsideAdvisory {}

impl<ClaimSlot> ClaimOutsideAdvisory for (Unset, ClaimSlot) {}
impl<Time, ClaimSlot> ClaimOutsideAdvisory for (Set<Lease<Time>>, ClaimSlot) {}
impl ClaimOutsideAdvisory for (Set<Advisory>, Unset) {}

/// A lease extension is an event of the lease form: the form and the service's own extend.
#[diagnostic::on_unimplemented(
    message = "`own::Extend` is an event of the lease form",
    label = "`own::Extend` outside the lease form",
    note = "set the lease form with `.lease(..)` and `Lease<..>`, or drop `.own::<own::Extend>()` \
            and `own::Extend` (with the derive: a `locked_until` field, or `extend` from \
            `custom(..)`; a message's form is its headers struct's)"
)]
pub trait ExtendInLease {}

impl<Form> ExtendInLease for (Form, Unset) {}
impl<Time> ExtendInLease for (Set<Lease<Time>>, Set<own::Extend>) {}

/// A lock and an unlock are events of the advisory lock form: the form and the service's own
/// lock or unlock.
#[diagnostic::on_unimplemented(
    message = "`own::Lock` and `own::Unlock` are events of the advisory lock form",
    label = "`own::Lock` or `own::Unlock` outside the advisory lock form",
    note = "set the advisory lock form with `.advisory(..)` and `Advisory`, or drop \
            `.own::<own::Lock>()`, `.own::<own::Unlock>()` and their markers (with the derive: \
            `advisory_lock = \"..\"`, or `lock` and `unlock` from `custom(..)`; a message's form \
            is its headers struct's)"
)]
pub trait LockInAdvisory {}

impl<Form> LockInAdvisory for (Form, Unset) {}
impl<Event> LockInAdvisory for (Set<Advisory>, Set<Event>) {}

/// The service's own unlock releases what its own lock took: the service's own lock and unlock.
#[diagnostic::on_unimplemented(
    message = "the service's own lock is released by its own unlock, so a table sets `own::Lock` \
               and `own::Unlock` together",
    label = "`own::Lock` without `own::Unlock`, or the other way round",
    note = "add the missing one with `.own::<..>()` and its marker in `type Table`, or drop the \
            other"
)]
pub trait LockWithUnlock {}

impl LockWithUnlock for (Unset, Unset) {}
impl LockWithUnlock for (Set<own::Lock>, Set<own::Unlock>) {}

/// A message assembled from header fields is handed to its handler itself: the message mode and
/// the headers layout.
#[diagnostic::on_unimplemented(
    message = "a message assembled from header fields is handed to its handler itself, as in row \
               mode, so it holds no payload",
    label = "`Payload` beside `HeaderFields`",
    note = "drop `.payload(..)` and `Payload`"
)]
pub trait PayloadOutsideHeaderFields {}

impl<Layout> PayloadOutsideHeaderFields for (Unset, Layout) {}
impl PayloadOutsideHeaderFields for (Set<Payload>, Unset) {}
impl PayloadOutsideHeaderFields for (Set<Payload>, Set<Headers>) {}
