//! The settings of a table as one type: each marker sets one slot, a tuple folds its markers slot
//! by slot, and a slot set twice has no fold.

use std::marker::PhantomData;

/// A setting a table leaves at its default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Unset;

/// A setting a table sets, with the marker that sets it.
#[derive(Debug)]
pub struct Set<Value>(PhantomData<fn() -> Value>);

/// Folds one setting of two declarations into one; a setting set by both has no fold.
#[diagnostic::on_unimplemented(
    message = "a table sets this setting twice: `{Self}` and `{Other}`",
    label = "set twice",
    note = "a table has one form, one message mode, one clock and one opening, and sets each role \
            and each event of its own once: drop one of the two setter calls on `InboxSpec`, and \
            its marker from `type Table`"
)]
pub trait Merge<Other> {
    /// The setting the two declare together.
    type Out;
}

impl<Other> Merge<Other> for Unset {
    type Out = Other;
}

impl<Value> Merge<Unset> for Set<Value> {
    type Out = Self;
}

/// The settings of a table, one associated type per setting: [`Unset`] where the table keeps the
/// default, [`Set`] with the marker that sets it otherwise.
///
/// Each marker of [`spec`](crate::spec) is one, and so is a tuple of them.
pub trait Declaration {
    /// The form: [`Lease`](super::Lease) or [`Advisory`](super::Advisory); unset for the row lock
    /// form.
    type Form;
    /// The message mode: [`Payload`](super::Payload); unset for row mode.
    type Message;
    /// The partition key: [`Key`](super::Key).
    type Key;
    /// The attempt count: [`Attempt`](super::Attempt).
    type Attempt;
    /// The delivery's headers: [`Headers`](super::Headers) or
    /// [`HeaderFields`](super::HeaderFields).
    type Headers;
    /// The time a retried row is due: [`RetryAfter`](super::RetryAfter).
    type RetryAfter;
    /// The time a row was processed: [`ProcessedAt`](super::ProcessedAt).
    type ProcessedAt;
    /// The clock: [`Clock`](super::Clock); unset for [`SystemClock`](crate::SystemClock).
    type Clock;
    /// What the table's transactions open at: [`Opens`](super::Opens); unset for the database's
    /// default.
    type Opening;
    /// Groups kept in order: [`Fifo`](super::Fifo).
    type Fifo;
    /// The service's own claim: [`own::Claim`](super::own::Claim).
    type OwnClaim;
    /// The service's own fetch: [`own::Fetch`](super::own::Fetch).
    type OwnFetch;
    /// The service's own ack: [`own::Ack`](super::own::Ack).
    type OwnAck;
    /// The service's own retry: [`own::Retry`](super::own::Retry).
    type OwnRetry;
    /// The service's own delayed retry: [`own::RetryAfter`](super::own::RetryAfter).
    type OwnRetryAfter;
    /// The service's own discard: [`own::Discard`](super::own::Discard).
    type OwnDiscard;
    /// The service's own dead letter: [`own::DeadLetter`](super::own::DeadLetter).
    type OwnDeadLetter;
    /// The service's own lease extension: [`own::Extend`](super::own::Extend).
    type OwnExtend;
    /// The service's own lock: [`own::Lock`](super::own::Lock).
    type OwnLock;
    /// The service's own unlock: [`own::Unlock`](super::own::Unlock).
    type OwnUnlock;
}

/// One marker: it sets its own slot to itself and leaves every other slot unset.
macro_rules! setting {
    ($(#[$doc:meta])* $name:ident $(<$param:ident>)?, $slot:ident) => {
        $(#[$doc])*
        #[derive(Debug)]
        pub struct $name$(<$param>(PhantomData<fn() -> $param>))?;

        impl$(<$param>)? Declaration for $name$(<$param>)? {
            setting!(@slots $slot, Set<Self>;
                Form Message Key Attempt Headers RetryAfter ProcessedAt Clock Opening Fifo
                OwnClaim OwnFetch OwnAck OwnRetry OwnRetryAfter OwnDiscard OwnDeadLetter
                OwnExtend OwnLock OwnUnlock);
        }
    };
    (@slots $slot:ident, $value:ty; $($each:ident)*) => {
        $(setting!(@slot $slot, $value; $each);)*
    };
    (@slot Form, $value:ty; Form) => { type Form = $value; };
    (@slot Message, $value:ty; Message) => { type Message = $value; };
    (@slot Key, $value:ty; Key) => { type Key = $value; };
    (@slot Attempt, $value:ty; Attempt) => { type Attempt = $value; };
    (@slot Headers, $value:ty; Headers) => { type Headers = $value; };
    (@slot RetryAfter, $value:ty; RetryAfter) => { type RetryAfter = $value; };
    (@slot ProcessedAt, $value:ty; ProcessedAt) => { type ProcessedAt = $value; };
    (@slot Clock, $value:ty; Clock) => { type Clock = $value; };
    (@slot Opening, $value:ty; Opening) => { type Opening = $value; };
    (@slot Fifo, $value:ty; Fifo) => { type Fifo = $value; };
    (@slot OwnClaim, $value:ty; OwnClaim) => { type OwnClaim = $value; };
    (@slot OwnFetch, $value:ty; OwnFetch) => { type OwnFetch = $value; };
    (@slot OwnAck, $value:ty; OwnAck) => { type OwnAck = $value; };
    (@slot OwnRetry, $value:ty; OwnRetry) => { type OwnRetry = $value; };
    (@slot OwnRetryAfter, $value:ty; OwnRetryAfter) => { type OwnRetryAfter = $value; };
    (@slot OwnDiscard, $value:ty; OwnDiscard) => { type OwnDiscard = $value; };
    (@slot OwnDeadLetter, $value:ty; OwnDeadLetter) => { type OwnDeadLetter = $value; };
    (@slot OwnExtend, $value:ty; OwnExtend) => { type OwnExtend = $value; };
    (@slot OwnLock, $value:ty; OwnLock) => { type OwnLock = $value; };
    (@slot OwnUnlock, $value:ty; OwnUnlock) => { type OwnUnlock = $value; };
    (@slot $slot:ident, $value:ty; $other:ident) => { type $other = Unset; };
}

pub(super) use setting;

impl Declaration for () {
    type Form = Unset;
    type Message = Unset;
    type Key = Unset;
    type Attempt = Unset;
    type Headers = Unset;
    type RetryAfter = Unset;
    type ProcessedAt = Unset;
    type Clock = Unset;
    type Opening = Unset;
    type Fifo = Unset;
    type OwnClaim = Unset;
    type OwnFetch = Unset;
    type OwnAck = Unset;
    type OwnRetry = Unset;
    type OwnRetryAfter = Unset;
    type OwnDiscard = Unset;
    type OwnDeadLetter = Unset;
    type OwnExtend = Unset;
    type OwnLock = Unset;
    type OwnUnlock = Unset;
}

/// A tuple of markers folds its head into the declaration of its tail, slot by slot.
macro_rules! tuple {
    ($head:ident $(, $tail:ident)*) => {
        tuple!(@impl [$head $(, $tail)*] $head, ($($tail,)*);
            Form Message Key Attempt Headers RetryAfter ProcessedAt Clock Opening Fifo
            OwnClaim OwnFetch OwnAck OwnRetry OwnRetryAfter OwnDiscard OwnDeadLetter
            OwnExtend OwnLock OwnUnlock);
    };
    (@impl [$($each:ident),*] $head:ident, $tail:ty; $($slot:ident)*) => {
        impl<$($each: Declaration),*> Declaration for ($($each,)*)
        where
            $tail: Declaration,
            $($head::$slot: Merge<<$tail as Declaration>::$slot>,)*
        {
            $(type $slot = <$head::$slot as Merge<<$tail as Declaration>::$slot>>::Out;)*
        }
    };
}

/// Appends a marker to a tuple of markers: what a typed setter of
/// [`InboxSpec`](crate::InboxSpec) does to its settings.
pub trait Push<New> {
    /// The tuple with `New` at its end.
    type Out;
}

impl<New> Push<New> for () {
    type Out = (New,);
}

macro_rules! push {
    ($($each:ident),+) => {
        impl<$($each,)+ New> Push<New> for ($($each,)+) {
            type Out = ($($each,)+ New);
        }
    };
}

tuple!(Setting1);
tuple!(Setting1, Setting2);
tuple!(Setting1, Setting2, Setting3);
tuple!(Setting1, Setting2, Setting3, Setting4);
tuple!(Setting1, Setting2, Setting3, Setting4, Setting5);
tuple!(Setting1, Setting2, Setting3, Setting4, Setting5, Setting6);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17,
    Setting18
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17,
    Setting18, Setting19
);
tuple!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17,
    Setting18, Setting19, Setting20
);
push!(Setting1);
push!(Setting1, Setting2);
push!(Setting1, Setting2, Setting3);
push!(Setting1, Setting2, Setting3, Setting4);
push!(Setting1, Setting2, Setting3, Setting4, Setting5);
push!(Setting1, Setting2, Setting3, Setting4, Setting5, Setting6);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17,
    Setting18
);
push!(
    Setting1, Setting2, Setting3, Setting4, Setting5, Setting6, Setting7, Setting8, Setting9,
    Setting10, Setting11, Setting12, Setting13, Setting14, Setting15, Setting16, Setting17,
    Setting18, Setting19
);
