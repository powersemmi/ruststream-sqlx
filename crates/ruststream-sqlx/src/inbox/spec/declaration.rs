//! The settings of a table as one type: each marker sets one slot, a tuple folds its markers slot
//! by slot, and a slot set twice has no fold.

pub use crate::settings::{Merge, Push, Set, Unset};

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
