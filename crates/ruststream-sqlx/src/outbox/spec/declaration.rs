//! The settings of an outbox table as one type: each marker sets one slot, a tuple folds its
//! markers slot by slot, and a slot set twice has no fold.

#[cfg(doc)]
use super::Set;
use super::{Merge, Unset};

/// The settings of an outbox table, one associated type per setting: [`Unset`] where the table
/// keeps the default, [`Set`] with the marker that sets it otherwise.
///
/// Each marker of [`outbox::spec`](super) is one, and so is a tuple of them.
pub trait Declaration {
    /// The headers column: [`Headers`](super::Headers).
    type Headers;
    /// The time a record was processed: [`ProcessedAt`](super::ProcessedAt).
    type ProcessedAt;
    /// The record's own fetch: [`own::Fetch`](super::own::Fetch).
    type OwnFetch;
    /// The record's own ack: [`own::Ack`](super::own::Ack).
    type OwnAck;
    /// The record's own retry: [`own::Retry`](super::own::Retry).
    type OwnRetry;
    /// The record's own discard: [`own::Discard`](super::own::Discard).
    type OwnDiscard;
    /// The record's own recovery: [`own::Recover`](super::own::Recover).
    type OwnRecover;
}

/// One marker: it sets its own slot to itself and leaves every other slot unset.
macro_rules! setting {
    ($(#[$doc:meta])* $name:ident, $slot:ident) => {
        $(#[$doc])*
        #[derive(Debug)]
        pub struct $name;

        impl Declaration for $name {
            setting!(@slots $slot, Set<Self>;
                Headers ProcessedAt OwnFetch OwnAck OwnRetry OwnDiscard OwnRecover);
        }
    };
    (@slots $slot:ident, $value:ty; $($each:ident)*) => {
        $(setting!(@slot $slot, $value; $each);)*
    };
    (@slot Headers, $value:ty; Headers) => { type Headers = $value; };
    (@slot ProcessedAt, $value:ty; ProcessedAt) => { type ProcessedAt = $value; };
    (@slot OwnFetch, $value:ty; OwnFetch) => { type OwnFetch = $value; };
    (@slot OwnAck, $value:ty; OwnAck) => { type OwnAck = $value; };
    (@slot OwnRetry, $value:ty; OwnRetry) => { type OwnRetry = $value; };
    (@slot OwnDiscard, $value:ty; OwnDiscard) => { type OwnDiscard = $value; };
    (@slot OwnRecover, $value:ty; OwnRecover) => { type OwnRecover = $value; };
    (@slot $slot:ident, $value:ty; $other:ident) => { type $other = Unset; };
}

pub(super) use setting;

impl Declaration for () {
    type Headers = Unset;
    type ProcessedAt = Unset;
    type OwnFetch = Unset;
    type OwnAck = Unset;
    type OwnRetry = Unset;
    type OwnDiscard = Unset;
    type OwnRecover = Unset;
}

/// A tuple of markers folds its head into the declaration of its tail, slot by slot.
macro_rules! tuple {
    ($head:ident $(, $tail:ident)*) => {
        tuple!(@impl [$head $(, $tail)*] $head, ($($tail,)*);
            Headers ProcessedAt OwnFetch OwnAck OwnRetry OwnDiscard OwnRecover);
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

// One marker per setting, and a tuple one longer than the settings, so a setting given twice
// reaches the fold's message instead of a missing impl.
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
