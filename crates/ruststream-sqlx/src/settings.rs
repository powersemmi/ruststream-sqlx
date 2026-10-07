//! The machinery of a table's settings as types, shared by the inbox's and the outbox's typed
//! builders: a setting is [`Unset`] or [`Set`], two declarations fold slot by slot through
//! [`Merge`], and a typed setter appends its marker with [`Push`].

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
    note = "a table sets each setting once: drop one of the two setter calls on its `InboxSpec` or \
            `OutboxSpec`, and the marker from `type Table`"
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

/// Appends a marker to a tuple of markers: what a typed setter of a table's spec does to its
/// settings.
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
