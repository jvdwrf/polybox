use std::{any::TypeId, fmt::Debug, marker::PhantomData};

pub struct Set<T>(PhantomData<fn() -> T>);

impl<T> Debug for Set<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Set<{}>", std::any::type_name::<T>())
    }
}

/// Indicates that a type contains member `E` in its set.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not contain `{E}`",
    label = "type does not contain this element",
    note = "a type implements `Contains<E>` when `E` is one of its set members"
)]
pub trait Contains<E>: Contains0 {}

#[diagnostic::do_not_recommend]
impl<E, T: ?Sized> Contains<E> for T where T: Contains1<E> {}

/// Indicates that a type is a subset of set `S`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a subset of `{S}`",
    label = "this set is not a subset of the required set",
    note = "every member of the left-hand set must also be present in the right-hand set"
)]
pub trait SubsetOf<S: ?Sized> {}

#[diagnostic::do_not_recommend]
impl<T: TypeSet, R: TypeSet> SubsetOf<R> for T where T::Set: SubsetOf<R::Set> {}

/// Indicates that a type is a superset of set `S`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a superset of `{S}`",
    label = "this set does not contain all required members",
    note = "every member of the right-hand set must also be present in the left-hand set"
)]
pub trait SupersetOf<S: ?Sized> {}

#[diagnostic::do_not_recommend]
impl<S1: ?Sized, S2: ?Sized> SupersetOf<S2> for S1 where S2: SubsetOf<S1> {}
pub trait TypeSet {
    type Set: ?Sized;
    fn members() -> &'static [TypeId]
    where
        Self: 'static;
}

// pub trait AsTypeSet {
//     type Set: TypeSet + 'static;

//     fn members() -> &'static [TypeId]
//     where
//         Self: 'static,
//     {
//         <Self::Set as TypeSet>::members()
//     }

//     fn contains(id: TypeId) -> bool
//     where
//         Self: 'static,
//     {
//         <Self::Set as TypeSet>::members().contains(&id)
//     }

//     fn contains_all(ids: &[TypeId]) -> bool
//     where
//         Self: 'static,
//     {
//         ids.iter()
//             .all(|id| <Self::Set as TypeSet>::members().contains(id))
//     }
// }
// impl<T: TypeSet + 'static> AsTypeSet for T {
//     type Set = T;
// }

#[diagnostic::on_unimplemented(
    message = "`{Self}` and `{R}` do not represent the same set",
    label = "the sets contain different members",
    note = "two sets are equal when every member of one is also a member of the other"
)]
pub trait SetEqual<R: ?Sized> {}

#[diagnostic::do_not_recommend]
impl<T: TypeSet, R: TypeSet> SetEqual<R> for T where T: SubsetOf<R> + SupersetOf<R> {}

mod generate_sets;
use generate_sets::*;

mod set_macro;
