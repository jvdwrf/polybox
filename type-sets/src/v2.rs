use std::any::TypeId;

#[diagnostic::on_unimplemented(
    message = "`{Self}` does not contain `{E}`",
    label = "type does not contain this element",
    note = "a type implements `Contains<E>` when `E` is one of its set members"
)]
pub trait Contains<E>: Contains0 {}

#[diagnostic::do_not_recommend]
impl<E, T: ?Sized> Contains<E> for T where T: Contains1<E> {}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a subset of `{S}`",
    label = "this set is not a subset of the required set",
    note = "every member of the left-hand set must also be present in the right-hand set"
)]
pub trait SubsetOf<S: ?Sized> {}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a superset of `{S}`",
    label = "this set does not contain all required members",
    note = "every member of the right-hand set must also be present in the left-hand set"
)]
pub trait SupersetOf<S: ?Sized> {}

#[diagnostic::do_not_recommend]
impl<T: AsSet, R: AsSet> SubsetOf<R> for T where T::Set: SubsetOf<R::Set> {}

#[diagnostic::do_not_recommend]
impl<S1: ?Sized, S2: ?Sized> SupersetOf<S2> for S1 where S2: SubsetOf<S1> {}
pub trait AsSet {
    type Set: ?Sized;
    fn members() -> &'static [TypeId]
    where
        Self: 'static;
}

mod generate_sets;
pub use generate_sets::*;

mod set_macro;
pub use set_macro::*;
