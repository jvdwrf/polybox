use type_sets::TypeSet;

use super::*;

pub trait Context: 'static {
    type Set: TypeSet + 'static;
}

impl<I: Interface> Context for I {
    type Set = I::Set;
}

impl<S: TypeSet + 'static> Context for Set<S> {
    type Set = S;
}
