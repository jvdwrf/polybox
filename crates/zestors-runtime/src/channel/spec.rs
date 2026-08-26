use type_sets::TypeSet;

use super::*;

pub trait ChannelSpec: 'static {
    type Set: TypeSet + 'static;
}

impl<I: Interface> ChannelSpec for I {
    type Set = I::Set;
}

impl<S: TypeSet + 'static> ChannelSpec for Set<S> {
    type Set = S;
}
