use super::*;
use type_sets::{SubsetOf, TypeSet};

pub trait IntoDyn: ActorRef + Sized {
    type Ref<T: ChannelSpec>;

    fn into_dyn_unchecked<S>(self) -> Self::Ref<S>
    where
        S: ChannelSpec;

    fn into_dyn<S>(self) -> Self::Ref<S>
    where
        S: ChannelSpec + SubsetOf<Self::Set>,
    {
        self.into_dyn_unchecked()
    }

    fn into_dyn_checked<S>(self) -> Result<Self::Ref<S>, Self>
    where
        S: TypeSet + ChannelSpec,
    {
        if self.is_superset_of(S::members()) {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }

    fn downcast<I>(self) -> Result<Self::Ref<I>, Self>
    where
        I: Interface,
    {
        if self.is_interface::<I>() {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }
}

pub trait AsDyn: IntoDyn {
    fn as_dyn_unchecked<S>(&self) -> &Self::Ref<S>
    where
        S: ChannelSpec;

    fn as_dyn<S>(&self) -> &Self::Ref<S>
    where
        S: ChannelSpec + SubsetOf<Self::Set>,
    {
        self.as_dyn_unchecked()
    }

    fn as_dyn_checked<S>(&self) -> Option<&Self::Ref<S>>
    where
        S: TypeSet + ChannelSpec,
    {
        if self.is_superset_of(S::members()) {
            Some(self.as_dyn_unchecked())
        } else {
            None
        }
    }

    fn downcast_ref<I>(&self) -> Option<&Self::Ref<I>>
    where
        I: Interface,
    {
        if self.is_interface::<I>() {
            Some(self.as_dyn_unchecked())
        } else {
            None
        }
    }
}
