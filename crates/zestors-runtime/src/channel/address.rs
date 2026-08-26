use crate::_prelude::*;
use std::{fmt::Debug, hash::Hash};

#[repr(transparent)]
pub struct Address<C: Context = Set!()> {
    pub(super) channel: ActorHandle<C>,
}

impl<C: Context> Address<C> {
    pub(super) fn new(channel: &ActorHandle<C>) -> Self {
        Self {
            channel: channel.clone_ref(),
        }
    }

    pub(super) fn new_ref(channel: &ActorHandle<C>) -> &Self {
        unsafe { std::mem::transmute::<&ActorHandle<C>, &Self>(channel) }
    }
}

impl<C: Context> AsActorHandle for Address<C> {
    type Ctx = C;

    fn handle(&self) -> &ActorHandle<Self::Ctx> {
        &self.channel
    }
}

impl<C: Context> IntoDyn for Address<C> {
    type Ref<R: Context> = Address<R>;

    fn into_dyn_unchecked<S>(self) -> Address<S>
    where
        S: Context,
    {
        Address {
            channel: unsafe { std::mem::transmute::<ActorHandle<C>, ActorHandle<S>>(self.channel) },
        }
    }
}

impl<T: Context> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone_ref(),
        }
    }
}

impl<T: Context> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("channel", &self.channel)
            .finish()
    }
}
impl<T: Context> PartialEq for Address<T> {
    fn eq(&self, other: &Self) -> bool {
        self.channel == other.channel
    }
}
impl<T: Context> Eq for Address<T> {}
impl<T: Context> Hash for Address<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.channel.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Interface)]
    #[interface(path = "crate")]
    pub enum MyInterface {
        A(Envelope<u32>),
    }

    #[tokio::test]
    async fn test_address_downcast_ref() {
        let child = crate::spawn(|_: Inbox<MyInterface>| async move { Ok(()) });
        let address = child.address().clone().into_dyn::<Set![]>();

        address
            .downcast::<MyInterface>()
            .expect("Should downcast to MyInterface");
    }
}
