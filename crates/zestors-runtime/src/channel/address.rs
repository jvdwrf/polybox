use crate::_prelude::*;
use std::{fmt::Debug, hash::Hash, sync::Arc};

#[repr(transparent)]
pub struct Address<C: ChannelSpec = Set!()> {
    pub(super) channel: Arc<Channel<C>>,
}

impl<C: ChannelSpec> Address<C> {
    pub(super) fn new(channel: Arc<Channel<C>>) -> Self {
        Self { channel }
    }

    pub(super) fn from_ref(channel: &ChannelHandle<C>) -> &Self {
        // SAFETY: This is safe because Address<T> is a transparent wrapper around Channel<T>
        unsafe { &*(channel as *const ChannelHandle<C> as *const Self) }
    }
}

impl<C: ChannelSpec> AsActorRef for Address<C> {
    type ChannelSpec = C;

    fn channel_data(&self) -> &Channel<Self::ChannelSpec> {
        &self.channel
    }

    fn get_address(&self) -> Address<Self::ChannelSpec> {
        self.clone()
    }
}

impl<C: ChannelSpec> IntoDyn for Address<C> {
    type Ref<R: ChannelSpec> = Address<R>;

    fn into_dyn_unchecked<S>(self) -> Address<S>
    where
        S: ChannelSpec,
    {
        Address {
            channel: Channel::arc_into_dyn_unchecked(self.channel),
        }
    }
}

impl<T: ChannelSpec> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T: ChannelSpec> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("channel", &self.channel)
            .finish()
    }
}
impl<T: ChannelSpec> PartialEq for Address<T> {
    fn eq(&self, other: &Self) -> bool {
        self.channel == other.channel
    }
}
impl<T: ChannelSpec> Eq for Address<T> {}
impl<T: ChannelSpec> Hash for Address<T> {
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
