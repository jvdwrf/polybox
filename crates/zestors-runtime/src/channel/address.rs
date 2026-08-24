use crate::_prelude::*;
use std::{fmt::Debug, hash::Hash};

#[repr(transparent)]
pub struct Address<T: ChannelSpec = Set!()> {
    channel: Channel<T>,
}

impl<T: ChannelSpec> AsActorRef for Address<T> {
    type ChannelSpec = T;

    fn as_channel(&self) -> &Channel<Self::ChannelSpec> {
        &self.channel
    }
}

impl<T: ChannelSpec> IntoDyn for Address<T> {
    type Ref<R: ChannelSpec> = Address<R>;

    fn into_dyn_unchecked<S>(self) -> Address<S>
    where
        S: ChannelSpec,
    {
        Address {
            channel: self.channel.into_dyn_unchecked(),
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

impl<T: ChannelSpec> Address<T> {
    pub(super) fn new(channel: Channel<T>) -> Self {
        Self { channel }
    }

    pub(super) fn from_ref(channel: &Channel<T>) -> &Self {
        // SAFETY: This is safe because Address<T> is a transparent wrapper around Channel<T>
        unsafe { &*(channel as *const Channel<T> as *const Self) }
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
        let child = crate::spawn(Pid::rand(), |_: Inbox<MyInterface>| async move { Ok(()) });
        let address = child.address().clone().into_dyn::<Set![]>();

        address
            .downcast::<MyInterface>()
            .expect("Should downcast to MyInterface");
    }
}
