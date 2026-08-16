use crate::_prelude::*;
use std::fmt::Debug;

#[repr(transparent)]
pub struct Address<T: ChannelKind = Set!()> {
    channel: Channel<T>,
}

impl<T: ChannelKind> AsActorRef for Address<T> {
    type QueueType = T;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}

impl<T: ChannelKind> IntoDyn for Address<T> {
    type Ref<R: ChannelKind> = Address<R>;

    fn into_dyn_unchecked<S>(self) -> Address<S>
    where
        S: ChannelKind,
    {
        Address {
            channel: self.channel.into_dyn_unchecked(),
        }
    }
}

impl<T: ChannelKind> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T: ChannelKind> Address<T> {
    pub(super) fn new(channel: Channel<T>) -> Self {
        Self { channel }
    }

    pub(super) fn from_ref(channel: &Channel<T>) -> &Self {
        // SAFETY: This is safe because Address<T> is a transparent wrapper around Channel<T>
        unsafe { &*(channel as *const Channel<T> as *const Self) }
    }
}

impl<T: ChannelKind> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("channel", &self.channel)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Interface)]
    #[interface(crate = "crate")]
    pub enum MyInterface {
        A(Payload<u32>),
    }

    #[tokio::test]
    async fn test_address_downcast_ref() {
        let child = crate::spawn(
            Pid::rand(),
            |_: EventStream<MyInterface>| async move { Ok(()) },
        );
        let address = child.address().clone().into_dyn::<Set![]>();

        address
            .downcast::<MyInterface>()
            .expect("Should downcast to MyInterface");
    }
}
