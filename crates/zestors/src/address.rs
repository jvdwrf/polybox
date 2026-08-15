use crate::_prelude::*;
use polybox::type_sets::Set;
use std::fmt::Debug;

#[repr(transparent)]
pub struct Address<T: QueueType = Set!()> {
    channel: Channel<T>,
}

impl<T: QueueType> AsActorRef for Address<T> {
    type QueueType = T;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}

impl<T: QueueType> IntoDyn for Address<T> {
    type Ref<R: QueueType> = Address<R>;

    fn into_dyn_unchecked<S>(self) -> Address<S>
    where
        S: QueueType,
    {
        Address {
            channel: self.channel.into_dyn_unchecked(),
        }
    }
}

impl<T: QueueType> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T: QueueType> Address<T> {
    pub(super) fn new(channel: Channel<T>) -> Self {
        Self { channel }
    }

    pub(super) fn from_ref(channel: &Channel<T>) -> &Self {
        // SAFETY: This is safe because Address<T> is a transparent wrapper around Channel<T>
        unsafe { &*(channel as *const Channel<T> as *const Self) }
    }
}

impl<T: QueueType> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("channel", &self.channel)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Interface, HandlerInterface)]
    #[interface(crate = "crate")]
    pub enum MyInterface {
        A(Payload<u32>),
    }

    #[tokio::test]
    async fn test_address_downcast_ref() {
        let child = crate::spawn(
            Pid::rand(),
            |_: ActorState<MyInterface>| async move { Ok(()) },
        );
        let address = child.address().clone().into_dyn::<Set![]>();

        address
            .downcast::<MyInterface>()
            .expect("Should downcast to MyInterface");
    }
}
