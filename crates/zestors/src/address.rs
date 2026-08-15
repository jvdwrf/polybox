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

    pub fn into_dyn_unchecked_test(self) -> Address {
        todo!()
    }

    pub fn status_stream(&self) -> &StatusStream {
        todo!()
    }

    pub fn is_alive(&self) -> bool {
        todo!()
    }

    pub fn is_same_process<R: QueueType>(&self, other: &Address<R>) -> bool {
        todo!()
    }

    pub async fn watch_exit(&mut self) {
        todo!()
    }

    pub async fn watch_start(&mut self) {
        todo!()
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
