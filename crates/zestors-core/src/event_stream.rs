use crate::_prelude::*;

#[derive(Debug)]
pub struct EventStream<T: Interface> {
    channel: Channel<T>,
}

impl<T: Interface> EventStream<T> {
    pub(crate) fn new(channel: Channel<T>) -> Self {
        Self { channel }
    }

    pub async fn next(&mut self) -> Option<Event<T>> {
        self.channel.recv().await
    }
}

impl<T: Interface> AsActorRef for EventStream<T> {
    type QueueType = T;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}
