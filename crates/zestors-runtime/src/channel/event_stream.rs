use crate::_prelude::*;

#[derive(Debug)]
pub struct EventStream<T: Interface> {
    channel: Channel<T>,
    initializing: bool,
}

impl<T: Interface> EventStream<T> {
    pub(crate) fn try_new(channel: Channel<T>) -> Result<Self, SpawnError> {
        if !channel.status().is_dead() {
            return Err(SpawnError::DoubleSpawn);
        }

        Ok(Self {
            channel,
            initializing: true,
        })
    }

    pub async fn next(&mut self) -> Option<Event<T>> {
        // If this is the first call to next(), set the status to Running
        if self.initializing {
            debug_assert!(self.status().is_initializing());
            self.initializing = false;
            self.channel.register_initialized();
        }

        self.channel.next().await
    }

    pub async fn next_signal(&mut self) -> Option<SignalEvent> {
        self.channel.recv_signal().await
    }
}

impl<T: Interface> AsActorRef for EventStream<T> {
    type QueueType = T;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}
