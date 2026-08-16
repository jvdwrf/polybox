use thiserror::Error;

use crate::_prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
#[error("Failed to spawn process: {0}")]
pub enum SpawnError {
    #[error("There is already an active process running on this channel.")]
    ActiveProcess,
}

#[derive(Debug)]
pub struct EventStream<T: Interface> {
    channel: Channel<T>,
    initializing: bool,
}

impl<T: Interface> EventStream<T> {
    pub(crate) fn try_new(channel: Channel<T>) -> Result<Self, SpawnError> {
        if !channel.status().is_dead() {
            return Err(SpawnError::ActiveProcess);
        }

        channel.set_status(ActorStatus::Initializing);
        channel.add_spawned_now();

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
            self.channel.set_status(ActorStatus::Running);
        }

        self.channel.next().await
    }
}

impl<T: Interface> AsActorRef for EventStream<T> {
    type QueueType = T;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}
