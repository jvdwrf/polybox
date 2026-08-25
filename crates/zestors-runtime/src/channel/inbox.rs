use crate::_prelude::*;

#[derive(Debug)]
pub struct Inbox<T: Interface> {
    channel: Channel<T>,
    initializing: bool,
}

impl<T: Interface> Inbox<T> {
    pub(crate) fn try_new(channel: Channel<T>) -> Result<Self, ConcurrentInboxError> {
        if !channel.status().is_dead() {
            return Err(ConcurrentInboxError);
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
            self.channel_data().register_initialized();
        }

        self.channel_data().next().await
    }

    pub async fn next_signal(&mut self) -> Option<Signal> {
        self.channel_data().recv_signal().await
    }
}

impl<T: Interface> AsActorRef for Inbox<T> {
    type ChannelSpec = T;

    fn channel_data(&self) -> &ChannelData<Self::ChannelSpec> {
        self.channel.channel_data()
    }

    fn get_address(&self) -> Address<Self::ChannelSpec> {
        self.channel.get_address()
    }
}
