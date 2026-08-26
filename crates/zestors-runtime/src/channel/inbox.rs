use crate::_prelude::*;

#[derive(Debug)]
pub struct Inbox<T: Interface> {
    channel: StrongAddress<T>,
    initializing: bool,
}

impl<T: Interface> Inbox<T> {
    pub(crate) fn try_new(channel: StrongAddress<T>) -> Result<Self, ConcurrentInboxError> {
        if !channel.status().is_dead() {
            return Err(ConcurrentInboxError);
        }

        let inbox = Self {
            channel,
            initializing: true,
        };

        Ok(inbox)
    }

    pub async fn next(&mut self) -> Option<Event<T>> {
        // If this is the first call to next(), set the status to Running
        if self.initializing {
            debug_assert!(self.status().is_initializing());
            self.initializing = false;
            self.handle().register_initialized();
        }

        self.handle().next().await
    }

    pub async fn next_signal(&mut self) -> Option<Signal> {
        self.handle().recv_signal().await
    }
}

impl<T: Interface> ActorOps for Inbox<T> {
    type Ctx = T;

    fn handle(&self) -> &Channel<Self::Ctx> {
        &self.channel.handle()
    }
}

impl<T: Interface> Drop for Inbox<T> {
    fn drop(&mut self) {
        self.handle().drain_messages_and_signals();
    }
}
