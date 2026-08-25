use super::*;
use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tracing::Instrument as _;

impl<T: ChannelSpec> Channel<T> {
    pub fn spawn<R, F>(
        self,
        spawn_fn: impl FnOnce(Inbox<T>) -> F,
    ) -> Result<Child<R, T>, ConcurrentInboxError>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, Report>> + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn({
            let this = self.clone();
            let stream = Inbox::try_new(this.clone())?;
            let span = tracing::debug_span!("process", pid = %this.pid());
            let future = AssertUnwindSafe(spawn_fn(stream)).catch_unwind();
            this.channel_data().register_spawn();

            async move {
                let mut bomb = AbortBomb::new(&this);

                let future_result = future.await;

                this.channel_data().drain_messages_and_signals();

                let mapped_result = match future_result {
                    Ok(val) => {
                        match &val {
                            Ok(_) => this.channel_data().register_exit(Ok(())),
                            Err(_) => this
                                .channel_data()
                                .register_exit(Err(ExitError::UnhandledError)),
                        };
                        val
                    }
                    Err(boxed) => {
                        this.channel_data().register_exit(Err(ExitError::Panicked));
                        std::panic::resume_unwind(boxed);
                    }
                };

                bomb.defuse();

                mapped_result
            }
            .instrument(span)
        });

        Ok(Child::new(handle, Address::new(self.data)))
    }
}

struct AbortBomb<'a, T: ChannelSpec> {
    channel: &'a Channel<T>,
    armed: bool,
}

impl<'a, T: ChannelSpec> AbortBomb<'a, T> {
    fn new(channel: &'a Channel<T>) -> Self {
        Self {
            channel,
            armed: true,
        }
    }

    fn defuse(&mut self) {
        self.armed = false;
    }
}

impl<'a, T: ChannelSpec> Drop for AbortBomb<'a, T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        tracing::debug!("AbortBomb triggered");

        if !self.channel.status().is_dead() {
            self.channel
                .channel_data()
                .register_exit(Err(ExitError::Aborted));
        }
    }
}
