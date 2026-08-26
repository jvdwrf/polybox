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
        let span = tracing::debug_span!("process", pid = %self.pid());

        let tokio_handle = tokio::spawn({
            let inbox = Inbox::try_new(self.clone())?;
            let address = inbox.address().clone();
            let mut bomb = AbortBomb::new(address);
            bomb.address.channel_data().register_spawn();
            let spawn_future = AssertUnwindSafe(spawn_fn(inbox)).catch_unwind();

            async move {
                let spawn_result = spawn_future.await;

                let mapped_result = match spawn_result {
                    Ok(result) => {
                        match &result {
                            Ok(_) => bomb.address.channel_data().register_exit(Ok(())),
                            Err(_) => bomb
                                .address
                                .channel_data()
                                .register_exit(Err(ExitError::UnhandledError)),
                        };

                        result
                    }

                    Err(boxed) => {
                        bomb.address
                            .channel_data()
                            .register_exit(Err(ExitError::Panicked));
                        std::panic::resume_unwind(boxed);
                    }
                };

                bomb.defuse();

                mapped_result
            }
            .instrument(span)
        });

        Ok(Child::new(tokio_handle, self))
    }
}

struct AbortBomb<T: ChannelSpec> {
    address: Address<T>,
    armed: bool,
}

impl<T: ChannelSpec> AbortBomb<T> {
    fn new(address: Address<T>) -> Self {
        Self {
            address,
            armed: true,
        }
    }

    fn defuse(&mut self) {
        self.armed = false;
    }
}

impl<T: ChannelSpec> Drop for AbortBomb<T> {
    fn drop(&mut self) {
        if self.armed {
            tracing::debug!("AbortBomb triggered");

            if !self.address.status().is_dead() {
                self.address
                    .channel_data()
                    .register_exit(Err(ExitError::Aborted));
            }
        }
    }
}
