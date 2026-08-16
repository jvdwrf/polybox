use super::*;
use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tracing::Instrument as _;

impl<T: ChannelKind> Channel<T> {
    pub fn spawn<R, F>(
        self,
        spawn_fn: impl FnOnce(EventStream<T>) -> F,
    ) -> Result<Child<R, T>, SpawnError>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, Report>> + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::spawn({
            let this = self.clone();
            let stream = EventStream::try_new(this.clone())?;
            let span = tracing::debug_span!("process", pid = %this.pid());
            let future = AssertUnwindSafe(spawn_fn(stream)).catch_unwind();

            async move {
                this.register_spawn();
                let bomb = AbortBomb { channel: &this };

                let res = match future.await {
                    Ok(val) => {
                        match &val {
                            Ok(_) => this.register_exit(Ok(())),
                            Err(_) => this.register_exit(Err(ExitError::UnhandledError)),
                        };
                        val
                    }
                    Err(boxed) => {
                        this.register_exit(Err(ExitError::Panic));
                        std::panic::resume_unwind(boxed);
                    }
                };

                bomb.defuse();

                res
            }
            .instrument(span)
        });

        Ok(Child::new(handle, Address::new(self)))
    }
}

/// Makes sure that the channel is marked as exited with an `Abort` error
/// if the spawned task is aborted before it finishes executing.
struct AbortBomb<'a, T: ChannelKind> {
    channel: &'a Channel<T>,
}

impl<'a, T: ChannelKind> Drop for AbortBomb<'a, T> {
    fn drop(&mut self) {
        tracing::debug!("AbortBomb triggered for channel {}", self.channel.pid());
        if !self.channel.status().is_dead() {
            self.channel.register_exit(Err(ExitError::Abort));
        }
    }
}

impl<'a, T: ChannelKind> AbortBomb<'a, T> {
    fn defuse(self) -> &'a Channel<T> {
        let Self { channel } = self;
        channel
    }
}
