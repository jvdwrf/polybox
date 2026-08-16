use super::*;
use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;

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
            let future = AssertUnwindSafe(spawn_fn(stream)).catch_unwind();

            async move {
                let _bomb = AbortBomb { channel: &this };
                tracing::debug!(pid = ?this.pid(), "Process started");

                match future.await {
                    Ok(val) => {
                        match &val {
                            Ok(_) => this.update_exit_result(Ok(())),
                            Err(_) => this.update_exit_result(Err(ExitError::UnhandledError)),
                        };
                        val
                    }
                    Err(boxed) => {
                        this.update_exit_result(Err(ExitError::Panic));
                        std::panic::resume_unwind(boxed);
                    }
                }
            }
        });

        Ok(Child::new(handle, Address::new(self)))
    }

    fn update_exit_result(&self, exit_result: Result<(), ExitError>) {
        match &exit_result {
            Ok(()) => tracing::debug!(pid = ?self.pid(), "Process exited normally"),
            Err(err) => tracing::error!(pid = ?self.pid(), "Process exited with error: {:?}", err),
        }

        self.add_exited_now(exit_result.clone());
        self.set_status(ActorStatus::Dead(exit_result.err()));
    }
}

/// Makes sure that the channel is marked as exited with an `Abort` error
/// if the spawned task is aborted before it finishes executing.
struct AbortBomb<'a, T: ChannelKind> {
    channel: &'a Channel<T>,
}

impl<'a, T: ChannelKind> Drop for AbortBomb<'a, T> {
    fn drop(&mut self) {
        if !self.channel.status().is_dead() {
            self.channel.update_exit_result(Err(ExitError::Abort));
        }
    }
}
