use std::panic::AssertUnwindSafe;

use futures::FutureExt as _;

use super::*;

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
        let stream = EventStream::try_new(self.clone())?;

        let spawned_future = AssertUnwindSafe(spawn_fn(stream)).catch_unwind();
        let this = self.clone();

        let handle = tokio::spawn(async move {
            tracing::debug!(pid = ?self.pid(), "Process started");

            match spawned_future.await {
                Ok(val) => {
                    match &val {
                        Ok(_) => self.update_exit_result(Ok(())),
                        Err(_) => self.update_exit_result(Err(ExitError::UnhandledError)),
                    };
                    val
                }
                Err(boxed) => {
                    self.update_exit_result(Err(ExitError::Panic));
                    std::panic::resume_unwind(boxed);
                }
            }
        });

        Ok(Child::new(handle, Address::new(this)))
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
