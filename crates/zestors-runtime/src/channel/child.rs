use crate::_prelude::*;
use futures::FutureExt as _;
use std::{fmt::Debug, task::Poll, time::Duration};

pub struct Child<T = (), R: ChannelSpec = Set!()> {
    join: Option<tokio::task::JoinHandle<Result<T, Report>>>,
    attached: bool,
    channel: Channel<R>,
}

impl<T, R: ChannelSpec> Child<T, R> {
    pub(crate) fn new(
        join: tokio::task::JoinHandle<Result<T, Report>>,
        channel: Channel<R>,
    ) -> Self {
        Self {
            join: Some(join),
            attached: true,
            channel,
        }
    }

    pub fn abort(&self) {
        self.handle().abort();
    }

    pub fn is_finished(&self) -> bool {
        self.handle().is_finished()
    }

    pub fn into_join_handle(mut self) -> tokio::task::JoinHandle<Result<T, Report>> {
        self.join.take().unwrap()
    }

    pub fn channel(&self) -> &Channel<R> {
        &self.channel
    }

    pub fn into_parts(mut self) -> (tokio::task::JoinHandle<Result<T, Report>>, Channel<R>) {
        (self.join.take().unwrap(), self.channel.clone())
    }

    pub fn handle(&self) -> &tokio::task::JoinHandle<Result<T, Report>> {
        self.join.as_ref().unwrap()
    }

    pub fn attached(mut self) -> Self {
        self.attached = true;
        self
    }

    pub fn detached(mut self) -> Self {
        self.attached = false;
        self
    }

    pub fn attach(&mut self) {
        self.attached = true;
    }

    pub fn detach(&mut self) {
        self.attached = false;
    }

    pub fn is_attached(&self) -> bool {
        self.attached
    }

    pub async fn shutdown_abort(mut self, timeout: Duration) -> Result<T, JoinAbortError> {
        self.channel.signal_shutdown();

        let sleep = tokio::time::sleep(timeout);

        tokio::select! {
            biased;

            exit_result = &mut self => {
                return exit_result.map_err(|err| err.into_aborted(false, timeout));
            }

            _ = sleep => {
                tracing::warn!("Child did not exit within timeout. Aborting child.");
            }
        };

        self.abort();
        self.await.map_err(|err| err.into_aborted(true, timeout))
    }
}

impl<T: Send, R: ChannelSpec> AsActorRef for Child<T, R> {
    type ChannelSpec = R;

    fn channel_data(&self) -> &ChannelData<Self::ChannelSpec> {
        self.channel.channel_data()
    }
}

impl<T: Send, R: ChannelSpec> IntoDyn for Child<T, R> {
    type Ref<S: ChannelSpec> = Child<T, S>;

    fn into_dyn_unchecked<S>(self) -> Self::Ref<S>
    where
        S: ChannelSpec,
    {
        let (handle, address) = self.into_parts();
        Child::new(handle, address.into_dyn_unchecked())
    }
}

impl<T, R: ChannelSpec> Future for Child<T, R> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        self.join
            .as_mut()
            .unwrap()
            .poll_unpin(cx)
            .map(|res| match res {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(err)) => Err(JoinError::UnhandledError(err)),
                Err(join_err) => Err(join_err.into()),
            })
    }
}

impl<T, R: ChannelSpec> Drop for Child<T, R> {
    fn drop(&mut self) {
        if self.attached && self.join.is_some() {
            self.abort();
        }
    }
}

impl<T, R: ChannelSpec> Debug for Child<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Child")
            .field("handle", &std::any::type_name::<T>())
            .field("attached", &self.attached)
            .field("address", &self.channel)
            .finish()
    }
}
