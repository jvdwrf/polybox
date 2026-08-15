#[allow(unused_imports)]
use crate::_prelude::*;
use polybox::type_sets::Set;
use std::{fmt::Debug, task::Poll, time::Duration};

pub struct Child<T = (), R: QueueType = Set!()> {
    handle: Option<tokio::task::JoinHandle<Result<T, Report>>>,
    attached: bool,
    address: Address<R>,
}

impl<T, R: QueueType> Child<T, R> {
    pub(crate) fn new(
        handle: tokio::task::JoinHandle<Result<T, Report>>,
        address: Address<R>,
    ) -> Self {
        Self {
            handle: Some(handle),
            attached: true,
            address,
        }
    }

    pub fn pid(&self) -> &Pid {
        self.address.pid()
    }

    pub fn address(&self) -> &Address<R> {
        &self.address
    }

    pub fn abort(&self) {
        self.handle().abort();
    }

    pub fn is_finished(&self) -> bool {
        self.handle().is_finished()
    }

    pub fn into_handle(mut self) -> tokio::task::JoinHandle<Result<T, Report>> {
        self.handle.take().unwrap()
    }

    pub fn into_parts(mut self) -> (tokio::task::JoinHandle<Result<T, Report>>, Address<R>) {
        (self.handle.take().unwrap(), self.address.clone())
    }

    pub fn handle(&self) -> &tokio::task::JoinHandle<Result<T, Report>> {
        self.handle.as_ref().unwrap()
    }

    pub fn handle_mut(&mut self) -> &mut tokio::task::JoinHandle<Result<T, Report>> {
        self.handle.as_mut().unwrap()
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

    pub async fn shutdown_abort(mut self, timeout: Duration) -> Result<T, JoinError> {
        self.address.signal_shutdown();

        let timeout = tokio::time::sleep(timeout);

        tokio::select! {
            biased;

            exit_result = &mut self => {
                return exit_result;
            }

            _ = timeout => {
                tracing::warn!("Child did not exit within timeout. Aborting child.");
            }
        };

        self.abort();
        self.await
    }
}

impl<T, R: QueueType> AsActorRef for Child<T, R> {
    type QueueType = R;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        self.address.as_channel()
    }
}

impl<T, R: QueueType> IntoDyn for Child<T, R> {
    type Ref<S: QueueType> = Child<T, S>;

    fn into_dyn_unchecked<S>(self) -> Self::Ref<S>
    where
        S: QueueType,
    {
        let (handle, address) = self.into_parts();
        Child::new(handle, address.into_dyn_unchecked())
    }
}

impl<T, R: QueueType> Future for Child<T, R> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        self.handle
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

#[derive(thiserror::Error, Debug)]
pub enum JoinError {
    /// The task panicked.
    #[error("task panicked")]
    Panic,

    /// The task was aborted.
    #[error("task was aborted / cancelled")]
    Aborted,

    /// The actor exited with an unhandled error.
    #[error("task returned an error: {0}")]
    UnhandledError(Report),
}

impl From<tokio::task::JoinError> for JoinError {
    fn from(err: tokio::task::JoinError) -> Self {
        if err.is_cancelled() {
            JoinError::Aborted
        } else if err.is_panic() {
            JoinError::Panic
        } else {
            unreachable!("JoinError is neither cancelled nor panicked: {:?}", err)
        }
    }
}

impl<T, R: QueueType> Drop for Child<T, R> {
    fn drop(&mut self) {
        if self.attached && self.handle.is_some() {
            self.abort();
        }
    }
}

impl<T, R: QueueType> Debug for Child<T, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Child")
            .field("handle", &std::any::type_name::<T>())
            .field("attached", &self.attached)
            .field("address", &self.address)
            .finish()
    }
}
