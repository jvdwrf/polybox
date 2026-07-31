use crate::_prelude::*;
use futures::FutureExt as _;
use std::any::Any;

#[derive(Debug)]
pub struct Child<T> {
    handle: tokio::task::JoinHandle<Result<T, anyhow::Error>>,
    attached: bool,
}

impl<T> Child<T> {
    pub(crate) fn new(handle: tokio::task::JoinHandle<Result<T, anyhow::Error>>) -> Self {
        Self {
            handle,
            attached: true,
        }
    }

    pub fn abort(&self) {
        self.handle.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub fn into_inner(self) -> tokio::task::JoinHandle<Result<T, anyhow::Error>> {
        self.handle
    }

    pub fn task_id(&self) -> tokio::task::Id {
        self.handle.id()
    }

    pub fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.handle.abort_handle()
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
}

impl<T> Future for Child<T> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.handle.poll_unpin(cx).map(|res| match res {
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
    Panic(Box<dyn Any + Send>),

    /// The task was aborted.
    #[error("task was aborted / cancelled")]
    Aborted,

    /// The actor exited with an unhandled error.
    #[error("task returned an error: {0}")]
    UnhandledError(anyhow::Error),
}

impl From<tokio::task::JoinError> for JoinError {
    fn from(err: tokio::task::JoinError) -> Self {
        if err.is_cancelled() {
            JoinError::Aborted
        } else if err.is_panic() {
            JoinError::Panic(err.into_panic())
        } else {
            unreachable!("JoinError is neither cancelled nor panicked: {:?}", err)
        }
    }
}
