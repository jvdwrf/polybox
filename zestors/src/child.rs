#[allow(unused_imports)]
use crate::_prelude::*;
use futures::FutureExt as _;
use std::{any::Any, fmt::Debug, task::Poll};

pub struct Child<T> {
    handle: Option<tokio::task::JoinHandle<Result<T, anyhow::Error>>>,
    attached: bool,
}

impl<T> Child<T> {
    pub(crate) fn new(handle: tokio::task::JoinHandle<Result<T, anyhow::Error>>) -> Self {
        Self {
            handle: Some(handle),
            attached: true,
        }
    }

    pub fn abort(&self) {
        self.handle().abort();
    }

    pub fn is_finished(&self) -> bool {
        self.handle().is_finished()
    }

    pub fn into_handle(mut self) -> tokio::task::JoinHandle<Result<T, anyhow::Error>> {
        self.handle.take().unwrap()
    }

    pub fn handle(&self) -> &tokio::task::JoinHandle<Result<T, anyhow::Error>> {
        self.handle.as_ref().unwrap()
    }

    pub fn handle_mut(&mut self) -> &mut tokio::task::JoinHandle<Result<T, anyhow::Error>> {
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

    pub fn into_any(self) -> AnyChild
    where
        T: Send + 'static,
    {
        AnyChild::new(self)
    }
}

impl<T> Future for Child<T> {
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

impl<T> Drop for Child<T> {
    fn drop(&mut self) {
        if self.attached {
            self.abort();
        }
    }
}

impl<T> Debug for Child<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Child")
            .field("handle", &std::any::type_name::<T>())
            .field("attached", &self.attached)
            .finish()
    }
}

#[derive(Debug)]
pub struct AnyChild {
    child: Box<dyn IsAnyChild>,
}

impl AnyChild {
    pub fn new<T: Send + 'static>(child: Child<T>) -> Self {
        Self {
            child: Box::new(child),
        }
    }

    pub fn abort(&self) {
        self.child.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.child.is_finished()
    }

    pub fn as_any(&self) -> &dyn Any {
        self.child.as_any()
    }

    pub fn attach(&mut self) {
        self.child.attach();
    }

    pub fn detach(&mut self) {
        self.child.detach();
    }

    pub fn attached(mut self) -> Self {
        self.child.attach();
        self
    }

    pub fn detached(mut self) -> Self {
        self.child.detach();
        self
    }

    pub fn is_attached(&self) -> bool {
        self.child.is_attached()
    }

    pub fn downcast_ref<T: Send + 'static>(&self) -> Option<&Child<T>> {
        self.child.as_any().downcast_ref::<Child<T>>()
    }

    pub fn downcast<T: Send + 'static>(self) -> Result<Child<T>, Self> {
        if self.child.as_any().is::<Child<T>>() {
            let boxed = self.child.into_any();
            Ok(*boxed.downcast::<Child<T>>().unwrap())
        } else {
            Err(self)
        }
    }
}

trait IsAnyChild: Debug + Send + Sync {
    fn abort(&self);
    fn is_finished(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn attach(&mut self);
    fn detach(&mut self);
    fn is_attached(&self) -> bool;
    fn poll_any_child(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Box<dyn Any + Send>, JoinError>>;
}

impl<T: Send + 'static> IsAnyChild for Child<T> {
    fn abort(&self) {
        self.abort();
    }

    fn is_finished(&self) -> bool {
        self.is_finished()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn attach(&mut self) {
        self.attach();
    }

    fn detach(&mut self) {
        self.detach();
    }

    fn is_attached(&self) -> bool {
        self.is_attached()
    }

    fn poll_any_child(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Box<dyn Any + Send>, JoinError>> {
        self.poll_unpin(cx).map(|res| match res {
            Ok(value) => Ok(Box::new(value) as Box<dyn Any + Send>),
            Err(err) => Err(err),
        })
    }
}

impl Future for AnyChild {
    type Output = Result<Box<dyn Any + Send>, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        self.child.poll_any_child(cx)
    }
}
