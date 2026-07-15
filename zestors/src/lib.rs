use std::{
    any::Any,
    marker::{PhantomData, Send},
    sync::{Arc, mpsc},
};
use tokio::sync::oneshot;

pub use zestors_protocols::*;

#[derive(Clone)]
pub struct Address<T = Set![]> {
    inner: Arc<dyn DynActorRef>,
    _t: PhantomData<fn() -> T>,
}

#[async_trait::async_trait]
pub trait DynActorRef: Send + Sync {
    async fn try_send(&self, msg: Box<dyn Any + Send>) -> Box<dyn Any + Send>;
    fn try_send_now(&self, msg: Box<dyn Any + Send>) -> Box<dyn Any + Send>;
    fn force_send(&self, msg: Box<dyn Any + Send>) -> Box<dyn Any + Send>;
}

#[derive(Clone)]
pub struct LocalActorRef {
    msg_sender: mpsc::Sender<Box<dyn Any + Send>>,
    signal_sender: mpsc::Sender<Signal>,
}

pub trait Sends<I: Invocation> {
    fn send(&self, msg: I) -> impl Future<Output = Output<I>> + Send;
    fn send_now(&self, msg: I) -> Output<I>;
    fn force_send(&self, msg: I) -> Output<I>;
}

impl<T, R> Sends<T> for Address<R>
where
    T: Invocation,
    R: type_sets::Contains<T>,
{
    async fn send(&self, msg: T) -> Output<T> {
        self.inner.try_send(Box::new(msg)).await;
        todo!()
    }

    fn send_now(&self, msg: T) -> Output<T> {
        self.inner.try_send_now(Box::new(msg));
        todo!()
    }

    fn force_send(&self, msg: T) -> Output<T> {
        self.inner.force_send(Box::new(msg));
        todo!()
    }
}

pub enum Signal {
    Healthy,
    Shutdown,
    Suspend,
    WakeUp,
}

pub enum SignalMessage {
    Healthy { reply: oneshot::Sender<bool> },
    Shutdown {},
    Sleep {},
    WakeUp {},
}

#[cfg(test)]
mod tests;
