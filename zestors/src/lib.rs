mod address;
mod errors;
mod protocol;
#[cfg(test)]
mod tests;

pub use {address::*, errors::*, protocol::*, sending::*};

mod sending {
    use super::*;

    pub trait Sends<I: Message> {
        fn send(&self, msg: I)
        -> impl Future<Output = Result<Output<I>, SendError<I>>> + Send + '_;
    }
}

// #[derive(Clone)]
// pub struct Address<T = Set![]> {
//     inner: Arc<DynActorRef<'static>>,
//     _t: PhantomData<fn() -> T>,
// }

// #[dynosaur(DynActorRef = dyn(box) ActorRef)]
// pub trait ActorRef: Send + Sync {
//     async fn try_send(
//         &self,
//         msg: Box<dyn Any + Send>,
//     ) -> Result<Box<dyn Any + Send>, TrySendError<Box<dyn Any + Send>>>;

//     fn try_send_now(
//         &self,
//         msg: Box<dyn Any + Send>,
//     ) -> Result<Box<dyn Any + Send>, TrySendError<Box<dyn Any + Send>>>;

//     fn force_send(
//         &self,
//         msg: Box<dyn Any + Send>,
//     ) -> Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>>>;
// }

// #[derive(Clone)]
// pub struct LocalActorRef {
//     msg_sender: mpsc::Sender<Box<dyn Any + Send>>,
//     signal_sender: mpsc::Sender<Signal>,
// }

// impl ActorRef for LocalActorRef {
//     async fn try_send(
//         &self,
//         msg: Box<dyn Any + Send>,
//     ) -> Result<Box<dyn Any + Send>, TrySendError<Box<dyn Any + Send>>> {
//         self.msg_sender
//             .send(msg)
//             .map_err(|e| TrySendError::Closed(e.0))?;
//         Ok(Box::new(()))
//     }

//     fn try_send_now(
//         &self,
//         msg: Box<dyn Any + Send>,
//     ) -> Result<Box<dyn Any + Send>, TrySendError<Box<dyn Any + Send>>> {
//         self.msg_sender.send(msg).map_err(|e| match e {
//             mpsc::TrySendError::Full(msg) => TrySendError::Full(msg),
//             mpsc::TrySendError::Disconnected(msg) => TrySendError::Closed(msg),
//         })?;
//         Ok(Box::new(()))
//     }

//     fn force_send(
//         &self,
//         msg: Box<dyn Any + Send>,
//     ) -> Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>>> {
//         self.msg_sender.send(msg).map_err(|e| SendError(e.0))?;
//         Ok(Box::new(()))
//     }
// }

// pub trait Sends<I: Message> {
//     fn send(&self, msg: I) -> Pin<Box<dyn Future<Output = Output<I>> + Send + '_>>;
//     fn send_now(&self, msg: I) -> Output<I>;
//     fn force_send(&self, msg: I) -> Output<I>;
// }

// impl<I, R> Sends<I> for Address<R>
// where
//     I: Message<Kind = FireAndForget>,
//     R: type_sets::Contains<I>,
// {
//     async fn send(&self, msg: I) -> Pin<Box<dyn Future<Output = Output<I>> + Send + '_>> {
//         self.inner.try_send(Box::new(msg)).await;
//         todo!()
//     }

//     fn send_now(&self, msg: I) -> Output<I> {
//         self.inner.try_send_now(Box::new(msg));
//         todo!()
//     }

//     fn force_send(&self, msg: I) -> Output<I> {
//         self.inner.force_send(Box::new(msg));
//         todo!()
//     }
// }

// pub enum Signal {
//     Healthy,
//     Shutdown,
//     Suspend,
//     WakeUp,
// }

// pub enum SignalMessage {
//     Healthy { reply: oneshot::Sender<bool> },
//     Shutdown {},
//     Sleep {},
//     WakeUp {},
// }
