use crate::*;
use futures::future::BoxFuture;
use polybox::{
    BoxedPayload, FromPayload, PolySender, SendsBoxedPayload, TryIntoPayload,
    errors::{SendCheckedError, SendError},
};
use polybox::{MessageExt, type_sets::TypeSet};
use std::sync::Arc;

/// A wrapper around a [`async_channel::Sender`] that acts as a [`PolyBox`].
pub struct Sender<T> {
    sender: async_channel::Sender<T>,
}

impl<T> Sender<T> {
    pub fn new() -> (Self, Receiver<T>) {
        Self::new_with_capacity(1_000_000)
    }

    pub fn new_with_capacity(capacity: usize) -> (Self, Receiver<T>) {
        let (sender, receiver) = async_channel::bounded(capacity);
        (Self { sender }, Receiver { receiver })
    }

    pub fn inner(&self) -> &async_channel::Sender<T> {
        &self.sender
    }

    pub fn into_inner(self) -> async_channel::Sender<T> {
        self.sender
    }

    pub fn from_inner(sender: async_channel::Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T: Interface> IntoDynVariant for Sender<T> {
    type DynVariant<R: DynSenderKind> = DynSender<R>;

    fn into_dyn_unchecked<R: DynSenderKind>(self) -> DynSender<R> {
        DynSender::new_unchecked(Arc::new(self))
    }
}
impl<T: Interface> AsTypeSet for Sender<T> {
    type Set = T::Set;
}

impl<T: Interface> SendsBoxedPayload for Sender<T> {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<BoxedPayload>>> {
        Box::pin(async move {
            let payload = msg
                .try_into_interface::<T>()
                .map_err(|payload| SendCheckedError::NotAccepted(payload))?;

            self.send(payload).await.map_err(|SendError(payload)| {
                SendCheckedError::Closed(T::into_boxed_payload(payload))
            })
        })
    }
}

impl<M, R> Sends<M> for Sender<R>
where
    M: Message,
    R: TryIntoPayload<M> + FromPayload<M> + Send,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        let (payload, output) = M::build_payload(msg);
        let interface = R::from_payload(payload);

        match self.sender.send(interface).await {
            Ok(()) => Ok(output),
            Err(e) => Err(SendError(M::destroy_payload(
                e.0.try_into_payload()
                    .map_err(|_| ())
                    .expect("Failed to convert payload back"),
            ))),
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> std::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

pub struct Receiver<T> {
    receiver: async_channel::Receiver<T>,
}

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await.ok()
    }

    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }
}

impl<T> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}
