use crate::*;
use futures::future::BoxFuture;
use polybox_core::errors::{SendCheckedError, SendError};
use std::sync::Arc;
use type_sets::AsSet;

/// A wrapper around a [`async_channel::Sender`] that acts as a [`PolyBox`].
pub struct TokioInbox<T> {
    sender: async_channel::Sender<T>,
}

impl<T> TokioInbox<T> {
    pub fn new(buffer: usize) -> (Self, async_channel::Receiver<T>) {
        let (sender, receiver) = async_channel::channel(buffer);
        (Self { sender }, receiver)
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

impl<T: Interface> PolyBox for TokioInbox<T> {
    type Set = T::Set;
    type Dyn<R: AsSet> = DynInbox<R>;

    fn into_dyn_unchecked<R>(self) -> DynInbox<R> {
        DynInbox::new_unchecked(Arc::new(self))
    }
}

impl<T: Interface> DynPolyBox for TokioInbox<T> {
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

impl<T, R> Sends<T> for TokioInbox<R>
where
    T: Message,
    R: TryIntoPayload<T> + FromPayload<T> + Send,
{
    async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        let (payload, output) = T::build_payload(msg);
        let interface = R::from_payload(payload);

        match self.sender.send(interface).await {
            Ok(()) => Ok(output),
            Err(e) => Err(SendError(T::destroy_payload(
                e.0.try_into_payload()
                    .map_err(|_| ())
                    .expect("Failed to convert payload back"),
            ))),
        }
    }
}

impl<T> Clone for TokioInbox<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
