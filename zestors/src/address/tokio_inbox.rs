use super::*;

pub struct TokioInbox<T> {
    sender: tokio::sync::mpsc::Sender<T>,
}

impl<T> TokioInbox<T> {
    pub fn new(buffer: usize) -> (Self, tokio::sync::mpsc::Receiver<T>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(buffer);
        (Self { sender }, receiver)
    }

    pub fn inner(&self) -> &tokio::sync::mpsc::Sender<T> {
        &self.sender
    }

    pub fn into_inner(self) -> tokio::sync::mpsc::Sender<T> {
        self.sender
    }

    pub fn from_inner(sender: tokio::sync::mpsc::Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T: Interface> Inbox for TokioInbox<T> {
    type Set = T::Set;

    fn into_dyn_unchecked<R>(self) -> DynInbox<R> {
        DynInbox::new_unchecked(Arc::new(self))
    }
}

impl<T: Interface> SendsDynPayload for TokioInbox<T> {
    fn _send_any_payload_checked(
        &self,
        msg: AnyPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<AnyPayload>>> {
        Box::pin(async move {
            let payload = msg
                .try_into_interface::<T>()
                .map_err(|payload| SendCheckedError::NotAccepted(payload))?;

            self.send(payload).await.map_err(|SendError(payload)| {
                SendCheckedError::Closed(T::into_any_payload(payload))
            })
        })
    }
}

impl<T, R> Sends<T> for TokioInbox<R>
where
    T: Message,
    Payload<T>: Into<R>,
    R: TryInto<Payload<T>> + Send,
{
    async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        let (payload, output) = T::into_payload(msg);
        let payload = payload.into();

        match self.sender.send(payload).await {
            Ok(()) => Ok(output),
            Err(e) => Err(SendError(T::from_payload(
                e.0.try_into()
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
