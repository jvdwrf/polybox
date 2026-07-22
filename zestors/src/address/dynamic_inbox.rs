use super::*;
use std::{marker::PhantomData, sync::Arc};

pub struct DynInbox<T> {
    inbox: Arc<dyn SendsDynPayload>,
    _t: PhantomData<fn() -> T>,
}

impl<T> Clone for DynInbox<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            _t: PhantomData,
        }
    }
}

impl<T> DynInbox<T> {
    pub fn new_unchecked(inbox: Arc<dyn SendsDynPayload>) -> Self {
        Self {
            inbox,
            _t: PhantomData,
        }
    }

    pub fn new<R>(inbox: R) -> Self
    where
        R: SendsDynPayload + Inbox + 'static,
        T: SubsetOf<R::Set>,
    {
        Self {
            inbox: Arc::new(inbox),
            _t: PhantomData,
        }
    }
}

impl<T: Members> Inbox for DynInbox<T> {
    type Set = T;

    fn into_dyn_unchecked<R>(self) -> DynInbox<R> {
        DynInbox::new_unchecked(self.inbox)
    }
}

impl<T, R> Sends<T> for DynInbox<R>
where
    T: Message<Kind: MessageSpecifier<T, Output: Send, Payload: Send>>,
    R: Members + Contains<T>,
{
    async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        self.send_checked(msg).await.map_err(|e| match e {
            SendCheckedError::Closed(msg) => SendError(msg),
            SendCheckedError::NotAccepted(_msg) => {
                panic!(
                    "Payload was not accepted, this should not happen if the type system is used correctly"
                )
            }
        })
    }
}

impl<T> SendsDynPayload for DynInbox<T> {
    fn _send_any_payload_checked(
        &self,
        msg: AnyPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<AnyPayload>>> {
        self.inbox._send_any_payload_checked(msg)
    }
}
