use super::*;
use std::{marker::PhantomData, sync::Arc};

pub struct DynInbox<T> {
    inbox: Arc<dyn SendsPayload>,
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
    pub fn new_unchecked(inbox: Arc<dyn SendsPayload>) -> Self {
        Self {
            inbox,
            _t: PhantomData,
        }
    }

    pub fn new<R>(inbox: R) -> Self
    where
        R: SendsPayload + Inbox + 'static,
        T: SubsetOf<R::Set>,
    {
        Self {
            inbox: Arc::new(inbox),
            _t: PhantomData,
        }
    }
}

impl<T> Inbox for DynInbox<T> {
    type Set = T;

    fn into_dyn_unchecked<R>(self) -> DynInbox<R> {
        DynInbox::new_unchecked(self.inbox)
    }
}

impl<T, R> Sends<T> for DynInbox<R>
where
    T: Message<Kind: MessageSpecifier<T, Output: Send, Payload: Send>>,
    R: Contains<T>,
{
    async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        let (payload, output) = T::into_payload(msg);
        let payload = AnyPayload::new::<T>(payload);

        match self.inbox._send_any_payload_checked(payload).await {
            Ok(()) => Ok(output),
            Err(SendCheckedError::Closed(payload)) => {
                let payload = payload
                    .downcast::<T>()
                    .expect("Failed to convert payload back");

                Err(SendError(T::from_payload(payload)))
            }
            Err(SendCheckedError::NotAccepted(_payload)) => {
                panic!(
                    "Payload was not accepted, this should not happen if the type system is used correctly"
                )
            }
        }
    }
}
