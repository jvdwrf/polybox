use super::*;
use crate::{BoxedPayload, FromPayload, Message, MessageExt, TryIntoPayload};
use type_sets::Contains;

pub trait Queue: Send {
    type Item;

    fn new(capacity: usize) -> Self
    where
        Self: Sized;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn pop_item(&self) -> Result<Self::Item, PopError>;
    fn push_item(&self, msg: Self::Item) -> Result<(), TrySendError<Self::Item>>;
}

pub(super) trait Pushes<M: Message>: Sync {
    fn push_msg(&self, msg: M) -> Result<M::Output, TrySendError<M>>;
}

pub(super) trait IsDynQueue<S>: Send + Sync {
    fn len(&self) -> usize;

    fn push_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> Result<(), TrySendCheckedError<BoxedPayload>>;

    fn pop_boxed_payload(&self) -> Result<BoxedPayload, PopError>;
}

impl<I: Send> Queue for ConcurrentQueue<I> {
    type Item = I;

    fn len(&self) -> usize {
        self.len()
    }

    fn new(capacity: usize) -> Self
    where
        Self: Sized,
    {
        ConcurrentQueue::bounded(capacity)
    }

    fn pop_item(&self) -> Result<Self::Item, PopError> {
        self.pop()
    }

    fn push_item(&self, msg: Self::Item) -> Result<(), TrySendError<Self::Item>> {
        self.push(msg).map_err(Into::into)
    }
}

impl<I, M> Pushes<M> for ConcurrentQueue<I>
where
    I: TryIntoPayload<M> + FromPayload<M> + Send,
    M: Message,
{
    fn push_msg(&self, msg: M) -> Result<<M as Message>::Output, TrySendError<M>> {
        let (payload, output) = <M as MessageExt>::build_payload(msg);
        let interface = I::from_payload(payload);

        if let Err(e) = self.push_item(interface) {
            let is_closed = match &e {
                TrySendError::Closed(_) => true,
                TrySendError::Full(_) => false,
            };

            let msg = <M as MessageExt>::destroy_payload(
                e.into_inner()
                    .try_into_payload()
                    .map_err(|_| ())
                    .expect("Failed to convert payload back"),
            );

            return if is_closed {
                Err(TrySendError::Closed(msg))
            } else {
                Err(TrySendError::Full(msg))
            };
        }

        Ok(output)
    }
}

impl<I: Interface, S> IsDynQueue<S> for ConcurrentQueue<I> {
    fn len(&self) -> usize {
        self.len()
    }

    fn push_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> Result<(), TrySendCheckedError<BoxedPayload>> {
        let payload = msg
            .try_into_interface::<I>()
            .map_err(|payload| TrySendCheckedError::NotAccepted(payload))?;

        self.push_item(payload).map_err(|e| {
            let is_closed = match &e {
                TrySendError::Closed(_) => true,
                TrySendError::Full(_) => false,
            };

            let payload = e.into_inner().into_boxed_payload();

            if is_closed {
                TrySendCheckedError::Closed(payload)
            } else {
                TrySendCheckedError::Full(payload)
            }
        })
    }

    fn pop_boxed_payload(&self) -> Result<BoxedPayload, PopError> {
        self.pop_item()
            .map(|interface| interface.into_boxed_payload())
    }
}

pub struct DynQueue<S>(dyn IsDynQueue<S>);

impl<S> Queue for DynQueue<S> {
    type Item = BoxedPayload;

    #[allow(unused)]
    fn new(_: usize) -> Self
    where
        Self: Sized,
    {
        unimplemented!("Cannot create a new instance of a trait object");
    }

    fn len(&self) -> usize {
        <dyn IsDynQueue<S> as IsDynQueue<S>>::len(&self.0)
    }

    fn pop_item(&self) -> Result<Self::Item, PopError> {
        <dyn IsDynQueue<S> as IsDynQueue<S>>::pop_boxed_payload(&self.0)
    }

    fn push_item(&self, msg: Self::Item) -> Result<(), TrySendError<Self::Item>> {
        if let Err(e) =
            <dyn IsDynQueue<S> as IsDynQueue<S>>::push_boxed_payload_checked(&self.0, msg)
        {
            return match e {
                TrySendCheckedError::Closed(payload) => Err(TrySendError::Closed(payload)),
                TrySendCheckedError::Full(payload) => Err(TrySendError::Full(payload)),
                TrySendCheckedError::NotAccepted(payload) => {
                    panic!("Message type not accepted by channel: {:?}", payload);
                }
            };
        }

        Ok(())
    }
}

impl<S, M> Pushes<M> for DynQueue<S>
where
    S: Contains<M>,
    M: Message,
{
    fn push_msg(&self, msg: M) -> Result<M::Output, TrySendError<M>> {
        let (payload, output) = <M as MessageExt>::build_payload(msg);
        let payload = BoxedPayload::new::<M>(payload);

        if let Err(e) =
            <dyn IsDynQueue<S> as IsDynQueue<S>>::push_boxed_payload_checked(&self.0, payload)
        {
            return match e {
                TrySendCheckedError::Closed(payload) => {
                    let payload = payload
                        .downcast::<M>()
                        .expect("Failed to convert payload back");

                    Err(TrySendError::Closed(M::destroy_payload(payload)))
                }
                TrySendCheckedError::Full(payload) => {
                    let payload = payload
                        .downcast::<M>()
                        .expect("Failed to convert payload back");

                    Err(TrySendError::Full(M::destroy_payload(payload)))
                }
                TrySendCheckedError::NotAccepted(_) => {
                    panic!("Message type not accepted by channel");
                }
            };
        }

        Ok(output)
    }
}
