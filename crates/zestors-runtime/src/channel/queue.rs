use super::*;
use std::any::{Any, TypeId};
use type_sets::TypeSet;

pub(crate) trait Queue: Send + 'static {
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

pub(crate) trait IsDynQueue: Any + Send + Sync + 'static {
    fn len(&self) -> usize;

    fn push_boxed_envelope_checked(
        &self,
        msg: BoxedEnvelope,
    ) -> Result<(), NotAccepted<BoxedEnvelope>>;

    fn pop_boxed_envelope(&self) -> Result<BoxedEnvelope, PopError>;

    fn members(&self) -> &'static [TypeId];
}

impl<I: Send + 'static> Queue for ConcurrentQueue<I> {
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

impl<I: Interface> IsDynQueue for ConcurrentQueue<I> {
    fn len(&self) -> usize {
        self.len()
    }

    fn push_boxed_envelope_checked(
        &self,
        msg: BoxedEnvelope,
    ) -> Result<(), NotAccepted<BoxedEnvelope>> {
        let envelope = msg
            .try_into_interface::<I>()
            .map_err(|envelope| NotAccepted(envelope))?;

        self.push_item(envelope).map_err(|e| match e {
            TrySendError::Closed(_) => unreachable!("Should never be closed"),
            TrySendError::Full(_) => {
                panic!("Queue is full: {:?}", std::any::type_name::<Self>());
            }
        })
    }

    fn pop_boxed_envelope(&self) -> Result<BoxedEnvelope, PopError> {
        self.pop_item()
            .map(|interface| interface.into_boxed_envelope())
    }

    fn members(&self) -> &'static [TypeId] {
        <I::Set as TypeSet>::members()
    }
}

impl Queue for dyn IsDynQueue {
    type Item = BoxedEnvelope;

    fn len(&self) -> usize {
        <dyn IsDynQueue as IsDynQueue>::len(self)
    }

    fn pop_item(&self) -> Result<Self::Item, PopError> {
        <dyn IsDynQueue as IsDynQueue>::pop_boxed_envelope(self)
    }

    fn push_item(&self, msg: Self::Item) -> Result<(), TrySendError<Self::Item>> {
        if let Err(NotAccepted(_msg)) = self.push_boxed_envelope_checked(msg) {
            panic!(
                "Message type not accepted by channel {:?}",
                std::any::type_name::<Self>()
            );
        }

        Ok(())
    }
}

impl dyn IsDynQueue {
    pub(super) fn try_push_msg<M: Message>(&self, msg: M) -> Result<M::Receipt, NotAccepted<M>> {
        let (envelope, output) = <M as MessageExt>::build_envelope(msg);
        let envelope = BoxedEnvelope::new::<M>(envelope);

        if let Err(NotAccepted(envelope)) =
            <dyn IsDynQueue as IsDynQueue>::push_boxed_envelope_checked(self, envelope)
        {
            let envelope = envelope
                .downcast::<M>()
                .expect("Failed to convert envelope back");

            return Err(NotAccepted(M::destroy_envelope(envelope)));
        }

        Ok(output)
    }
}
