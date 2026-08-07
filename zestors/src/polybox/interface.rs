use super::*;
use std::any::TypeId;
use type_sets::Members;

/// An interface defines the set of messages that can be sent to a given actor.
/// This is usually derived on an enum using the `#[derive(Interface)]` macro.
///
/// It defines conversion methods to and from a boxed payload, which is used for dynamic dispatch of messages.
pub trait Interface:
    Message<Kind = FireAndForget>
    + TryIntoPayload<Self>
    + FromPayload<Self>
    + AsSet
    + Sized
    + Send
    + 'static
{
    fn try_from_boxed_payload(payload: BoxedPayload) -> Result<Self, BoxedPayload>;
    fn into_boxed_payload(self) -> BoxedPayload;

    fn try_from_any_payload<I: Message>(payload: Payload<I>) -> Result<Self, Payload<I>>
    where
        Payload<I>: Send,
    {
        // This can be implemented faster using unsafe transmute
        Self::try_from_boxed_payload(BoxedPayload::new::<I>(payload))
            .map_err(|payload| payload.downcast::<I>().expect("Conversion back"))
    }

    fn try_into_any_payload<I: Message>(self) -> Result<Payload<I>, Self> {
        // This can be implemented faster using unsafe transmute
        self.into_boxed_payload()
            .downcast::<I>()
            .map_err(|payload| Self::try_from_boxed_payload(payload).expect("Conversion back"))
    }

    fn invocable_with(type_id: TypeId) -> bool {
        Self::members().contains(&type_id)
    }
}

pub trait FromPayload<T: Message> {
    fn from_payload(payload: Payload<T>) -> Self;
}

pub trait TryIntoPayload<T: Message>: Sized {
    fn try_into_payload(self) -> Result<Payload<T>, Self>;
}
