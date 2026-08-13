use super::*;
use std::{any::TypeId, convert::Infallible};
use type_sets::TypeSet;

/// An interface defines the set of messages that can be sent to a given actor.
/// This is usually derived on an enum using the `#[derive(Interface)]` macro.
///
/// It defines conversion methods to and from a boxed payload, which is used for dynamic dispatch of messages.
pub trait Interface:
    Message<Output = ()> + TryIntoPayload<Self> + FromPayload<Self> + Sized + Send + 'static
{
    type Set: TypeSet;

    fn try_from_boxed_payload(payload: BoxedPayload) -> Result<Self, BoxedPayload>;
    fn into_boxed_payload(self) -> BoxedPayload;

    fn try_from_any_payload<I: Message>(payload: I::Payload) -> Result<Self, I::Payload>
    where
        I::Payload: Send,
    {
        // This can be implemented faster using unsafe transmute
        Self::try_from_boxed_payload(BoxedPayload::new::<I>(payload))
            .map_err(|payload| payload.downcast::<I>().expect("Conversion back"))
    }

    fn try_into_any_payload<I: Message>(self) -> Result<I::Payload, Self> {
        // This can be implemented faster using unsafe transmute
        self.into_boxed_payload()
            .downcast::<I>()
            .map_err(|payload| Self::try_from_boxed_payload(payload).expect("Conversion back"))
    }

    fn invocable_with(type_id: TypeId) -> bool {
        <Self::Set as TypeSet>::members().contains(&type_id)
    }
}

pub trait FromPayload<M: Message> {
    fn from_payload(payload: M::Payload) -> Self;
}

pub trait TryIntoPayload<M: Message>: Sized {
    fn try_into_payload(self) -> Result<M::Payload, Self>;
}

impl Interface for () {
    type Set = Set![];

    fn try_from_boxed_payload(payload: BoxedPayload) -> Result<Self, BoxedPayload> {
        payload.downcast::<()>()
    }

    fn into_boxed_payload(self) -> BoxedPayload {
        BoxedPayload::new::<()>(())
    }
}

impl FromPayload<()> for () {
    fn from_payload(_payload: ()) -> Self {
        ()
    }
}
impl TryIntoPayload<()> for () {
    fn try_into_payload(self) -> Result<(), Self> {
        Ok(())
    }
}

impl Interface for Infallible {
    type Set = Set![];

    fn try_from_boxed_payload(payload: BoxedPayload) -> Result<Self, BoxedPayload> {
        Err(payload)
    }

    fn into_boxed_payload(self) -> BoxedPayload {
        unreachable!()
    }
}

impl FromPayload<Infallible> for Infallible {
    fn from_payload(_payload: Infallible) -> Self {
        unreachable!()
    }
}
impl TryIntoPayload<Infallible> for Infallible {
    fn try_into_payload(self) -> Result<Infallible, Self> {
        unreachable!()
    }
}
