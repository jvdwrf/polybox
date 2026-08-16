use crate::*;
use std::any::Any;

/// Holds a [`MessageSpecifier::Payload`]
#[derive(Debug)]
pub struct BoxedPayload(Box<dyn Any + Send>);

impl BoxedPayload {
    pub fn new<T>(payload: Payload<T>) -> Self
    where
        T: Message,
        Payload<T>: Send + 'static,
    {
        Self(Box::new(payload))
    }

    pub fn downcast<T>(self) -> Result<Payload<T>, Self>
    where
        T: Message,
        Payload<T>: 'static,
    {
        match self.0.downcast() {
            Ok(cast) => Ok(*cast),
            Err(boxed) => Err(Self(boxed)),
        }
    }

    pub fn try_into_interface<T>(self) -> Result<T, Self>
    where
        T: Interface,
    {
        T::try_from_boxed_payload(self)
    }
}
