use super::*;
use std::any::Any;

/// Holds a [`MessageSpecifier::Envelope`]
#[derive(Debug)]
pub struct BoxedEnvelope(Box<dyn Any + Send>);

impl BoxedEnvelope {
    pub fn new<T>(envelope: Envelope<T>) -> Self
    where
        T: Message,
        Envelope<T>: Send + 'static,
    {
        Self(Box::new(envelope))
    }

    pub fn downcast<T>(self) -> Result<Envelope<T>, Self>
    where
        T: Message,
        Envelope<T>: 'static,
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
        T::try_from_boxed_envelope(self)
    }
}
