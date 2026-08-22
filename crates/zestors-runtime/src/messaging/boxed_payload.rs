use super::*;
use std::any::Any;

/// Holds a [`MessageSpecifier::Envelope`]
#[derive(Debug)]
pub struct BoxedEnvelope(Box<dyn Any + Send>);

impl BoxedEnvelope {
    pub fn new<T: Message>(envelope: T::Envelope) -> Self {
        Self(Box::new(envelope))
    }

    pub fn downcast<T: Message>(self) -> Result<Envelope<T>, Self> {
        match self.0.downcast() {
            Ok(cast) => Ok(*cast),
            Err(boxed) => Err(Self(boxed)),
        }
    }

    pub fn try_into_interface<T: Interface>(self) -> Result<T, Self> {
        T::try_from_boxed_envelope(self)
    }
}
