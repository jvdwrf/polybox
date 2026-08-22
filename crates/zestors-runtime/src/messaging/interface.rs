use super::*;
use std::convert::Infallible;
use type_sets::{Set, TypeSet};

/// An interface defines the set of messages that can be sent to a given actor.
/// This is usually derived on an enum using the `#[derive(Interface)]` macro.
///
/// It defines conversion methods to and from a boxed envelope, which is used for dynamic dispatch of messages.
pub trait Interface:
    Message<Receipt = ()> + TryIntoEnvelope<Self> + FromEnvelope<Self> + Sized + Send + 'static
{
    /// The [set](TypeSet) of messages that this interface can handle.
    type Set: TypeSet;

    /// Attempt to convert a boxed envelope into this interface by downcasting.
    fn try_from_boxed_envelope(envelope: BoxedEnvelope) -> Result<Self, BoxedEnvelope>;

    /// Convert the inner envelope of this interface into a boxed envelope.
    fn into_boxed_envelope(self) -> BoxedEnvelope;
}

pub trait FromEnvelope<M: Message> {
    fn from_envelope(envelope: Envelope<M>) -> Self;
}

pub trait TryIntoEnvelope<M: Message>: Sized {
    fn try_into_envelope(self) -> Result<Envelope<M>, Self>;
}

impl Interface for () {
    type Set = Set!();

    fn try_from_boxed_envelope(envelope: BoxedEnvelope) -> Result<Self, BoxedEnvelope> {
        envelope.downcast::<()>().map(|env| env.msg)
    }

    fn into_boxed_envelope(self) -> BoxedEnvelope {
        BoxedEnvelope::new::<()>(Envelope::new(self, ()))
    }
}

impl FromEnvelope<()> for () {
    fn from_envelope(_envelope: Envelope<()>) -> Self {
        ()
    }
}
impl TryIntoEnvelope<()> for () {
    fn try_into_envelope(self) -> Result<Envelope<()>, Self> {
        Ok(Envelope::new((), ()))
    }
}

impl Interface for Infallible {
    type Set = Set!();

    fn try_from_boxed_envelope(envelope: BoxedEnvelope) -> Result<Self, BoxedEnvelope> {
        Err(envelope)
    }

    fn into_boxed_envelope(self) -> BoxedEnvelope {
        unreachable!()
    }
}

impl FromEnvelope<Infallible> for Infallible {
    fn from_envelope(_envelope: Envelope<Infallible>) -> Self {
        unreachable!()
    }
}
impl TryIntoEnvelope<Infallible> for Infallible {
    fn try_into_envelope(self) -> Result<Envelope<Infallible>, Self> {
        unreachable!()
    }
}
