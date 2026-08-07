use crate::{
    _prelude::EventStream,
    address::Address,
    child::Child,
    inbox::{Inbox, Receiver},
    signals::{Signal, SignalSender},
};

pub fn spawn<T, R, F>(f: impl FnOnce(EventStream<T>, Address<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    let (inbox, receiver) = Inbox::new(1_000_000);
    let (signal_inbox, signal_receiver) = SignalSender::new();
    let address = Address::new(inbox, signal_inbox);
    let stream = EventStream::new(receiver, signal_receiver);
    let handle = tokio::spawn(f(stream, address.clone()));
    Child::new(handle, address)
}

pub mod actor;
pub mod address;
pub mod child;
pub mod event_stream;
pub mod inbox;
pub mod signals;
pub mod state;
pub mod supervision;
pub use ::type_sets;
pub(crate) use polybox::*;

pub mod polybox;

pub(crate) mod _prelude {
    #![allow(unused_imports)]
    pub(crate) use crate::{
        actor::*, address::*, child::*, event_stream::*, inbox::*, signals::*, state::*,
        supervision::*, *,
    };
}

pub use polybox_codegen::{
    ActorInterface, InterfaceZestors as Interface, MessageZestors as Message,
};
