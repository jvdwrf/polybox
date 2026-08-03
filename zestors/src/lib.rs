use crate::{
    address::Address,
    child::Child,
    inbox::{Inbox, Receiver},
    signals::{Signal, SignalReceiver, SignalSender},
};

pub fn spawn<T, R, F>(
    f: impl FnOnce(Receiver<T>, SignalReceiver, Address<T>) -> F,
) -> (Address<T>, Child<R>)
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    let (inbox, receiver) = Inbox::new(1_000_000);
    let (signal_inbox, signal_receiver) = SignalSender::new();
    let address = Address::new(inbox, signal_inbox);
    let handle = tokio::spawn(f(receiver, signal_receiver, address.clone()));
    let child = Child::new(handle, address.clone().into_dyn_subset());
    (address, child)
}

pub mod actor;
pub mod address;
pub mod child;
pub mod inbox;
pub mod signals;
pub mod state;
pub use polybox;
pub mod event_stream;
pub mod supervision;
pub(crate) use polybox::*;

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
