use std::panic::AssertUnwindSafe;

use crate::{
    _prelude::*,
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
    let (inbox, receiver) = Inbox::new();
    let (signal_inbox, signal_receiver) = SignalSender::new();
    let (exit_watcher, exit_alerter) = ProcessWatcher::new();
    spawn_with(
        (inbox, receiver),
        (signal_inbox, signal_receiver),
        (exit_watcher, exit_alerter),
        f,
    )
}

pub(crate) fn spawn_with<T, R, F>(
    (inbox, receiver): (Inbox<T>, Receiver<T>),
    (signal_inbox, signal_receiver): (SignalSender, SignalReceiver),
    (exit_watcher, mut exit_alerter): (ProcessWatcher, ProcessAlerter),
    f: impl FnOnce(EventStream<T>, Address<T>) -> F,
) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    let address = Address::new(inbox, signal_inbox, exit_watcher);
    let stream = EventStream::new(receiver, signal_receiver);
    let spawned_future = AssertUnwindSafe(f(stream, address.clone())).catch_unwind();

    let handle = tokio::spawn(async move {
        // Notify that the process is alive
        exit_alerter.alert(ProcessStatus::Alive);

        // Run the future and catch any panics that occur
        let exit_value = spawned_future.await;

        // Depending on the exit_value, set the correct ExitSignal
        match exit_value {
            Ok(val) => {
                match &val {
                    Ok(_) => {
                        exit_alerter.alert(ExitStatus::Normal.into());
                    }
                    Err(_) => {
                        exit_alerter.alert(ExitStatus::Error.into());
                    }
                };
                val
            }
            Err(boxed) => {
                exit_alerter.alert(ExitStatus::Panic.into());
                std::panic::resume_unwind(boxed);
            }
        }
    });
    Child::new(handle, address)
}

pub mod actor;
pub mod address;
pub mod child;
pub mod event_stream;
pub mod exit_watcher;
pub mod inbox;
pub mod signals;
pub mod state;
pub mod supervision;
pub use ::type_sets;
use futures::FutureExt;
pub(crate) use polybox::*;

pub mod polybox;

pub(crate) mod _prelude {
    #![allow(unused_imports)]
    pub(crate) use crate::{
        actor::*, address::*, child::*, event_stream::*, exit_watcher::*, inbox::*, polybox::*,
        signals::*, state::*, supervision::*, *,
    };
}

pub use polybox_codegen::{
    ActorInterface, InterfaceZestors as Interface, MessageZestors as Message,
};
