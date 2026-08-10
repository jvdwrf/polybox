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
    ProcessData::new().spawn(f)
}

pub(crate) fn spawn_with<T, R, F>(
    data: ProcessData<T>,
    f: impl FnOnce(EventStream<T>, Address<T>) -> F,
) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    let ProcessData {
        inbox,
        receiver,
        signal_sender,
        signal_receiver,
        exit_watcher,
        mut exit_alerter,
    } = data;

    let address = Address::new(inbox, signal_sender, exit_watcher);
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

pub(crate) struct ProcessData<T> {
    inbox: Inbox<T>,
    receiver: Receiver<T>,
    signal_sender: SignalSender,
    signal_receiver: SignalReceiver,
    exit_watcher: ProcessWatcher,
    exit_alerter: ProcessAlerter,
}

impl<T> std::fmt::Debug for ProcessData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessData")
            .field("inbox", &self.inbox)
            .field("receiver", &self.receiver)
            .field("signal_sender", &self.signal_sender)
            .field("signal_receiver", &self.signal_receiver)
            .field("exit_watcher", &self.exit_watcher)
            .field("exit_alerter", &self.exit_alerter)
            .finish()
    }
}

impl<T> Clone for ProcessData<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            receiver: self.receiver.clone(),
            signal_sender: self.signal_sender.clone(),
            signal_receiver: self.signal_receiver.clone(),
            exit_watcher: self.exit_watcher.clone(),
            exit_alerter: self.exit_alerter.clone(),
        }
    }
}

impl<T> ProcessData<T> {
    pub fn new() -> Self {
        let (inbox, receiver) = Inbox::new();
        let (signal_sender, signal_receiver) = SignalSender::new();
        let (exit_watcher, exit_alerter) = ProcessWatcher::new();

        Self {
            inbox,
            receiver,
            signal_sender,
            signal_receiver,
            exit_watcher,
            exit_alerter,
        }
    }

    pub fn spawn<R, F>(self, f: impl FnOnce(EventStream<T>, Address<T>) -> F) -> Child<R, T>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_with(self, f)
    }
}

pub mod actor;
pub mod address;
pub mod child;
pub mod event_stream;
pub mod exit_watcher;
pub mod inbox;
pub mod registry;
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
        registry::*, signals::*, state::*, supervision::*, *,
    };

    pub(crate) use serde::{Deserialize, Serialize};
}

pub use polybox_codegen::{
    ActorInterface, InterfaceZestors as Interface, MessageZestors as Message,
};
