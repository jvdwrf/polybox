use crate::{
    _prelude::*,
    address::Address,
    child::Child,
    inbox::{Inbox, Receiver},
    signals::{Signal, SignalSender},
};
use std::{ops::Deref, panic::AssertUnwindSafe, sync::Arc};

pub fn spawn<T, R, F>(
    pid: Option<Pid>,
    f: impl FnOnce(EventStream<T>, Address<T>) -> F,
) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    ProcessData::new().spawn(pid, f)
}

pub(crate) fn spawn_with_data<I, O, F>(
    data: ProcessData<I>,
    pid: Option<Pid>,
    f: impl FnOnce(EventStream<I>, Address<I>) -> F,
) -> Child<O, I>
where
    I: Interface,
    O: Send + 'static,
    F: Future<Output = Result<O, anyhow::Error>> + Send + 'static,
{
    let ProcessData {
        address,
        receiver,
        signal_receiver,
        mut exit_alerter,
    } = data;

    if let Some(pid) = pid {
        address.override_pid(pid);
    }

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

pub struct ProcessData<T: InboxKind = Dyn<Set![]>> {
    address: Address<T>,
    receiver: T::Receiver,
    signal_receiver: SignalReceiver,
    exit_alerter: ProcessAlerter,
}

impl<T: InboxKind> std::fmt::Debug for ProcessData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessData")
            .field("address", &self.address)
            .field("receiver", &"Receiver")
            .field("signal_receiver", &self.signal_receiver)
            .field("exit_alerter", &self.exit_alerter)
            .finish()
    }
}

impl<T: Interface> ProcessData<T> {
    pub fn new() -> Self {
        let (inbox, receiver) = Inbox::new();
        let (signal_sender, signal_receiver) = SignalSender::new();
        let (exit_watcher, exit_alerter) = ProcessWatcher::new();

        Self {
            address: Address::new(inbox, signal_sender, exit_watcher, None),
            receiver,
            signal_receiver,
            exit_alerter,
        }
    }

    pub fn spawn<R, F>(
        self,
        pid: Option<Pid>,
        f: impl FnOnce(EventStream<T>, Address<T>) -> F,
    ) -> Child<R, T>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_with_data(self, pid, f)
    }

    pub fn pid(&self) -> Pid {
        self.address.pid()
    }
}

impl<T: InboxKind> ProcessData<T> {
    pub fn into_any(self) -> ProcessData {
        let ProcessData {
            address,
            receiver,
            signal_receiver,
            exit_alerter,
        } = self;

        ProcessData {
            address: address.into_dyn(),
            receiver: T::map_receiver_into_any(receiver),
            signal_receiver,
            exit_alerter,
        }
    }
}

impl ProcessData {
    pub fn downcast<T: Interface>(self) -> Result<ProcessData<T>, Self> {
        let address = match self.address.downcast_ref::<T>() {
            Some(addr) => addr,
            None => return Err(self),
        };

        let receiver = self
            .receiver
            .downcast::<Receiver<T>>()
            .expect("Receiver should have the same type as the address")
            .deref()
            .clone();

        Ok(ProcessData {
            address,
            receiver,
            signal_receiver: self.signal_receiver,
            exit_alerter: self.exit_alerter,
        })
    }
}

impl<T: InboxKind> Clone for ProcessData<T> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            receiver: self.receiver.clone(),
            signal_receiver: self.signal_receiver.clone(),
            exit_alerter: self.exit_alerter.clone(),
        }
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
