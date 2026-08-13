use crate::{
    _prelude::*,
    address::Address,
    child::Child,
    inbox::{Inbox, Receiver},
    signals::{Signal, SignalSender},
};
use futures::FutureExt;
pub(crate) use polybox::*;
use std::{ops::Deref, panic::AssertUnwindSafe, sync::Arc};

pub fn spawn<T, R, F>(pid: Pid, f: impl FnOnce(EventStream<T>, Address<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    SpawnData::new(pid).spawn(f)
}

pub struct SpawnData<T: InboxKind = Dyn<Set![]>> {
    address: Address<T>,
    receiver: T::Receiver,
    signal_receiver: SignalReceiver,
    exit_alerter: ProcessAlerter,
}

impl<T: Interface> SpawnData<T> {
    pub fn new(pid: Pid) -> Self {
        let (inbox, receiver) = Inbox::new();
        let (signal_sender, signal_receiver) = SignalSender::new();
        let (exit_watcher, exit_alerter) = ProcessWatcher::new();

        Self {
            address: Address::new(inbox, signal_sender, exit_watcher, pid),
            receiver,
            signal_receiver,
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
        let SpawnData {
            address,
            receiver,
            signal_receiver,
            mut exit_alerter,
        } = self;

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
}

impl<T: InboxKind> std::fmt::Debug for SpawnData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessData")
            .field("address", &self.address)
            .field("receiver", &"Receiver")
            .field("signal_receiver", &self.signal_receiver)
            .field("exit_alerter", &self.exit_alerter)
            .finish()
    }
}

impl<T: InboxKind> SpawnData<T> {
    pub fn pid(&self) -> &Pid {
        self.address.pid()
    }

    pub fn into_any(self) -> SpawnData {
        let SpawnData {
            address,
            receiver,
            signal_receiver,
            exit_alerter,
        } = self;

        SpawnData {
            address: address.into_dyn(),
            receiver: T::map_receiver_into_any(receiver),
            signal_receiver,
            exit_alerter,
        }
    }
}

impl SpawnData {
    pub fn downcast<T: Interface>(self) -> Result<SpawnData<T>, Self> {
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

        Ok(SpawnData {
            address,
            receiver,
            signal_receiver: self.signal_receiver,
            exit_alerter: self.exit_alerter,
        })
    }
}

impl<T: InboxKind> Clone for SpawnData<T> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            receiver: self.receiver.clone(),
            signal_receiver: self.signal_receiver.clone(),
            exit_alerter: self.exit_alerter.clone(),
        }
    }
}

pub mod address;
pub mod child;
pub mod event_stream;
pub mod exit_watcher;
pub mod handler;
pub mod inbox;
pub mod node;
pub mod polybox;
pub mod registry;
pub mod signals;
pub mod supervision;
pub use ::type_sets;
pub use polybox_codegen::{
    HandlerInterface, InterfaceZestors as Interface, MessageZestors as Message,
};

pub(crate) mod _prelude {
    #![allow(unused_imports)]
    pub(crate) use crate::{
        address::*, child::*, event_stream::*, exit_watcher::*, handler::*, inbox::*, node::*,
        polybox::*, registry::*, signals::*, supervision::*, *,
    };

    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{
        fmt::{Debug, Display},
        time::Duration,
    };
}
