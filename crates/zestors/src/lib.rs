use crate::{
    _prelude::*,
    address::Address,
    child::Child,
    inbox::{Receiver, Sender},
    signals::{SignalInterface, SignalSender},
};
use futures::FutureExt;
pub(crate) use polybox::*;
use std::{ops::Deref, panic::AssertUnwindSafe, sync::Arc};

pub fn spawn<T, R, F>(pid: Pid, f: impl FnOnce(ActorState<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, Report>> + Send + 'static,
    F::Output: Send + 'static,
{
    SpawnData::new(pid).spawn(f)
}

pub struct SpawnData<T: SenderKind = Set!()> {
    address: Address<T>,
    receiver: T::Receiver,
    data: SharedProcessData,
}

impl<T: Interface> SpawnData<T> {
    pub fn new(pid: Pid) -> Self {
        let (inbox, receiver) = Sender::new();
        let data = SharedProcessData::new(pid);

        Self {
            address: Address::new(inbox, data.clone()),
            receiver,
            data,
        }
    }

    pub fn spawn<R, F>(self, f: impl FnOnce(ActorState<T>) -> F) -> Child<R, T>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, Report>> + Send + 'static,
        F::Output: Send + 'static,
    {
        let SpawnData {
            address,
            receiver,
            data,
        } = self;

        let mut status_updater = data.status_updater.clone();

        let state = ActorState::new(
            EventStream::new(receiver, data.signal_receiver.clone()),
            address.clone(),
        );
        let spawned_future = AssertUnwindSafe(f(state)).catch_unwind();
        let pid = address.pid().clone();

        let handle = tokio::spawn(async move {
            // Notify that the process is alive
            tracing::debug!(pid = ?pid, "Process started");
            status_updater.alert(ProcessStatus::Alive);

            // Run the future and catch any panics that occur
            let exit_value = spawned_future.await;

            // Depending on the exit_value, set the correct ExitSignal
            match exit_value {
                Ok(val) => {
                    match &val {
                        Ok(_) => {
                            tracing::debug!(pid = ?pid, "Process exited normally");
                            status_updater.alert(ExitStatus::Normal.into());
                        }
                        Err(_) => {
                            tracing::error!(pid = ?pid, "Process exited with error");
                            status_updater.alert(ExitStatus::Error.into());
                        }
                    };
                    val
                }
                Err(boxed) => {
                    tracing::error!(pid = ?pid, "Process panicked");
                    status_updater.alert(ExitStatus::Panic.into());
                    std::panic::resume_unwind(boxed);
                }
            }
        });

        Child::new(handle, address)
    }
}

impl<T: SenderKind> std::fmt::Debug for SpawnData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnData")
            .field("address", &self.address)
            .field("receiver", &"Receiver")
            .field("data", &self.data)
            .finish()
    }
}

impl<T: SenderKind> SpawnData<T> {
    pub fn pid(&self) -> &Pid {
        self.address.pid()
    }

    pub fn into_any(self) -> SpawnData {
        let SpawnData {
            address,
            receiver,
            data,
        } = self;

        SpawnData {
            address: address.into_dyn(),
            receiver: T::map_receiver_into_any(receiver),
            data,
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
            data: self.data,
        })
    }
}

impl<T: SenderKind> Clone for SpawnData<T> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            receiver: self.receiver.clone(),
            data: self.data.clone(),
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
pub mod process_data;
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
        polybox::*, process_data::*, registry::*, signals::*, supervision::*, *,
    };

    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{
        fmt::{Debug, Display},
        time::Duration,
    };

    pub(crate) use rootcause::Report;
}

pub(crate) mod schemas;
