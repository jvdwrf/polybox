use crate::{_prelude::*, address::Address, child::Child};
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
    Channel::new(pid, ActorStatus::Exiting).spawn(f)
}

impl<T: Interface> Channel<T> {
    pub fn spawn<R, F>(self, f: impl FnOnce(ActorState<T>) -> F) -> Child<R, T>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, Report>> + Send + 'static,
        F::Output: Send + 'static,
    {
        let state = ActorState::new(EventStream::new(self.clone()));
        let spawned_future = AssertUnwindSafe(f(state)).catch_unwind();
        let pid = self.pid().clone();
        let this = self.clone();

        let handle = tokio::spawn(async move {
            // Notify that the process is alive
            tracing::debug!(pid = ?pid, "Process started");
            self.alert(ActorStatus::Running);

            // Run the future and catch any panics that occur
            let exit_value = spawned_future.await;

            // Depending on the exit_value, set the correct ExitSignal
            match exit_value {
                Ok(val) => {
                    match &val {
                        Ok(_) => {
                            tracing::debug!(pid = ?pid, "Process exited normally");
                            self.alert(ActorStatus::Exiting);
                        }
                        Err(_) => {
                            tracing::error!(pid = ?pid, "Process exited with error");
                            self.alert(ActorStatus::Exiting);
                        }
                    };
                    val
                }
                Err(boxed) => {
                    tracing::error!(pid = ?pid, "Process panicked");
                    self.alert(ActorStatus::Exiting);
                    std::panic::resume_unwind(boxed);
                }
            }
        });

        Child::new(handle, Address::new(this))
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

mod channel;
pub use channel::*;
