use crate::{_prelude::*, address::Address, child::Child};
use futures::FutureExt;
pub(crate) use polybox::*;
use std::{panic::AssertUnwindSafe, sync::Arc};

pub fn spawn<T, R, F>(pid: Pid, f: impl FnOnce(ActorState<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, Report>> + Send + 'static,
    F::Output: Send + 'static,
{
    Channel::new(pid).spawn(f)
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
