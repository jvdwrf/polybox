mod channel;
pub use channel::*;

mod messaging;
pub use messaging::*;

mod address;
pub use address::*;

mod child;
pub use child::*;

mod pid;
pub use pid::*;

mod signals;
pub use signals::*;

mod event_stream;
pub use event_stream::*;

pub(crate) mod schemas;

pub(crate) mod _prelude {
    pub(crate) use crate::*;

    pub(crate) use polybox_codegen::{Interface, Message};
    pub(crate) use rootcause::Report;
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::{future::Future, time::Duration};
    pub(crate) use type_sets::{Contains, Set, SubsetOf};
}

pub fn spawn<T, R, F>(pid: Pid, f: impl FnOnce(EventStream<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, rootcause::Report>> + Send + 'static,
    F::Output: Send + 'static,
{
    Channel::new(pid).spawn(f)
}

pub(crate) use messaging as polybox;
