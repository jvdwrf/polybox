use crate::_prelude::*;
use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use std::time::Duration;
use std::{pin::pin, sync::Arc};
use tokio::sync::Notify;

mod channel;
pub use channel::*;

pub mod errors;
pub(crate) use errors::*;

mod backpressure;
pub use backpressure::*;

mod actor_ref;
pub use actor_ref::*;

mod queue;
pub use queue::*;

mod dyn_conv;
pub use dyn_conv::*;

mod status;
pub use status::*;

mod spawn;
pub use spawn::*;

mod inbox;
pub use inbox::*;

mod pid;
pub use pid::*;

mod address;
pub use address::*;

mod child;
pub use child::*;

#[cfg(test)]
mod tests;

pub fn spawn<T, R, F>(pid: Pid, f: impl FnOnce(Inbox<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, rootcause::Report>> + Send + 'static,
    F::Output: Send + 'static,
{
    Channel::new(pid)
        .spawn(f)
        .expect("Channel was just created. Must be valid")
}
