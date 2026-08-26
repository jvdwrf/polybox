use crate::_prelude::*;
use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use std::time::Duration;
use std::{pin::pin, sync::Arc};
use tokio::sync::Notify;

pub(super) const BACKPRESSURE_LIMIT: usize = 100;
const KEEP_N_SPAWNS: usize = 5;
const KEEP_N_EXITS: usize = 5;
const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;

mod handle;
pub use handle::*;

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

mod channel_data;
pub use channel_data::*;

#[cfg(test)]
mod tests;

pub fn spawn_with<T, R, F>(
    pid: Pid,
    f: impl FnOnce(Inbox<T>) -> F,
) -> Result<Child<R, T>, DuplicatePidError>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, rootcause::Report>> + Send + 'static,
    F::Output: Send + 'static,
{
    Ok(Channel::create(pid)?
        .spawn(f)
        .expect("Channel was just created. Must be valid"))
}

pub fn spawn<T, R, F>(f: impl FnOnce(Inbox<T>) -> F) -> Child<R, T>
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, rootcause::Report>> + Send + 'static,
    F::Output: Send + 'static,
{
    Channel::create(Pid::rand())
        .expect("Pid is unique")
        .spawn(f)
        .expect("Only one inbox")
}
