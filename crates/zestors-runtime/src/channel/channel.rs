use super::*;
use crate::registry::Registry;
use jiff::Zoned;
use std::{fmt::Debug, hash::Hash};
use type_sets::Set;

#[repr(transparent)]
pub struct Channel<C: Context = Set!()> {
    data: ActorHandle<C>,
}

impl<T: Context> Channel<T> {
    /// Creates a new channel with the given `pid` and registers it in the local registry.
    pub fn create(pid: Pid) -> Result<Self, DuplicatePidError>
    where
        T: Interface,
    {
        let channel = Channel {
            data: ActorHandle::new(pid, 1),
        };

        let address = channel.address().clone();

        Registry::local()
            .register(address)
            .map_err(|_e| DuplicatePidError {
                pid: channel.pid().clone(),
            })?;

        Ok(channel)
    }
}

impl<C: Context> Drop for Channel<C> {
    fn drop(&mut self) {
        self.data.decr_strong_count();
    }
}

impl<T: Context> Clone for Channel<T> {
    fn clone(&self) -> Self {
        self.handle().incr_strong_count();

        Channel {
            data: self.data.clone_ref(),
        }
    }
}

impl<C: Context> IntoDyn for Channel<C> {
    type Ref<T: Context> = Channel<T>;

    fn into_dyn_unchecked<S>(self) -> Channel<S>
    where
        S: Context,
    {
        unsafe { std::mem::transmute::<Channel<C>, Channel<S>>(self) }
    }
}

impl<C: Context> AsDyn for Channel<C> {
    fn as_dyn_unchecked<S>(&self) -> &Channel<S>
    where
        S: Context,
    {
        unsafe { std::mem::transmute::<&Channel<C>, &Channel<S>>(self) }
    }
}

impl<C: Context> ActorOps for Channel<C> {
    type Ctx = C;

    fn handle(&self) -> &ActorHandle<Self::Ctx> {
        &self.data
    }
}

impl<T: Context> Debug for Channel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ActorHandle<T> as Debug>::fmt(&self.data, f)
    }
}

impl<T: Context> PartialEq for Channel<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pid() == other.pid()
    }
}
impl<T: Context> Eq for Channel<T> {}

impl<T: Context> Hash for Channel<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pid().hash(state);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelSnapshot {
    pub pid: Pid,
    pub status: ActorStatus,
    pub signal_len: usize,
    pub msg_len: usize,
    pub spawns: Vec<Zoned>,
    pub exits: Vec<(Zoned, ExitStatus)>,
    pub created_at: Zoned,
}
