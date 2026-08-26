use super::*;
use crate::registry::Registry;
use jiff::Zoned;
use std::{fmt::Debug, hash::Hash};
use type_sets::{Set, TypeSet};

#[repr(transparent)]
pub struct Channel<C: ChannelSpec = Set!()> {
    data: ChannelData<C>,
}

impl<T: ChannelSpec> Channel<T> {
    /// Creates a new channel with the given `pid` and registers it in the local registry.
    pub fn create(pid: Pid) -> Result<Self, DuplicatePidError>
    where
        T: Interface,
    {
        let channel = Channel {
            data: ChannelData::new(pid, 1),
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

impl<C: ChannelSpec> Drop for Channel<C> {
    fn drop(&mut self) {
        self.data.decr_strong_count();
    }
}

impl<T: ChannelSpec> Clone for Channel<T> {
    fn clone(&self) -> Self {
        self.channel_data().incr_strong_count();

        Channel {
            data: self.data.clone_channel(),
        }
    }
}

impl<C: ChannelSpec> IntoDyn for Channel<C> {
    type Ref<T: ChannelSpec> = Channel<T>;

    fn into_dyn_unchecked<S>(self) -> Channel<S>
    where
        S: ChannelSpec,
    {
        unsafe { std::mem::transmute::<Channel<C>, Channel<S>>(self) }
    }
}

impl<C: ChannelSpec> AsDyn for Channel<C> {
    fn as_dyn_unchecked<S>(&self) -> &Channel<S>
    where
        S: ChannelSpec,
    {
        unsafe { std::mem::transmute::<&Channel<C>, &Channel<S>>(self) }
    }
}

impl<C: ChannelSpec> AsActorRef for Channel<C> {
    type ChannelSpec = C;

    fn channel_data(&self) -> &ChannelData<Self::ChannelSpec> {
        &self.data
    }
}

impl<T: ChannelSpec> Debug for Channel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ChannelData<T> as Debug>::fmt(&self.data, f)
    }
}

impl<T: ChannelSpec> PartialEq for Channel<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pid() == other.pid()
    }
}
impl<T: ChannelSpec> Eq for Channel<T> {}

impl<T: ChannelSpec> Hash for Channel<T> {
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
