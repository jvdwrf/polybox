use crate::registry::Registry;

use super::*;
use jiff::Zoned;
use std::{fmt::Debug, hash::Hash};
use type_sets::{Set, TypeSet};

#[repr(transparent)]
pub struct ChannelHandle<C: ChannelSpec = Set!()> {
    data: Arc<Channel<C>>,
}

impl<T: ChannelSpec> ChannelHandle<T> {
    /// Creates a new channel with the given `pid` and registers it in the local registry.
    pub fn create(pid: Pid) -> Result<Self, DuplicatePidError>
    where
        T: Interface,
    {
        let channel = ChannelHandle {
            data: Channel::new(pid),
        };

        let address = channel.get_address();

        Registry::local()
            .register(address)
            .map_err(|_e| DuplicatePidError {
                pid: channel.pid().clone(),
            })?;

        Ok(channel)
    }
}

impl<C: ChannelSpec> Drop for ChannelHandle<C> {
    fn drop(&mut self) {
        self.data.incr_strong_count();
    }
}

impl<C: ChannelSpec> IntoDyn for ChannelHandle<C> {
    type Ref<T: ChannelSpec> = ChannelHandle<T>;

    fn into_dyn_unchecked<S>(self) -> ChannelHandle<S>
    where
        S: ChannelSpec,
    {
        unsafe { std::mem::transmute::<ChannelHandle<C>, ChannelHandle<S>>(self) }
    }
}

impl<C: ChannelSpec> AsDyn for ChannelHandle<C> {
    fn as_dyn_unchecked<S>(&self) -> &ChannelHandle<S>
    where
        S: ChannelSpec,
    {
        unsafe { std::mem::transmute::<&ChannelHandle<C>, &ChannelHandle<S>>(self) }
    }
}

impl<C: ChannelSpec> AsActorRef for ChannelHandle<C> {
    type ChannelSpec = C;

    fn channel_data(&self) -> &Channel<Self::ChannelSpec> {
        &*self.data
    }

    fn get_address(&self) -> Address<Self::ChannelSpec> {
        Address::new(self.data.clone())
    }
}

impl<T: ChannelSpec> Debug for ChannelHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Channel<T> as Debug>::fmt(&self.data, f)
    }
}

impl<T: ChannelSpec> Clone for ChannelHandle<T> {
    fn clone(&self) -> Self {
        ChannelHandle {
            data: self.data.clone(),
        }
    }
}

impl<T: ChannelSpec> PartialEq for ChannelHandle<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pid() == other.pid()
    }
}
impl<T: ChannelSpec> Eq for ChannelHandle<T> {}

impl<T: ChannelSpec> Hash for ChannelHandle<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pid().hash(state);
    }
}

pub trait ChannelSpec: 'static {
    type Set: TypeSet + 'static;
}

impl<I: Interface> ChannelSpec for I {
    type Set = I::Set;
}

impl<S: TypeSet + 'static> ChannelSpec for Set<S> {
    type Set = S;
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
