use crate::registry::Registry;

use super::*;
use jiff::Zoned;
use std::{fmt::Debug, hash::Hash};
use type_sets::{Set, TypeSet};

#[repr(transparent)]
pub struct Channel<C: ChannelSpec = Set!()> {
    pub(super) data: Arc<ChannelData<C>>,
}

impl<T: ChannelSpec> Channel<T> {
    pub fn create(pid: Pid) -> Result<Self, DuplicatePidError>
    where
        T: Interface,
    {
        let channel = Channel {
            data: ChannelData::new(pid),
        };

        let address = channel.get_address();

        Registry::local()
            .register(address)
            .map_err(|_e| DuplicatePidError {
                pid: channel.pid().clone(),
            })?;

        Ok(channel)
    }

    pub(crate) fn from_ref(data: &ChannelData<T>) -> &Self {
        unsafe { &*(data as *const ChannelData<T> as *const Self) }
    }
}

impl<C: ChannelSpec> IntoDyn for Channel<C> {
    type Ref<T: ChannelSpec> = Channel<T>;

    fn into_dyn_unchecked<S>(self) -> Channel<S>
    where
        S: ChannelSpec,
    {
        Channel {
            data: ChannelData::arc_into_dyn_unchecked(self.data),
        }
    }
}

impl<C: ChannelSpec> AsDyn for Channel<C> {
    fn as_dyn_unchecked<S>(&self) -> &Channel<S>
    where
        S: ChannelSpec,
    {
        let data = ChannelData::arc_as_dyn_unchecked(&self.data);
        Channel::from_ref(data)
    }
}

impl<C: ChannelSpec> AsActorRef for Channel<C> {
    type ChannelSpec = C;

    fn channel_data(&self) -> &ChannelData<Self::ChannelSpec> {
        &*self.data
    }

    fn get_address(&self) -> Address<Self::ChannelSpec> {
        Address::new(self.data.clone())
    }
}

impl<T: ChannelSpec> Debug for Channel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ChannelData<T> as Debug>::fmt(&self.data, f)
    }
}

impl<T: ChannelSpec> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel {
            data: self.data.clone(),
        }
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
