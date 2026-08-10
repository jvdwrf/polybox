use crate::_prelude::*;
use dashmap::DashMap;
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone)]
pub struct Registry {
    processes: DashMap<Pid, RegistryEntry>,
    next_pid: Arc<AtomicU64>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

impl Registry {
    fn new() -> Self {
        Self {
            processes: DashMap::new(),
            next_pid: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn next_pid(&self) -> Pid {
        loop {
            let pid = Pid::new(self.next_pid.fetch_add(1, Ordering::Relaxed));

            if !self.contains(&pid) {
                return pid;
            }
        }
    }

    pub fn global() -> &'static Self {
        REGISTRY.get_or_init(Self::new)
    }

    pub fn add_or_replace(
        &self,
        pid: Pid,
        address: impl Into<RegistryEntry>,
    ) -> Option<RegistryEntry> {
        self.processes.insert(pid, address.into())
    }

    /// Add a process to the registry if not already present.
    pub fn add<T: InboxKind>(
        &self,
        pid: Pid,
        entry: impl Into<RegistryEntry<T>>,
    ) -> Result<(), RegistryAddError<T>> {
        if let Some(_) = self.processes.get(&pid) {
            return Err(RegistryAddError {
                pid,
                entry: entry.into(),
            });
        } else {
            self.processes.insert(pid, entry.into().into_dyn());
            Ok(())
        }
    }

    pub fn get(&self, pid: &Pid) -> Option<RegistryEntry> {
        self.processes.get(pid).map(|entry| entry.value().clone())
    }

    pub fn get_address(&self, pid: &Pid) -> Option<Address> {
        self.processes
            .get(pid)
            .map(|entry| entry.value().address().clone())
    }

    pub fn remove(&self, pid: &Pid) -> Option<RegistryEntry> {
        self.processes.remove(pid).map(|(_, address)| address)
    }

    pub fn contains(&self, pid: &Pid) -> bool {
        self.processes.contains_key(pid)
    }
}

/// An entry into a [`Registry`] that can either be [`ProcessData`] or just an [`Address`].
pub struct RegistryEntry<T: InboxKind = Dyn<Set![]>> {
    inner: _RegistryEntry<T>,
}

enum _RegistryEntry<T: InboxKind> {
    Data(ProcessData<T>),
    Address(Address<T>),
}

impl<T: InboxKind> RegistryEntry<T> {
    pub fn address(&self) -> &Address<T> {
        match &self.inner {
            _RegistryEntry::Data(data) => &data.address,
            _RegistryEntry::Address(address) => address,
        }
    }

    pub fn into_address(self) -> Address<T> {
        match self.inner {
            _RegistryEntry::Data(data) => data.address,
            _RegistryEntry::Address(address) => address,
        }
    }

    pub fn data(&self) -> Option<&ProcessData<T>> {
        match &self.inner {
            _RegistryEntry::Data(data) => Some(data),
            _RegistryEntry::Address(_) => None,
        }
    }

    pub fn into_data(self) -> Option<ProcessData<T>> {
        match self.inner {
            _RegistryEntry::Data(data) => Some(data),
            _RegistryEntry::Address(_) => None,
        }
    }

    pub fn into_dyn(self) -> RegistryEntry {
        match self.inner {
            _RegistryEntry::Data(data) => RegistryEntry {
                inner: _RegistryEntry::Data(data.into_any()),
            },
            _RegistryEntry::Address(address) => RegistryEntry {
                inner: _RegistryEntry::Address(address.into_dyn()),
            },
        }
    }
}

impl<T: InboxKind> From<ProcessData<T>> for RegistryEntry<T> {
    fn from(data: ProcessData<T>) -> Self {
        RegistryEntry {
            inner: _RegistryEntry::Data(data),
        }
    }
}

impl<T: InboxKind> From<Address<T>> for RegistryEntry<T> {
    fn from(address: Address<T>) -> Self {
        RegistryEntry {
            inner: _RegistryEntry::Address(address),
        }
    }
}

impl<T: InboxKind> Clone for RegistryEntry<T> {
    fn clone(&self) -> Self {
        match &self.inner {
            _RegistryEntry::Data(data) => RegistryEntry {
                inner: _RegistryEntry::Data(data.clone()),
            },
            _RegistryEntry::Address(address) => RegistryEntry {
                inner: _RegistryEntry::Address(address.clone()),
            },
        }
    }
}

impl<T: InboxKind> std::fmt::Debug for RegistryEntry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            _RegistryEntry::Data(data) => {
                f.debug_struct("RegistryEntry").field("data", data).finish()
            }
            _RegistryEntry::Address(address) => f
                .debug_struct("RegistryEntry")
                .field("address", address)
                .finish(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Failed to add entry for pid {pid}")]
pub struct RegistryAddError<T: InboxKind> {
    pid: Pid,
    entry: RegistryEntry<T>,
}
