use crate::_prelude::*;
use dashmap::DashMap;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct Registry {
    processes: DashMap<Pid, Option<RegistryEntry>>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

impl Registry {
    fn new() -> Self {
        Self {
            processes: DashMap::new(),
        }
    }

    pub fn local() -> &'static Self {
        REGISTRY.get_or_init(Self::new)
    }

    pub fn register_or_replace(
        &self,
        pid: Pid,
        address: impl Into<RegistryEntry>,
    ) -> Option<RegistryEntry> {
        self.processes.insert(pid, Some(address.into())).flatten()
    }

    /// Add a process to the registry if not already present.
    pub fn register<T: InboxKind>(
        &self,
        entry: impl Into<RegistryEntry<T>>,
    ) -> Result<(), RegistryAddError<T>> {
        let entry = entry.into();
        let pid = entry.pid();

        // If the pid is already present and the address is different, return an error.
        if let Some(val) = self.processes.get(pid)
            && let Some(val) = val.as_ref()
            && !val.address().is_same_process(entry.address())
        {
            return Err(RegistryAddError {
                pid: pid.clone(),
                entry: entry.into(),
            });
        }

        self.processes.insert(pid.clone(), Some(entry.into_dyn()));

        Ok(())
    }

    pub fn update_pid(&self, old_pid: Pid, new_pid: Pid) {
        if let Some(entry) = self
            .processes
            .remove(&old_pid)
            .map(|(_, entry)| entry)
            .flatten()
        {
            self.processes.insert(new_pid, Some(entry));
        }
    }

    pub fn get_entry(&self, pid: &Pid) -> Option<RegistryEntry> {
        self.processes
            .get(pid)
            .map(|entry| entry.value().clone())
            .flatten()
    }

    pub fn get(&self, pid: &Pid) -> Option<Address> {
        self.processes
            .get(pid)
            .map(|entry| entry.value().as_ref().map(|e| e.address().clone()))
            .flatten()
    }

    pub fn get_typed<T: Interface>(&self, pid: &Pid) -> Result<Address<T>, Report> {
        self.get(pid)
            .ok_or_else(|| rootcause::report!("Address not found for pid: {}", pid))?
            .downcast_ref::<T>()
            .ok_or_else(|| rootcause::report!("Address found for pid: {} but type mismatch", pid))
    }

    pub fn remove(&self, pid: &Pid) -> Option<RegistryEntry> {
        self.processes
            .remove(pid)
            .map(|(_, address)| address)
            .flatten()
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
    Data(SpawnData<T>),
    Address(Address<T>),
}

impl<T: InboxKind> RegistryEntry<T> {
    pub fn new(data: impl Into<RegistryEntry<T>>) -> Self {
        data.into()
    }

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

    pub fn data(&self) -> Option<&SpawnData<T>> {
        match &self.inner {
            _RegistryEntry::Data(data) => Some(data),
            _RegistryEntry::Address(_) => None,
        }
    }

    pub fn into_data(self) -> Option<SpawnData<T>> {
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
                inner: _RegistryEntry::Address(address.into_dyn::<Set!()>()),
            },
        }
    }

    pub fn pid(&self) -> &Pid {
        self.address().pid()
    }
}

impl<T: InboxKind> From<SpawnData<T>> for RegistryEntry<T> {
    fn from(data: SpawnData<T>) -> Self {
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

#[derive(thiserror::Error)]
#[error("Failed to add entry for pid {pid}")]
pub struct RegistryAddError<T: InboxKind = Dyn<Set![]>> {
    pid: Pid,
    entry: RegistryEntry<T>,
}

impl<T: InboxKind> std::fmt::Debug for RegistryAddError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryAddError")
            .field("pid", &self.pid)
            .field("entry", &self.entry)
            .finish()
    }
}
