use crate::_prelude::*;
use dashmap::DashMap;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct Registry {
    processes: DashMap<Pid, Option<Channel>>,
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

    pub fn register_or_replace(&self, pid: Pid, address: impl Into<Channel>) -> Option<Channel> {
        self.processes.insert(pid, Some(address.into())).flatten()
    }

    /// Add a process to the registry if not already present.
    pub fn register<T: QueueType>(
        &self,
        entry: impl Into<Channel<T>>,
    ) -> Result<(), RegistryAddError<T>> {
        let entry = entry.into();
        let pid = entry.pid();

        // If the pid is already present and the address is different, return an error.
        if let Some(val) = self.processes.get(pid)
            && let Some(val) = val.as_ref()
            && val.pid() != entry.pid()
        {
            return Err(RegistryAddError {
                pid: pid.clone(),
                entry: entry.into(),
            });
        }

        self.processes
            .insert(pid.clone(), Some(entry.into_dyn::<Set<()>>()));

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

    pub fn get(&self, pid: &Pid) -> Option<Address> {
        self.processes
            .get(pid)
            .map(|entry| {
                entry
                    .value()
                    .as_ref()
                    .map(|channel| Address::new(channel.clone()))
            })
            .flatten()
    }

    pub fn get_typed<T: Interface>(&self, pid: &Pid) -> Result<Address<T>, Report> {
        self.get(pid)
            .ok_or_else(|| rootcause::report!("Address not found for pid: {}", pid))?
            .downcast::<T>()
            .map_err(|_| rootcause::report!("Address found for pid: {} but type mismatch", pid))
    }

    pub fn remove(&self, pid: &Pid) -> Option<Channel> {
        self.processes
            .remove(pid)
            .map(|(_, address)| address)
            .flatten()
    }

    pub fn contains(&self, pid: &Pid) -> bool {
        self.processes.contains_key(pid)
    }
}

#[derive(thiserror::Error)]
#[error("Failed to add entry for pid {pid}")]
pub struct RegistryAddError<T: QueueType = Set!()> {
    pid: Pid,
    entry: Channel<T>,
}

impl<T: QueueType> std::fmt::Debug for RegistryAddError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryAddError")
            .field("pid", &self.pid)
            .field("entry", &self.entry)
            .finish()
    }
}
