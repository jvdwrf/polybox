use crate::_prelude::*;
use dashmap::DashMap;
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct Registry {
    processes: DashMap<Pid, Address>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

impl Registry {
    fn new() -> Self {
        Self {
            processes: DashMap::new(),
        }
    }

    pub fn global() -> &'static Self {
        REGISTRY.get_or_init(Self::new)
    }

    pub fn add_or_replace<T: InboxKind>(&self, pid: Pid, address: Address<T>) -> Option<Address> {
        self.processes.insert(pid, address.into_dyn())
    }

    /// Add a process to the registry if not already present.
    pub fn add<T: InboxKind>(
        &self,
        pid: Pid,
        address: Address<T>,
    ) -> Result<(), RegistryAddError<T>> {
        if let Some(_) = self.processes.get(&pid) {
            return Err(RegistryAddError { pid, address });
        } else {
            self.processes.insert(pid, address.into_dyn());
            Ok(())
        }
    }

    pub fn get(&self, pid: &Pid) -> Option<Address> {
        self.processes.get(pid).map(|entry| entry.value().clone())
    }

    pub fn remove(&self, pid: &Pid) -> Option<Address> {
        self.processes.remove(pid).map(|(_, address)| address)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Failed to add entry for pid {pid}")]
pub struct RegistryAddError<T: InboxKind> {
    pid: Pid,
    address: Address<T>,
}
