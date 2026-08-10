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

    pub fn insert(&self, pid: Pid, address: Address) -> Option<Address> {
        self.processes.insert(pid, address)
    }

    pub fn get(&self, pid: &Pid) -> Option<Address> {
        self.processes.get(pid).map(|entry| entry.value().clone())
    }

    pub fn remove(&self, pid: &Pid) -> Option<Address> {
        self.processes.remove(pid).map(|(_, address)| address)
    }
}
