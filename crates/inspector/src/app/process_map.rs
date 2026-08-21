use super::*;

#[derive(Default, Debug)]
pub struct ProcessMap {
    pub map: IndexMap<Pid, ProcessMapEntry>,
}

impl ProcessMap {
    pub fn merge(&mut self, new_map: IndexMap<Pid, (ChildConfig, ActorStatus, Vec<Pid>)>) {
        // Mark outdated entries that are no longer present
        for (pid, entry) in self.map.iter_mut() {
            if !new_map.contains_key(pid) {
                entry.outdated_since.get_or_insert_with(Instant::now);
            } else {
                entry.outdated_since = None;
            }
        }

        // Add or update entries from the new map
        for (pid, (cfg, status, children)) in new_map {
            match self.map.get_mut(&pid) {
                Some(entry) => {
                    entry.update(cfg, status, children);
                }
                None => {
                    let entry = ProcessMapEntry::new(pid.clone(), cfg, status, children);
                    self.map.insert(pid.clone(), entry);
                }
            }
        }
    }

    pub fn tree(&self) -> Vec<ProcessTree<'_>> {
        let child_pids: HashSet<_> = self
            .map
            .values()
            .flat_map(|entry| entry.children.iter().cloned())
            .collect();

        self.map
            .keys()
            .filter(|pid| !child_pids.contains(*pid))
            .filter_map(|pid| self.build_tree(pid))
            .collect()
    }

    fn build_tree(&self, pid: &Pid) -> Option<ProcessTree<'_>> {
        let process = self.map.get(pid)?;

        let children = process
            .children
            .iter()
            .filter_map(|child_pid| self.build_tree(child_pid))
            .collect();

        Some(ProcessTree {
            entry: process,
            children,
        })
    }
}
#[derive(Debug)]
pub struct ProcessTree<'a> {
    pub entry: &'a ProcessMapEntry,
    pub children: Vec<ProcessTree<'a>>,
}

#[derive(Clone, Debug)]
pub struct ProcessMapEntry {
    pub pid: Pid,
    pub cfg: ChildConfig,
    pub status: ActorStatus,
    pub children: Vec<Pid>,
    pub snapshot: Option<ChannelSnapshot>,
    pub debug: Option<DebugInfo>,
    pub outdated_since: Option<Instant>,
}

impl ProcessMapEntry {
    pub fn new(pid: Pid, cfg: ChildConfig, status: ActorStatus, children: Vec<Pid>) -> Self {
        Self {
            pid,
            cfg,
            status,
            children,
            snapshot: None,
            debug: None,
            outdated_since: None,
        }
    }

    pub fn update(&mut self, cfg: ChildConfig, status: ActorStatus, children: Vec<Pid>) {
        self.cfg = cfg;
        self.status = status;
        self.children = children;
        self.outdated_since = None;
    }
}
