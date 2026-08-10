use crate::_prelude::*;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProcessStatus {
    Alive,
    Dead(ExitReason),
}

#[derive(Clone, Debug)]
pub struct ExitWatcher {
    watcher: watch::Receiver<ProcessStatus>,
}

#[derive(Clone, Debug)]
pub struct ExitAlerter {
    alerter: watch::Sender<ProcessStatus>,
}

impl ExitWatcher {
    pub fn new() -> (Self, ExitAlerter) {
        let (alerter, watcher) = watch::channel(ProcessStatus::Dead(ExitReason::Shutdown));
        (Self { watcher }, ExitAlerter { alerter })
    }
}
