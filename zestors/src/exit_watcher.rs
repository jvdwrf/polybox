use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProcessStatus {
    Alive,
    Exited(ProcessExitStatus),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProcessExitStatus {
    Normal,
    Error,
    Panic,
}

impl From<ProcessExitStatus> for ProcessStatus {
    fn from(status: ProcessExitStatus) -> Self {
        match status {
            ProcessExitStatus::Normal => ProcessStatus::Exited(ProcessExitStatus::Normal),
            ProcessExitStatus::Error => ProcessStatus::Exited(ProcessExitStatus::Error),
            ProcessExitStatus::Panic => ProcessStatus::Exited(ProcessExitStatus::Panic),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExitWatcher {
    watcher: watch::Receiver<ProcessStatus>,
}

impl ExitWatcher {
    pub fn new() -> (Self, ExitAlerter) {
        let (alerter, watcher) = watch::channel(ProcessExitStatus::Normal.into());
        (Self { watcher }, ExitAlerter { alerter })
    }

    pub async fn wait(&mut self) -> ProcessStatus {
        self.watcher.changed().await.ok();
        *self.watcher.borrow_and_update()
    }

    pub async fn wait_for_exit(&mut self) {
        self.watcher
            .wait_for(|status| matches!(status, ProcessStatus::Exited(_)))
            .await
            .ok();

        self.watcher.borrow_and_update();
    }

    pub fn get(&mut self) -> ProcessStatus {
        *self.watcher.borrow_and_update()
    }
}

#[derive(Clone, Debug)]
pub struct ExitAlerter {
    alerter: watch::Sender<ProcessStatus>,
}

impl ExitAlerter {
    pub fn alert(&mut self, status: ProcessStatus) {
        let _ = self.alerter.send(status);
    }
}
