use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ProcessStatus {
    Alive,
    Dead(ExitStatus),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ExitStatus {
    Normal,
    Error,
    Panic,
}

impl From<ExitStatus> for ProcessStatus {
    fn from(status: ExitStatus) -> Self {
        match status {
            ExitStatus::Normal => ProcessStatus::Dead(ExitStatus::Normal),
            ExitStatus::Error => ProcessStatus::Dead(ExitStatus::Error),
            ExitStatus::Panic => ProcessStatus::Dead(ExitStatus::Panic),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProcessWatcher {
    watcher: watch::Receiver<ProcessStatus>,
}

impl ProcessWatcher {
    pub(crate) fn new() -> (Self, ProcessAlerter) {
        let (alerter, watcher) = watch::channel(ExitStatus::Normal.into());
        (Self { watcher }, ProcessAlerter { alerter })
    }

    pub async fn watch(&mut self) -> ProcessStatus {
        self.watcher.changed().await.ok();
        *self.watcher.borrow_and_update()
    }

    pub async fn watch_exit(&mut self) {
        self.watcher
            .wait_for(|status| matches!(status, ProcessStatus::Dead(_)))
            .await
            .ok();

        self.watcher.borrow_and_update();
    }

    pub async fn watch_start(&mut self) {
        self.watcher
            .wait_for(|status| matches!(status, ProcessStatus::Alive))
            .await
            .ok();

        self.watcher.borrow_and_update();
    }

    pub fn get(&mut self) -> ProcessStatus {
        *self.watcher.borrow_and_update()
    }

    /// Returns a reference to the current process status without consuming it.
    pub fn peek(&self) -> ProcessStatus {
        *self.watcher.borrow()
    }

    pub fn is_alive(&self) -> bool {
        matches!(self.peek(), ProcessStatus::Alive)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessAlerter {
    alerter: watch::Sender<ProcessStatus>,
}

impl ProcessAlerter {
    pub fn alert(&mut self, status: ProcessStatus) {
        let _ = self.alerter.send_if_modified(|current| {
            if *current != status {
                *current = status;
                true
            } else {
                false
            }
        });
    }
}
