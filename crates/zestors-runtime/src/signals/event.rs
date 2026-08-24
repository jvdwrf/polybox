use super::*;

#[derive(Debug)]
pub enum Event<M> {
    Signal(Signal),
    Message(M),
}

#[derive(Debug)]
pub enum Signal {
    Resume,
    Suspend,
    Shutdown,
}

impl Signal {
    pub fn is_shutdown(&self) -> bool {
        matches!(self, Signal::Shutdown)
    }

    pub fn is_resume(&self) -> bool {
        matches!(self, Signal::Resume)
    }

    pub fn is_suspend(&self) -> bool {
        matches!(self, Signal::Suspend)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RestartMode {
    Always,
    OnError,
    Never,
}

impl Default for RestartMode {
    fn default() -> Self {
        RestartMode::OnError
    }
}

impl RestartMode {
    pub fn should_restart(&self, exit: &Exit) -> bool {
        match (self, exit) {
            (RestartMode::Always, _) => true,
            (RestartMode::Never, _) => false,
            (RestartMode::OnError, Exit::Error(_)) => true,
            (RestartMode::OnError, Exit::Normal) => false,
        }
    }
}
