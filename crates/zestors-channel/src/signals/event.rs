use super::*;
use smol_str::SmolStr;
use std::fmt::Display;

#[derive(Debug)]
pub enum Event<M> {
    Signal(SignalEvent),
    Message(M),
}

#[derive(Debug)]
pub enum SignalEvent {
    Resume,
    Suspend,
    Shutdown,
}

impl SignalEvent {
    pub fn is_shutdown(&self) -> bool {
        matches!(self, SignalEvent::Shutdown)
    }

    pub fn is_resume(&self) -> bool {
        matches!(self, SignalEvent::Resume)
    }

    pub fn is_suspend(&self) -> bool {
        matches!(self, SignalEvent::Suspend)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DebugInfo(SmolStr);

impl DebugInfo {
    pub fn new(description: impl Into<SmolStr>) -> Self {
        Self(description.into())
    }
}

impl<T: Into<SmolStr>> From<T> for DebugInfo {
    fn from(description: T) -> Self {
        Self::new(description)
    }
}

impl Display for DebugInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
