use std::fmt::Display;

use smol_str::SmolStr;

use super::*;

#[derive(Debug)]
pub enum Event<M> {
    Signal(SignalEvent),
    Message(M),
}

#[derive(Debug)]
pub enum SignalEvent {
    GetState(Tx<DebugState>),
    GetChildren(Tx<Vec<ChildDescription>>),
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDescription {
    pub pid: Pid,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[schema(value_type = String, format = "string", example = "MyActor is running")]
pub struct DebugState(SmolStr);

impl DebugState {
    pub fn new(description: impl Into<SmolStr>) -> Self {
        Self(description.into())
    }
}

impl<T: Into<SmolStr>> From<T> for DebugState {
    fn from(description: T) -> Self {
        Self::new(description)
    }
}

impl Display for DebugState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
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
