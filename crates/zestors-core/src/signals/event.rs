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
    StatusUpdate(StatusUpdateEvent),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum StatusUpdateEvent {
    Resume,
    Suspend,
    Shutdown,
}

impl StatusUpdateEvent {
    pub fn is_shutdown(self) -> bool {
        self == StatusUpdateEvent::Shutdown
    }

    pub fn is_resume(self) -> bool {
        self == StatusUpdateEvent::Resume
    }

    pub fn is_suspend(self) -> bool {
        self == StatusUpdateEvent::Suspend
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDescription {
    pub pid: Pid,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DebugState {
    // pub status: ActorStatus,

    // #[schema(value_type = DurationSchema)]
    // #[serde(with = "DurationSchema")]
    // pub uptime: std::time::Duration,
    pub description: String,
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
