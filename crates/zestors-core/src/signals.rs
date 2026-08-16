use crate::{_prelude::*, schemas::DurationSchema};
use polybox::Payload;
use polybox_codegen::Message;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Shutdown;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Kill;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Suspend;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Resume;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ActorStatus)]
pub struct GetStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
pub enum ActorStatus {
    Running,
    Suspended,
    Exiting,
}

impl ActorStatus {
    pub fn should_exit(&self) -> bool {
        matches!(self, ActorStatus::Exiting)
    }

    pub fn should_accept_messages(&self) -> bool {
        !matches!(self, ActorStatus::Suspended | ActorStatus::Exiting)
    }

    pub fn running(&self) -> bool {
        matches!(self, ActorStatus::Running)
    }

    pub fn suspended(&self) -> bool {
        matches!(self, ActorStatus::Suspended)
    }

    pub fn shutting_down(&self) -> bool {
        matches!(self, ActorStatus::Exiting)
    }
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub struct GetState;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DebugState {
    pub status: ActorStatus,

    #[schema(value_type = DurationSchema)]
    #[serde(with = "DurationSchema")]
    pub uptime: std::time::Duration,

    pub description: String,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<ChildDescription>")]
pub struct GetChildren;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDescription {
    pub pid: Pid,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ())]
pub struct Ping;

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum SignalInterface {
    Shutdown(Payload<Shutdown>),
    Suspend(Payload<Suspend>),
    Resume(Payload<Resume>),
    GetStatus(Payload<GetStatus>),
    GetState(Payload<GetState>),
    Ping(Payload<Ping>),
    GetChildren(Payload<GetChildren>),
}

impl SignalInterface {
    pub fn kind(&self) -> SignalKind {
        match self {
            SignalInterface::Shutdown(_) => SignalKind::Shutdown,
            SignalInterface::Suspend(_) => SignalKind::Suspend,
            SignalInterface::Resume(_) => SignalKind::Resume,
            SignalInterface::GetStatus(_) => SignalKind::GetStatus,
            SignalInterface::GetState(_) => SignalKind::GetState,
            SignalInterface::Ping(_) => SignalKind::Ping,
            SignalInterface::GetChildren(_) => SignalKind::GetChildren,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKind {
    Shutdown,
    Suspend,
    Resume,
    GetStatus,
    GetState,
    Ping,
    GetChildren,
}

#[derive(Debug)]
pub enum Event<M> {
    Signal(SignalEvent),
    Message(M),
}

#[derive(Debug)]
pub enum SignalEvent {
    GetState(Tx<DebugState>),
    GetChildren(Tx<Vec<ChildDescription>>),
    StatusUpdate(ActorStatus),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
pub enum RestartMode {
    Always,
    OnError,
    Never,
}
