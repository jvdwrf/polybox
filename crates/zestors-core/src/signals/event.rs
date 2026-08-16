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
    pub fn is_exit(self) -> bool {
        self == StatusUpdateEvent::Shutdown
    }

    pub fn is_resume(self) -> bool {
        self == StatusUpdateEvent::Resume
    }

    pub fn is_suspend(self) -> bool {
        self == StatusUpdateEvent::Suspend
    }
}
