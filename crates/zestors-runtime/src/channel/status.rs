use super::*;

/// Represents the status of an actor in it's lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Copy)]
pub enum ActorStatus {
    /// The actor is not running. This is either the initial state of the actor, or the actor has exited. It is does not accept messages or signals in this state.
    Exited(Exit),

    /// The actor is in the process of initializing. Once the actor calls [`EvenStream::next`] for the first time, it will transition to the [`ActorStatus::Running`] state. It accepts messages and signals in this state, but will not process messages until
    Initializing,

    /// The actor is running and processing messages.
    Running,

    /// The actor is suspended and will not process messages until it is resumed.
    Suspended,

    /// The actor is in the process of shutting down. It will not accept new messages, but will finish processing any messages that are already in the queue.
    Exiting,
}

impl ActorStatus {
    pub fn should_exit(&self) -> bool {
        matches!(self, ActorStatus::Exiting | ActorStatus::Exited(_))
    }

    pub fn accepts_messages(&self) -> bool {
        matches!(
            self,
            ActorStatus::Initializing | ActorStatus::Running | ActorStatus::Suspended
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(self, ActorStatus::Running)
    }

    pub fn is_suspended(&self) -> bool {
        matches!(self, ActorStatus::Suspended)
    }

    pub fn is_shutting_down(&self) -> bool {
        matches!(self, ActorStatus::Exiting)
    }

    pub fn is_dead(&self) -> bool {
        matches!(self, ActorStatus::Exited(_))
    }

    pub fn is_initializing(&self) -> bool {
        matches!(self, ActorStatus::Initializing)
    }
}
