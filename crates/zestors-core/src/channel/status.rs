use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema, Copy)]
pub enum ActorStatus {
    Initializing,
    Running,
    Suspended,
    Exiting,
    Dead(Exit),
}

impl ActorStatus {
    pub fn should_exit(&self) -> bool {
        matches!(self, ActorStatus::Exiting | ActorStatus::Dead(_))
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
        matches!(self, ActorStatus::Dead(_))
    }

    pub fn is_initializing(&self) -> bool {
        matches!(self, ActorStatus::Initializing)
    }
}
