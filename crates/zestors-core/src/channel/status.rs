use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema, Copy)]
pub enum ActorStatus {
    Initializing,
    Running,
    Suspended,
    Exiting,
    Exited(Option<ExitError>),
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
