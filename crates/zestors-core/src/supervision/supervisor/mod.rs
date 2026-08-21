use super::*;
use futures::future::join_all;
use rootcause::{
    prelude::{IteratorExt, ResultExt},
    report,
};

mod supervisee;
use supervisee::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SupervisionStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

impl Default for SupervisionStrategy {
    fn default() -> Self {
        Self::OneForOne
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Terminal child error: {id}, error: {error:?}")]
pub struct RestartLimitReached {
    pub id: Pid,
    pub error: Option<JoinError>,
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("Restart limit reached for child {0}")]
    RestartLimit(#[from] RestartLimitReached),

    #[error("Another process is already registered with the same pid")]
    RegistryAddError(
        #[source]
        #[from]
        RegistryAddError,
    ),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RestartLimiter {
    intensity: RestartIntensity,
    restarts: VecDeque<Instant>,
}

impl RestartLimiter {
    pub fn new(intensity: RestartIntensity) -> Self {
        Self {
            intensity,
            restarts: VecDeque::new(),
        }
    }

    pub fn allow_restart(&mut self) -> bool {
        self.intensity.allow_restart(&mut self.restarts)
    }
}

mod blueprint;
pub use blueprint::*;

mod interface;
pub use interface::*;

mod actor;
pub use actor::*;
