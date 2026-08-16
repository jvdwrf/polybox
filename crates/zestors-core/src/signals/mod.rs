use crate::{_prelude::*, schemas::DurationSchema};
use polybox::Payload;
use polybox_codegen::Message;

mod interface;
pub(crate) use interface::*;

mod event;
pub use event::*;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDescription {
    pub pid: Pid,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DebugState {
    pub status: ActorStatus,

    #[schema(value_type = DurationSchema)]
    #[serde(with = "DurationSchema")]
    pub uptime: std::time::Duration,

    pub description: String,
}
