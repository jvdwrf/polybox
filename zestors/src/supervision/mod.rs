use crate::_prelude::*;
use futures::{FutureExt, Stream, StreamExt as _, future::join_all};
use indexmap::IndexMap;
use std::{
    any::Any,
    collections::VecDeque,
    fmt::{Debug, Display},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Instant;

pub struct ChildSpec {
    pub id: ChildId,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
    pub runnable: Runnable,
}

impl ChildSpec {
    pub fn new(id: impl Into<ChildId>, runnable: impl Into<Runnable>) -> Self {
        Self {
            id: id.into(),
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            runnable: runnable.into(),
        }
    }

    pub fn mode(mut self, restart_mode: RestartMode) -> Self {
        self.restart_mode = restart_mode;
        self
    }

    pub fn timeout(mut self, abort_timeout: Duration) -> Self {
        self.abort_timeout = abort_timeout;
        self
    }

    pub fn spawn(&self) -> Child {
        self.runnable.spawn()
    }
}

impl Debug for ChildSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildSpec")
            .field("id", &self.id)
            .field("restart_mode", &self.restart_mode)
            .field("abort_timeout", &self.abort_timeout)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestartMode {
    Always,
    OnError,
    Never,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RestartIntensity {
    max_restarts: usize,
    within: Duration,
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self {
            max_restarts: 3,
            within: Duration::from_secs(5),
        }
    }
}

impl RestartIntensity {
    fn allow_restart(&self, restarts: &mut VecDeque<Instant>) -> bool {
        let now = Instant::now();

        while let Some(front) = restarts.front() {
            if now.duration_since(*front) > self.within {
                restarts.pop_front();
            } else {
                break;
            }
        }

        if restarts.len() >= self.max_restarts {
            return false;
        }

        restarts.push_back(now);
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChildId(Arc<str>);

impl Deref for ChildId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ChildId {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }
}

impl<T: Into<Arc<str>>> From<T> for ChildId {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Display for ChildId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub use runnable::*;
mod runnable;

pub use supervisor::*;
mod supervisor;
