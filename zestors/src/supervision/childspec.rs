use super::*;

#[derive(Debug)]
pub struct ChildSpec<T = DynSpawnFn> {
    pub id: ChildId,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
    pub spawner: T,
}

impl<T: ActorSpawner> ChildSpec<T> {
    pub fn new(id: impl Into<ChildId>, spawner: T) -> Self {
        Self {
            id: id.into(),
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            spawner: spawner.into(),
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

    pub fn spawn(&mut self) -> Child<T::Exit, T::Inbox> {
        self.spawner.spawn_mut()
    }

    pub fn into_dyn(self) -> ChildSpec
    where
        T: Into<DynSpawnFn>,
    {
        ChildSpec {
            id: self.id,
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.into(),
        }
    }
}

impl<T: ActorBlueprint + Send + 'static> From<ChildSpec<T>> for ChildSpec<DynSpawnFn> {
    fn from(value: ChildSpec<T>) -> Self {
        ChildSpec {
            id: value.id,
            restart_mode: value.restart_mode,
            abort_timeout: value.abort_timeout,
            spawner: value.spawner.into(),
        }
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
    pub(super) fn allow_restart(&self, restarts: &mut VecDeque<Instant>) -> bool {
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
