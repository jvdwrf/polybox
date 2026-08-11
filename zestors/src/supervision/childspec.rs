use super::*;

#[derive(Debug)]
pub struct ChildSpec<T = DynSpawner> {
    pub(crate) restart_mode: RestartMode,
    pub(crate) abort_timeout: Duration,
    pub(crate) spawner: T,
    pub(crate) data: SpawnData,
}

impl<T> ChildSpec<T> {
    pub fn new(id: impl Into<Pid>, blueprint: T) -> Self
    where
        T: Blueprint,
    {
        Self {
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            spawner: blueprint.into(),
            data: SpawnData::<<T::Runner as ActorRunner>::Interface>::new(id.into()).into_any(),
        }
    }

    pub fn pid(&self) -> &Pid {
        self.data.pid()
    }

    pub fn new_uuid(spawner: T) -> Self
    where
        T: Blueprint,
    {
        Self {
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            spawner: spawner.into(),
            data: SpawnData::<<T::Runner as ActorRunner>::Interface>::new(Pid::rand_uuid())
                .into_any(),
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

    pub fn spawn(&mut self) -> Child
    where
        T: SpawnRef,
    {
        self.spawner.spawn(self.data.clone())
    }

    pub fn into_dyn(self) -> ChildSpec
    where
        T: Into<DynSpawner>,
    {
        ChildSpec {
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.into(),
            data: self.data,
        }
    }

    pub fn supervise(
        self,
        supervisor: &mut Supervisor,
    ) -> Address<<T::Runner as ActorRunner>::Interface>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        supervisor.add_child(self)
    }

    pub fn address(&self) -> Address<<T::Runner as ActorRunner>::Interface>
    where
        T: Blueprint,
    {
        self.data.address.downcast_ref().unwrap()
    }
}

impl<T: Clone> Clone for ChildSpec<T> {
    fn clone(&self) -> Self {
        Self {
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.clone(),
            data: self.data.clone(),
        }
    }
}

impl<T: Blueprint + Send + Sync + 'static> From<ChildSpec<T>> for ChildSpec<DynSpawner> {
    fn from(value: ChildSpec<T>) -> Self {
        ChildSpec {
            restart_mode: value.restart_mode,
            abort_timeout: value.abort_timeout,
            spawner: DynSpawner::new(value.spawner),
            data: value.data,
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
