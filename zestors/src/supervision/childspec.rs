use super::*;

#[derive(Debug)]
pub struct ChildSpec<T = DynSpawner> {
    pub id: Pid,
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
    pub spawner: T,
}

impl<T> ChildSpec<T> {
    pub fn new(id: impl Into<Pid>, spawner: T) -> Self {
        Self {
            id: id.into(),
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            spawner: spawner.into(),
        }
    }

    pub fn new_uuid(spawner: T) -> Self {
        Self {
            id: Pid::rand_uuid(),
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

    pub fn spawn(&mut self) -> Child
    where
        T: Spawn,
    {
        self.spawner.spawn(Some(self.id.clone()))
    }

    pub fn get_data(&self) -> ProcessData
    where
        T: Spawn,
    {
        self.spawner.get_data()
    }

    pub fn into_dyn(self) -> ChildSpec
    where
        T: Into<DynSpawner>,
    {
        ChildSpec {
            id: self.id,
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.into(),
        }
    }

    pub fn supervise(
        self,
        supervisor: &mut Supervisor,
    ) -> AddressFuture<<T::Runner as ActorRunner>::Interface>
    where
        T: ActorBlueprint + Send + Sync + 'static,
    {
        supervisor.add_child(self)
    }

    pub fn split_address(
        self,
    ) -> (
        ChildSpec<ExtractAddressBlueprint<T>>,
        AddressFuture<<T::Runner as ActorRunner>::Interface>,
    )
    where
        T: ActorBlueprint + Send + Sync + 'static,
    {
        let (rx, tx) = AddressFuture::new();

        let spec = ChildSpec {
            id: self.id,
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: ExtractAddressBlueprint {
                inner: self.spawner,
                tx,
            },
        };

        (spec, rx)
    }
}

impl<T: Clone> Clone for ChildSpec<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.clone(),
        }
    }
}

impl<T: ActorBlueprint + Send + Sync + 'static> From<ChildSpec<T>> for ChildSpec<DynSpawner> {
    fn from(value: ChildSpec<T>) -> Self {
        ChildSpec {
            id: value.id,
            restart_mode: value.restart_mode,
            abort_timeout: value.abort_timeout,
            spawner: DynSpawner::new(value.spawner),
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
