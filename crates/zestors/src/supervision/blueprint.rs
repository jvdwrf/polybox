use super::*;

pub trait Blueprint: Debug + Send + Sync + 'static {
    type Actor: Actor;

    fn instantiate(&self) -> Self::Actor;

    fn default_abort_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_init_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }
}

impl<T: Actor + Clone + Debug + Send + Sync + 'static> Blueprint for T {
    type Actor = T;

    fn instantiate(&self) -> Self::Actor {
        self.clone()
    }
}

pub trait BlueprintExt: Blueprint + Sized {
    fn into_spawn_fn(self) -> DynRepeatSpawner
    where
        Self: Send + Sync + 'static,
    {
        DynRepeatSpawner::new(self)
    }

    fn spawn(
        &self,
        pid: Pid,
    ) -> Child<<Self::Actor as Actor>::Exit, <Self::Actor as Actor>::Interface>
    where
        Self: Send + Sync + 'static,
    {
        self.instantiate().spawn(pid)
    }
}
impl<T: Blueprint> BlueprintExt for T {}

pub trait IntoBlueprint {
    type Blueprint: Blueprint;

    fn into_blueprint(self) -> Self::Blueprint;
}

impl<T: Blueprint> IntoBlueprint for T {
    type Blueprint = T;

    fn into_blueprint(self) -> Self::Blueprint {
        self
    }
}
