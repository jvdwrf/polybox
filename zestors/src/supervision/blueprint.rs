use super::*;

pub trait Blueprint: Debug {
    type Runner: Actor;

    fn instantiate(&self) -> Self::Runner;
}

impl<T: Actor + Clone + Debug> Blueprint for T {
    type Runner = T;

    fn instantiate(&self) -> Self::Runner {
        self.clone()
    }
}

pub trait BlueprintExt: Blueprint + Sized {
    fn into_spawn_fn(self) -> DynSpawner
    where
        Self: Send + Sync + 'static,
    {
        DynSpawner::new(self)
    }

    fn spawn(
        &self,
        pid: Pid,
    ) -> Child<<Self::Runner as Actor>::Exit, <Self::Runner as Actor>::Interface>
    where
        Self: Send + Sync + 'static,
    {
        self.instantiate().spawn(pid)
    }
}
impl<T: Blueprint> BlueprintExt for T {}
