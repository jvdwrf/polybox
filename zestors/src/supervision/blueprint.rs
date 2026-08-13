use super::*;

pub trait Blueprint: Debug {
    type Actor: Actor;

    fn instantiate(&self) -> Self::Actor;
}

impl<T: Actor + Clone + Debug> Blueprint for T {
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
