use super::*;

pub struct ChildSpec<T: RepeatSpawn = DynRepeatSpawner> {
    pub(crate) restart_mode: RestartMode,
    pub(crate) abort_timeout: Duration,
    pub(crate) spawner: T,
    pub(crate) data: SpawnData<T::Inbox>,
}

impl<T: Blueprint> ChildSpec<T> {
    pub fn new(id: impl Into<Pid>, blueprint: T) -> Self {
        Self {
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            spawner: blueprint.into(),
            data: SpawnData::<<T::Actor as Actor>::Interface>::new(id.into()),
        }
    }

    pub fn new_uuid(spawner: T) -> Self {
        let data = SpawnData::<<T::Actor as Actor>::Interface>::new(Pid::rand());

        Self {
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            spawner: spawner.into(),
            data: data,
        }
    }
}

impl<T: RepeatSpawn> ChildSpec<T> {
    pub fn pid(&self) -> &Pid {
        self.data.pid()
    }

    pub fn mode(mut self, restart_mode: RestartMode) -> Self {
        self.restart_mode = restart_mode;
        self
    }

    pub fn timeout(mut self, abort_timeout: Duration) -> Self {
        self.abort_timeout = abort_timeout;
        self
    }

    pub fn spawn(&self) -> Child<T::Exit, T::Inbox>
    where
        T: RepeatSpawn,
    {
        self.spawner.spawn_with_data(self.data.clone())
    }

    pub fn into_dyn(self) -> ChildSpec
    where
        T: Into<DynRepeatSpawner>,
    {
        ChildSpec {
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.into(),
            data: self.data.into_any(),
        }
    }

    pub fn supervise(self, supervisor: &mut Supervisor) -> Address<<T::Actor as Actor>::Interface>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        supervisor.add_child(self)
    }

    pub fn address(&self) -> &Address<T::Inbox> {
        &self.data.address
    }
}

impl<T: RepeatSpawn + Clone> Clone for ChildSpec<T> {
    fn clone(&self) -> Self {
        Self {
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            spawner: self.spawner.clone(),
            data: self.data.clone(),
        }
    }
}

impl<T: RepeatSpawn + Debug> Debug for ChildSpec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildSpec")
            .field("restart_mode", &self.restart_mode)
            .field("abort_timeout", &self.abort_timeout)
            .field("spawner", &self.spawner)
            .field("data", &self.data)
            .finish()
    }
}

impl<T: Blueprint + Send + Sync + 'static> From<ChildSpec<T>> for ChildSpec<DynRepeatSpawner> {
    fn from(value: ChildSpec<T>) -> Self {
        ChildSpec {
            restart_mode: value.restart_mode,
            abort_timeout: value.abort_timeout,
            spawner: DynRepeatSpawner::new(value.spawner),
            data: value.data.into_any(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Interface, HandlerInterface)]
    #[interface(crate = "crate")]
    pub enum MyInterface {
        A(Payload<u32>),
    }

    #[derive(Debug, Clone)]
    struct MyActor;

    impl Handler for MyActor {
        type Interface = MyInterface;
        type Error = anyhow::Error;
        type Exit = ();

        async fn exit(&mut self, _reason: ExitReason) -> Result<Self::Exit, Self::Error> {
            Ok(())
        }
    }

    impl Handle<u32> for MyActor {
        async fn handle(
            &mut self,
            _state: &mut HandlerState<Self>,
            msg: Payload<u32>,
        ) -> Result<(), Self::Error> {
            println!("Received message: {:?}", msg);
            Ok(())
        }
    }

    #[test]
    fn test_childspec_doesnt_panic_on_address_retrieval() {
        let spec = ChildSpec::new("test", MyActor);
        let _ = spec.address();
    }
}
