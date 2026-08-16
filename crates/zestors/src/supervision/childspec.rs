use super::*;

pub struct ChildSpec<T: RepeatSpawn = DynRepeatSpawner> {
    pub(crate) restart_mode: RestartMode,
    pub(crate) abort_timeout: Duration,
    pub(crate) blueprint: T,
    pub(crate) channel: Channel<T::Inbox>,
}

// Implementations just when T is statically known
impl<T: Blueprint> ChildSpec<T> {
    pub fn new(id: impl Into<Pid>, blueprint: T) -> Self {
        Self {
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            blueprint: blueprint.into(),
            channel: Channel::<<T::Actor as Actor>::Interface>::new(id.into()),
        }
    }

    pub fn new_uuid(spawner: T) -> Self {
        let data = Channel::<<T::Actor as Actor>::Interface>::new(Pid::rand());

        Self {
            restart_mode: RestartMode::OnError,
            abort_timeout: Duration::from_millis(5_000),
            blueprint: spawner.into(),
            channel: data,
        }
    }

    pub fn supervise(self, supervisor: &mut Supervisor) -> Address<<T::Actor as Actor>::Interface> {
        supervisor.add_child(self)
    }

    pub fn into_dyn(self) -> ChildSpec {
        ChildSpec {
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            blueprint: self.blueprint.into(),
            channel: self.channel.into_dyn(),
        }
    }
}

// Implementations when T can be any type that implements RepeatSpawn
// (including DynRepeatSpawner)
impl<T: RepeatSpawn> ChildSpec<T> {
    pub fn pid(&self) -> &Pid {
        self.channel.pid()
    }

    pub fn mode(mut self, restart_mode: RestartMode) -> Self {
        self.restart_mode = restart_mode;
        self
    }

    pub fn timeout(mut self, abort_timeout: Duration) -> Self {
        self.abort_timeout = abort_timeout;
        self
    }

    pub fn spawn(&self) -> Result<Child<T::Exit, T::Inbox>, SpawnError> {
        self.blueprint.spawn_with_data(self.channel.clone())
    }
}

impl<T: RepeatSpawn> AsActorRef for ChildSpec<T> {
    type QueueType = T::Inbox;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}

impl<T: RepeatSpawn + Clone> Clone for ChildSpec<T> {
    fn clone(&self) -> Self {
        Self {
            restart_mode: self.restart_mode,
            abort_timeout: self.abort_timeout,
            blueprint: self.blueprint.clone(),
            channel: self.channel.clone(),
        }
    }
}

impl<T: RepeatSpawn + Debug> Debug for ChildSpec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildSpec")
            .field("restart_mode", &self.restart_mode)
            .field("abort_timeout", &self.abort_timeout)
            .field("spawner", &self.blueprint)
            .field("data", &self.channel)
            .finish()
    }
}

impl<T: Blueprint> From<ChildSpec<T>> for ChildSpec {
    fn from(spec: ChildSpec<T>) -> Self {
        spec.into_dyn()
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
        type Error = Report;
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
