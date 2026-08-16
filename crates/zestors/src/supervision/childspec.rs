use super::*;

#[derive(Debug, Clone)]
pub struct SuperviseeConfig {
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
    pub init_timeout: Duration,
}

impl SuperviseeConfig {
    pub fn for_blueprint<T: Blueprint>(blueprint: &T) -> Self {
        Self {
            restart_mode: blueprint.default_restart_mode(),
            abort_timeout: blueprint.default_abort_timeout(),
            init_timeout: blueprint.default_init_timeout(),
        }
    }
}

pub struct ChildSpec<T: SpawnOn = DynRepeatSpawner> {
    cfg: SuperviseeConfig,
    blueprint: T,
    channel: Channel<T::Inbox>,
}

// Implementations just when T is statically known
impl<T: Blueprint> ChildSpec<T> {
    pub fn new(id: impl Into<Pid>, blueprint: T) -> Self {
        Self {
            cfg: blueprint.default_cfg(),
            blueprint: blueprint.into(),
            channel: Channel::<<T::Actor as Actor>::Interface>::new(id.into()),
        }
    }

    pub fn new_uuid(blueprint: T) -> Self {
        let data = Channel::<<T::Actor as Actor>::Interface>::new(Pid::rand());

        Self {
            cfg: blueprint.default_cfg(),
            blueprint: blueprint.into(),
            channel: data,
        }
    }

    pub fn supervise(self, supervisor: &mut Supervisor) -> Address<<T::Actor as Actor>::Interface> {
        supervisor.add_child(self)
    }

    pub fn into_dyn(self) -> ChildSpec {
        ChildSpec {
            cfg: self.cfg,
            blueprint: self.blueprint.into(),
            channel: self.channel.into_dyn(),
        }
    }
}

// Implementations when T can be any type that implements RepeatSpawn
// (including DynRepeatSpawner)
impl<T: SpawnOn> ChildSpec<T> {
    pub fn cfg(&self) -> &SuperviseeConfig {
        &self.cfg
    }

    pub fn blueprint(&self) -> &T {
        &self.blueprint
    }

    pub fn blueprint_mut(&mut self) -> &mut T {
        &mut self.blueprint
    }

    pub fn with_mode(mut self, restart_mode: RestartMode) -> Self {
        self.cfg.restart_mode = restart_mode;
        self
    }

    pub fn with_abort_timeout(mut self, abort_timeout: Duration) -> Self {
        self.cfg.abort_timeout = abort_timeout;
        self
    }

    pub fn with_init_timeout(mut self, init_timeout: Duration) -> Self {
        self.cfg.init_timeout = init_timeout;
        self
    }

    pub fn with_cfg(mut self, cfg: SuperviseeConfig) -> Self {
        self.cfg = cfg;
        self
    }

    pub fn spawn(&self) -> Result<Child<T::Exit, T::Inbox>, SpawnError> {
        self.blueprint.spawn_on(self.channel.clone())
    }
}

impl<T: SpawnOn> AsActorRef for ChildSpec<T> {
    type QueueType = T::Inbox;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        &self.channel
    }
}

impl<T: SpawnOn + Clone> Clone for ChildSpec<T> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            blueprint: self.blueprint.clone(),
            channel: self.channel.clone(),
        }
    }
}

impl<T: SpawnOn + Debug> Debug for ChildSpec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildSpec")
            .field("cfg", &self.cfg)
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

        async fn exit(
            &mut self,
            _address: &Address<Self::Interface>,
            _reason: ExitReason,
        ) -> Result<Self::Exit, Self::Error> {
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
