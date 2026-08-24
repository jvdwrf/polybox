use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildConfig {
    pub restart_mode: RestartMode,
    pub abort_timeout: Duration,
    pub init_timeout: Duration,
    pub instantiation_timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildDescription {
    pub pid: Pid,
    pub cfg: ChildConfig,
}

impl ChildConfig {
    pub fn new_for_blueprint<T: Blueprint>(blueprint: &T) -> Self {
        Self {
            restart_mode: blueprint.default_restart_mode(),
            abort_timeout: blueprint.default_abort_timeout(),
            init_timeout: blueprint.default_init_timeout(),
            instantiation_timeout: blueprint.default_instantiation_timeout(),
        }
    }
}

pub struct ChildSpec<T: Spawnable = DynSpawner> {
    cfg: ChildConfig,
    blueprint: T,
    channel: Channel<T::ChannelSpec>,
}

// Implementations just when T is statically known
impl<T: Blueprint> ChildSpec<T> {
    pub fn new(id: impl Into<Pid>, blueprint: T) -> Self {
        Self {
            cfg: blueprint.generate_config(),
            blueprint: blueprint.into(),
            channel: Channel::<<T::Actor as Actor>::Interface>::new(id.into()),
        }
    }

    pub fn new_uuid(blueprint: T) -> Self {
        Self::new(Pid::rand(), blueprint)
    }

    pub fn split(self) -> (ChildSpec, Address<<T::Actor as Actor>::Interface>) {
        let address = self.channel.address().clone();
        (self.into_dyn(), address)
    }
}

// Implementations when T can be any type that implements RepeatSpawn
// (including DynRepeatSpawner)
impl<T: Spawnable> ChildSpec<T> {
    pub fn cfg(&self) -> &ChildConfig {
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

    pub fn with_cfg(mut self, cfg: ChildConfig) -> Self {
        self.cfg = cfg;
        self
    }

    pub async fn spawn(&self) -> Result<Child<T::Exit, T::ChannelSpec>, SpawnError> {
        self.blueprint.spawn_on(self.channel.clone()).await
    }

    pub fn into_dyn(self) -> ChildSpec {
        ChildSpec {
            cfg: self.cfg,
            blueprint: self.blueprint.into(),
            channel: self.channel.into_dyn(),
        }
    }
}

impl<T: Spawnable> AsActorRef for ChildSpec<T> {
    type ChannelSpec = T::ChannelSpec;

    fn as_channel(&self) -> &Channel<Self::ChannelSpec> {
        &self.channel
    }
}

impl<T: Spawnable + Clone> Clone for ChildSpec<T> {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            blueprint: self.blueprint.clone(),
            channel: self.channel.clone(),
        }
    }
}

impl<T: Spawnable + Debug> Debug for ChildSpec<T> {
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
    #[interface(path = "crate")]
    pub enum MyInterface {
        A(Envelope<u32>),
    }

    #[derive(Debug, Clone)]
    struct MyActor;

    impl Handler for MyActor {
        type Interface = MyInterface;
        type Error = Report;
        type Exit = ();

        async fn shut_down(
            &mut self,
            _address: &Address<Self::Interface>,
        ) -> Result<Self::Exit, Self::Error> {
            Ok(())
        }
    }

    impl Handle<u32> for MyActor {
        async fn handle(
            &mut self,
            _state: &mut HandlerState<Self>,
            msg: Envelope<u32>,
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
