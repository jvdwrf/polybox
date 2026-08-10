use super::*;

pub trait ActorSpawner {
    fn spawn_mut(&mut self) -> Child;
}

pub struct SpawnFn<R: ActorBlueprint> {
    inbox: Inbox<<R::Runner as ActorRunner>::Interface>,
    receiver: Receiver<<R::Runner as ActorRunner>::Interface>,
    signal_sender: SignalSender,
    signal_receiver: SignalReceiver,
    exit_watcher: ExitWatcher,
    exit_alerter: ExitAlerter,
    blueprint: R,
}

impl<R: ActorBlueprint> ActorSpawner for SpawnFn<R> {
    fn spawn_mut(&mut self) -> Child {
        let runner = self
            .blueprint
            .instantiate()
            .map(|res| res.map(std::mem::forget));

        crate::spawn_with(
            (self.inbox.clone(), self.receiver.clone()),
            (self.signal_sender.clone(), self.signal_receiver.clone()),
            (self.exit_watcher.clone(), self.exit_alerter.clone()),
            |stream, address| runner.run(stream, address),
        )
        .into_dyn()
    }
}

impl<R: ActorBlueprint> SpawnFn<R> {
    pub fn new(blueprint: R) -> Self {
        let (inbox, receiver) = Inbox::new();
        let (signal_sender, signal_receiver) = SignalSender::new();
        let (exit_watcher, exit_alerter) = ExitWatcher::new();

        Self {
            inbox,
            receiver,
            signal_sender,
            signal_receiver,
            exit_watcher,
            exit_alerter,
            blueprint,
        }
    }

    pub fn into_dyn(self) -> DynSpawnFn
    where
        R: Send + 'static,
    {
        DynSpawnFn(Box::new(self))
    }
}

impl<R> From<R> for SpawnFn<R>
where
    R: ActorBlueprint,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}

pub struct DynSpawnFn(Box<dyn ActorSpawner + Send>);

impl Debug for DynSpawnFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DynSpawnFn").finish()
    }
}

impl ActorSpawner for DynSpawnFn {
    fn spawn_mut(&mut self) -> Child {
        self.0.spawn_mut()
    }
}

impl DynSpawnFn {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: ActorBlueprint + Send + 'static,
    {
        DynSpawnFn(Box::new(SpawnFn::new(blueprint)))
    }
}

impl<R> From<R> for DynSpawnFn
where
    R: ActorBlueprint + Send + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}
