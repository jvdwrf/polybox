use super::*;

pub trait Spawn: Debug {
    fn spawn(&self, pid: Option<Pid>) -> Child;
    fn get_data(&self) -> ProcessData;
}

pub struct Spawner<R: ActorBlueprint> {
    blueprint: R,
    data: ProcessData<<R::Runner as ActorRunner>::Interface>,
}

impl<R: ActorBlueprint> Spawn for Spawner<R> {
    fn spawn(&self, override_pid: Option<Pid>) -> Child {
        let runner = self
            .blueprint
            .instantiate()
            .map(|res| res.map(std::mem::forget));

        self.data
            .clone()
            .spawn(override_pid, |stream, address| runner.run(stream, address))
            .into_dyn()
    }

    fn get_data(&self) -> ProcessData {
        self.data.clone().into_any()
    }
}

impl<R: ActorBlueprint> Spawner<R> {
    pub fn new(blueprint: R) -> Self {
        Self {
            data: ProcessData::new(Pid::rand_uuid()),
            blueprint,
        }
    }

    pub fn into_dyn(self) -> DynSpawner
    where
        R: Send + Sync + 'static,
    {
        DynSpawner(Arc::new(self))
    }
}

impl<R: ActorBlueprint> From<R> for Spawner<R> {
    fn from(value: R) -> Self {
        Self::new(value)
    }
}

impl<R: ActorBlueprint> Debug for Spawner<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spawner")
            .field("data", &self.data)
            .field("blueprint", &self.blueprint)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct DynSpawner(Arc<dyn Spawn + Send + Sync + 'static>);

impl Spawn for DynSpawner {
    fn spawn(&self, pid: Option<Pid>) -> Child {
        self.0.spawn(pid)
    }

    fn get_data(&self) -> ProcessData {
        self.0.get_data()
    }
}

impl DynSpawner {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: ActorBlueprint + Send + Sync + 'static,
    {
        DynSpawner(Arc::new(Spawner::new(blueprint)))
    }
}

impl<R> From<R> for DynSpawner
where
    R: ActorBlueprint + Send + Sync + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}
