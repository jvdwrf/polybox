use super::*;

pub trait SpawnRef: Debug {
    fn spawn(&self, data: SpawnData) -> Child;
}

pub struct Spawner<R: Blueprint> {
    blueprint: R,
    data: SpawnData<<R::Runner as Actor>::Interface>,
}

impl<R: Blueprint> SpawnRef for Spawner<R> {
    fn spawn(&self, data: SpawnData) -> Child {
        let runner = self
            .blueprint
            .instantiate()
            .map(|res| res.map(std::mem::forget));

        data.clone()
            .downcast::<<R::Runner as Actor>::Interface>()
            .expect("ProcessData should be of correct type")
            .spawn(|stream, address| runner.run(stream, address))
            .into_dyn()
    }
}

impl<R: Blueprint> Spawner<R> {
    pub fn new(blueprint: R) -> Self {
        Self {
            data: SpawnData::new(Pid::rand_uuid()),
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

impl<R: Blueprint> From<R> for Spawner<R> {
    fn from(value: R) -> Self {
        Self::new(value)
    }
}

impl<R: Blueprint> Debug for Spawner<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spawner")
            .field("data", &self.data)
            .field("blueprint", &self.blueprint)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct DynSpawner(Arc<dyn SpawnRef + Send + Sync + 'static>);

impl SpawnRef for DynSpawner {
    fn spawn(&self, data: SpawnData) -> Child {
        self.0.spawn(data)
    }
}

impl DynSpawner {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: Blueprint + Send + Sync + 'static,
    {
        DynSpawner(Arc::new(Spawner::new(blueprint)))
    }
}

impl<R> From<R> for DynSpawner
where
    R: Blueprint + Send + Sync + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}
