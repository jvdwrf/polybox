use super::*;

pub trait SpawnMut: Debug {
    fn spawn_mut(&mut self) -> Child;
    fn get_data(&self) -> ProcessData;
}

pub struct Spawner<R: ActorBlueprint> {
    blueprint: R,
    data: ProcessData<<R::Runner as ActorRunner>::Interface>,
}

impl<R: ActorBlueprint> SpawnMut for Spawner<R> {
    fn spawn_mut(&mut self) -> Child {
        let runner = self
            .blueprint
            .instantiate()
            .map(|res| res.map(std::mem::forget));

        self.data
            .clone()
            .spawn(|stream, address| runner.run(stream, address))
            .into_dyn()
    }

    fn get_data(&self) -> ProcessData {
        self.data.clone().into_any()
    }
}

impl<R: ActorBlueprint> Spawner<R> {
    pub fn new(blueprint: R) -> Self {
        Self {
            data: ProcessData::new(),
            blueprint,
        }
    }

    pub fn into_dyn(self) -> DynSpawner
    where
        R: Send + 'static,
    {
        DynSpawner(Box::new(self))
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

#[derive(Debug)]
pub struct DynSpawner(Box<dyn SpawnMut + Send>);

impl SpawnMut for DynSpawner {
    fn spawn_mut(&mut self) -> Child {
        self.0.spawn_mut()
    }

    fn get_data(&self) -> ProcessData {
        self.0.get_data()
    }
}

impl DynSpawner {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: ActorBlueprint + Send + 'static,
    {
        DynSpawner(Box::new(Spawner::new(blueprint)))
    }
}

impl<R> From<R> for DynSpawner
where
    R: ActorBlueprint + Send + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}
