use super::*;

pub trait SpawnWithData {
    type Inbox: InboxKind;
    type Exit: Send + 'static;

    fn spawn_with_data(&self, data: SpawnData<Self::Inbox>) -> Child<Self::Exit, Self::Inbox>;
}

trait SpawnDyn: Debug {
    fn spawn_dyn_with_data(&self, data: SpawnData) -> Child;
}

impl<R: Blueprint> SpawnDyn for R {
    fn spawn_dyn_with_data(&self, data: SpawnData) -> Child {
        let runner = self.instantiate().map(|res| res.map(std::mem::forget));

        data.clone()
            .downcast::<<R::Actor as Actor>::Interface>()
            .expect("ProcessData should be of correct type")
            .spawn(|stream, address| runner.run(stream, address))
            .into_dyn()
    }
}

#[derive(Debug, Clone)]
pub struct DynSpawner(Arc<dyn SpawnDyn + Send + Sync + 'static>);

impl SpawnWithData for DynSpawner {
    type Inbox = Dyn<Set![]>;
    type Exit = ();

    fn spawn_with_data(&self, data: SpawnData<Self::Inbox>) -> Child<Self::Exit, Self::Inbox> {
        self.0.spawn_dyn_with_data(data)
    }
}

impl<T: Blueprint> SpawnWithData for T {
    type Inbox = <T::Actor as Actor>::Interface;
    type Exit = <T::Actor as Actor>::Exit;

    fn spawn_with_data(&self, data: SpawnData<Self::Inbox>) -> Child<Self::Exit, Self::Inbox> {
        let runner = self.instantiate();

        data.clone()
            .spawn(|stream, address| runner.run(stream, address))
    }
}

impl DynSpawner {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: Blueprint + Send + Sync + 'static,
    {
        DynSpawner(Arc::new(blueprint))
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
