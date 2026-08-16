use super::*;

pub trait RepeatSpawn {
    type Inbox: ChannelKind;
    type Exit: Send + 'static;

    fn spawn_with_data(&self, data: &Channel<Self::Inbox>) -> Child<Self::Exit, Self::Inbox>;
}

impl<T: Blueprint> RepeatSpawn for T {
    type Inbox = <T::Actor as Actor>::Interface;
    type Exit = <T::Actor as Actor>::Exit;

    fn spawn_with_data(&self, data: &Channel<Self::Inbox>) -> Child<Self::Exit, Self::Inbox> {
        let runner = self.instantiate();

        data.spawn_ref(|state| runner.run(state))
    }
}

#[derive(Debug, Clone)]
pub struct DynRepeatSpawner(Arc<dyn RepeatSpawnDyn + Send + Sync + 'static>);

trait RepeatSpawnDyn: Debug {
    fn spawn_dyn_with_data(&self, data: &Channel) -> Child;
}

impl<R: Blueprint> RepeatSpawnDyn for R {
    fn spawn_dyn_with_data(&self, data: &Channel) -> Child {
        let runner = self.instantiate().map(|res| res.map(std::mem::forget));

        data.downcast_ref::<<R::Actor as Actor>::Interface>()
            .expect("ProcessData should be of correct type")
            .spawn_ref(|state| runner.run(state))
            .into_dyn()
    }
}

impl RepeatSpawn for DynRepeatSpawner {
    type Inbox = Set!();
    type Exit = ();

    fn spawn_with_data(&self, data: &Channel<Self::Inbox>) -> Child<Self::Exit, Self::Inbox> {
        self.0.spawn_dyn_with_data(data)
    }
}

impl DynRepeatSpawner {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: Blueprint + Send + Sync + 'static,
    {
        DynRepeatSpawner(Arc::new(blueprint))
    }
}

impl<R> From<R> for DynRepeatSpawner
where
    R: Blueprint + Send + Sync + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}
