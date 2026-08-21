use super::*;

pub trait SpawnOn: Into<DynRepeatSpawner> {
    type Inbox: ChannelKind;
    type Exit: Send + 'static;

    fn spawn_on(
        &self,
        data: Channel<Self::Inbox>,
    ) -> Result<Child<Self::Exit, Self::Inbox>, SpawnError>;
}

impl<T: Blueprint> SpawnOn for T {
    type Inbox = <T::Actor as Actor>::Interface;
    type Exit = <T::Actor as Actor>::Exit;

    fn spawn_on(
        &self,
        data: Channel<Self::Inbox>,
    ) -> Result<Child<Self::Exit, Self::Inbox>, SpawnError> {
        let runner = self.instantiate();
        data.clone().spawn(|state| runner.run(state))
    }
}

#[derive(Debug, Clone)]
pub struct DynRepeatSpawner(Arc<dyn SpawnOnDyn + Send + Sync + 'static>);

trait SpawnOnDyn: Debug {
    fn spawn_on_dyn(&self, data: &Channel) -> Result<Child, SpawnError>;
}

impl<R: Blueprint> SpawnOnDyn for R {
    fn spawn_on_dyn(&self, data: &Channel) -> Result<Child, SpawnError> {
        let runner = self.instantiate().map(|res| res.map(std::mem::forget));

        data.downcast_ref::<<R::Actor as Actor>::Interface>()
            .expect("ProcessData should be of correct type")
            .clone()
            .spawn(|state| runner.run(state))
            .map(IntoDyn::into_dyn)
    }
}

impl SpawnOn for DynRepeatSpawner {
    type Inbox = Set!();
    type Exit = ();

    fn spawn_on(
        &self,
        data: Channel<Self::Inbox>,
    ) -> Result<Child<Self::Exit, Self::Inbox>, SpawnError> {
        self.0.spawn_on_dyn(&data)
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
