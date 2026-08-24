use futures::future::BoxFuture;
use rootcause::compat::ReportAsError;

use super::*;

pub trait SpawnOn: Into<DynRepeatSpawner> {
    type Inbox: ChannelKind;
    type Exit: Send + 'static;

    fn spawn_on(
        &self,
        data: Channel<Self::Inbox>,
    ) -> impl Future<Output = Result<Child<Self::Exit, Self::Inbox>, SpawnError>> + Send;
}

impl<T: Blueprint> SpawnOn for T {
    type Inbox = <T::Actor as Actor>::Interface;
    type Exit = <T::Actor as Actor>::Exit;

    async fn spawn_on(
        &self,
        data: Channel<Self::Inbox>,
    ) -> Result<Child<Self::Exit, Self::Inbox>, SpawnError> {
        let runner = self
            .build()
            .await
            .map_err(|x| SpawnError::Instantiation(x.into()))?;

        data.clone()
            .spawn(|state| runner.run(state))
            .map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct DynRepeatSpawner(Arc<dyn SpawnOnDyn + Send + Sync + 'static>);

trait SpawnOnDyn: Debug {
    fn spawn_on_dyn<'a>(&'a self, data: &'a Channel) -> BoxFuture<'a, Result<Child, SpawnError>>;
}

impl<R: Blueprint> SpawnOnDyn for R {
    fn spawn_on_dyn<'a>(&'a self, data: &'a Channel) -> BoxFuture<'a, Result<Child, SpawnError>> {
        Box::pin(async move {
            let runner = self
                .build()
                .await
                .map_err(|x| SpawnError::Instantiation(x.into()))?
                .map_actor_exit(|res| res.map(|_| ()));

            data.downcast_ref::<<R::Actor as Actor>::Interface>()
                .expect("ProcessData should be of correct type")
                .clone()
                .spawn(|state| runner.run(state))
                .map(IntoDyn::into_dyn)
                .map_err(Into::into)
        })
    }
}

impl SpawnOn for DynRepeatSpawner {
    type Inbox = Set!();
    type Exit = ();

    async fn spawn_on(
        &self,
        data: Channel<Self::Inbox>,
    ) -> Result<Child<Self::Exit, Self::Inbox>, SpawnError> {
        self.0.spawn_on_dyn(&data).await
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
