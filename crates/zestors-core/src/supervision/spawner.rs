use super::*;
use futures::future::BoxFuture;

pub trait Launch: Into<DynLauncher> {
    type ChannelSpec: ChannelSpec;
    type Exit: Send + 'static;

    fn launch_on(
        &self,
        channel: Channel<Self::ChannelSpec>,
    ) -> impl Future<Output = Result<Child<Self::Exit, Self::ChannelSpec>, LaunchOnError>> + Send;
}

impl<B: Blueprint> Launch for B {
    type ChannelSpec = <B::Actor as Actor>::Interface;
    type Exit = <B::Actor as Actor>::Exit;

    async fn launch_on(
        &self,
        channel: Channel<Self::ChannelSpec>,
    ) -> Result<Child<Self::Exit, Self::ChannelSpec>, LaunchOnError> {
        let actor = self
            .instantiate()
            .await
            .map_err(|x| LaunchOnError::Instantiation(x.into()))?;

        channel.spawn(|state| actor.run(state)).map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct DynLauncher(Arc<dyn _Spawnable + Send + Sync + 'static>);

trait _Spawnable: Debug {
    fn spawn_on_dyn<'a>(&'a self, data: &'a Channel)
    -> BoxFuture<'a, Result<Child, LaunchOnError>>;
}

impl<R: Blueprint> _Spawnable for R {
    fn spawn_on_dyn<'a>(
        &'a self,
        data: &'a Channel,
    ) -> BoxFuture<'a, Result<Child, LaunchOnError>> {
        Box::pin(async move {
            let runner = self
                .instantiate()
                .await
                .map_err(|x| LaunchOnError::Instantiation(x.into()))?
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

impl Launch for DynLauncher {
    type ChannelSpec = Set!();
    type Exit = ();

    async fn launch_on(
        &self,
        data: Channel<Self::ChannelSpec>,
    ) -> Result<Child<Self::Exit, Self::ChannelSpec>, LaunchOnError> {
        self.0.spawn_on_dyn(&data).await
    }
}

impl DynLauncher {
    pub fn new<R>(blueprint: R) -> Self
    where
        R: Blueprint + Send + Sync + 'static,
    {
        DynLauncher(Arc::new(blueprint))
    }
}

impl<R> From<R> for DynLauncher
where
    R: Blueprint + Send + Sync + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}
