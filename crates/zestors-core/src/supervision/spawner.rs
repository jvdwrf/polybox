use crate::_prelude::*;
use futures::future::BoxFuture;

pub trait Start: Into<DynLauncher> {
    type Ctx: Context;
    type Exit: Send + 'static;

    fn start_on(
        &self,
        channel: Channel<Self::Ctx>,
    ) -> impl Future<Output = Result<Child<Self::Exit, Self::Ctx>, StartOnError>> + Send;
}

impl<B: Blueprint> Start for B {
    type Ctx = <B::Actor as Actor>::Interface;
    type Exit = <B::Actor as Actor>::Exit;

    async fn start_on(
        &self,
        channel: Channel<Self::Ctx>,
    ) -> Result<Child<Self::Exit, Self::Ctx>, StartOnError> {
        let actor = self
            .instantiate()
            .await
            .map_err(|x| StartOnError::Instantiation(x.into()))?;

        channel.spawn(|state| actor.run(state)).map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct DynLauncher(Arc<dyn _Spawnable + Send + Sync + 'static>);

trait _Spawnable: Debug {
    fn spawn_on_dyn<'a>(&'a self, data: &'a Channel) -> BoxFuture<'a, Result<Child, StartOnError>>;
}

impl<R: Blueprint> _Spawnable for R {
    fn spawn_on_dyn<'a>(&'a self, data: &'a Channel) -> BoxFuture<'a, Result<Child, StartOnError>> {
        Box::pin(async move {
            let runner = self
                .instantiate()
                .await
                .map_err(|x| StartOnError::Instantiation(x.into()))?
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

impl Start for DynLauncher {
    type Ctx = Set!();
    type Exit = ();

    async fn start_on(
        &self,
        data: Channel<Self::Ctx>,
    ) -> Result<Child<Self::Exit, Self::Ctx>, StartOnError> {
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
