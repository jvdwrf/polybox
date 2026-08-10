use tokio::sync::watch;

use super::*;

pub trait ActorBlueprint {
    type Runner: ActorRunner;

    fn instantiate(&mut self) -> Self::Runner;
}

impl<T: ActorRunner + Clone> ActorBlueprint for T {
    type Runner = T;

    fn instantiate(&mut self) -> Self::Runner {
        self.clone()
    }
}

pub trait ActorBlueprintExt: ActorBlueprint + Sized {
    fn into_spawn_fn(self) -> DynSpawnFn
    where
        Self: Send + 'static,
    {
        DynSpawnFn::new(self)
    }

    fn extract_address(
        self,
    ) -> (
        ExtractAddressBlueprint<Self>,
        SupervisionAddress<<Self::Runner as ActorRunner>::Interface>,
    ) {
        let (rx, tx) = SupervisionAddress::new();
        (ExtractAddressBlueprint { inner: self, tx }, rx)
    }
}
impl<T: ActorBlueprint> ActorBlueprintExt for T {}

pub struct ExtractAddressBlueprint<T: ActorBlueprint> {
    inner: T,
    tx: watch::Sender<Option<Address<<T::Runner as ActorRunner>::Interface>>>,
}

impl<T: ActorBlueprint> ActorBlueprint for ExtractAddressBlueprint<T> {
    type Runner = ExtractAddressRunnable<T::Runner>;

    fn instantiate(&mut self) -> Self::Runner {
        ExtractAddressRunnable {
            inner: self.inner.instantiate(),
            tx: self.tx.clone(),
        }
    }
}
