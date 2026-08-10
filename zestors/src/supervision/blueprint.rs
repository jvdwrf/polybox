use tokio::sync::watch;

use super::*;

pub trait ActorBlueprint: Debug {
    type Runner: ActorRunner;

    fn instantiate(&self) -> Self::Runner;
}

impl<T: ActorRunner + Clone + Debug> ActorBlueprint for T {
    type Runner = T;

    fn instantiate(&self) -> Self::Runner {
        self.clone()
    }
}

pub trait ActorBlueprintExt: ActorBlueprint + Sized {
    fn into_spawn_fn(self) -> DynSpawner
    where
        Self: Send + Sync + 'static,
    {
        DynSpawner::new(self)
    }

    fn split_address(
        self,
    ) -> (
        ExtractAddressBlueprint<Self>,
        AddressFuture<<Self::Runner as ActorRunner>::Interface>,
    ) {
        let (rx, tx) = AddressFuture::new();
        (ExtractAddressBlueprint { inner: self, tx }, rx)
    }

    fn spawn_ref(
        &self,
    ) -> Child<<Self::Runner as ActorRunner>::Exit, <Self::Runner as ActorRunner>::Interface>
    where
        Self: Send + Sync + 'static,
    {
        self.instantiate().spawn()
    }
}
impl<T: ActorBlueprint> ActorBlueprintExt for T {}

pub struct ExtractAddressBlueprint<T: ActorBlueprint> {
    pub(super) inner: T,
    pub(super) tx: watch::Sender<Option<Address<<T::Runner as ActorRunner>::Interface>>>,
}

impl<T: ActorBlueprint> ActorBlueprint for ExtractAddressBlueprint<T> {
    type Runner = ExtractAddressRunnable<T::Runner>;

    fn instantiate(&self) -> Self::Runner {
        ExtractAddressRunnable {
            inner: self.inner.instantiate(),
            tx: self.tx.clone(),
        }
    }
}

impl<T: ActorBlueprint> Debug for ExtractAddressBlueprint<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtractAddressBlueprint")
            .field("inner", &self.inner)
            .finish()
    }
}
