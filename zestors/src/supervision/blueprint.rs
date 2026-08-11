use tokio::sync::watch;

use super::*;

pub trait Blueprint: Debug {
    type Runner: ActorRunner;

    fn instantiate(&self) -> Self::Runner;
}

impl<T: ActorRunner + Clone + Debug> Blueprint for T {
    type Runner = T;

    fn instantiate(&self) -> Self::Runner {
        self.clone()
    }
}

pub trait ActorBlueprintExt: Blueprint + Sized {
    fn into_spawn_fn(self) -> DynSpawner
    where
        Self: Send + Sync + 'static,
    {
        DynSpawner::new(self)
    }

    fn spawn(
        &self,
        pid: Pid,
    ) -> Child<<Self::Runner as ActorRunner>::Exit, <Self::Runner as ActorRunner>::Interface>
    where
        Self: Send + Sync + 'static,
    {
        self.instantiate().spawn(pid)
    }
}
impl<T: Blueprint> ActorBlueprintExt for T {}

// pub struct ExtractAddressBlueprint<T: Blueprint> {
//     pub(super) inner: T,
// }

// impl<T: Blueprint> Blueprint for ExtractAddressBlueprint<T> {
//     type Runner = ExtractAddressRunnable<T::Runner>;

//     fn instantiate(&self) -> Self::Runner {
//         ExtractAddressRunnable {
//             inner: self.inner.instantiate(),
//         }
//     }
// }

// impl<T: Blueprint> Debug for ExtractAddressBlueprint<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("ExtractAddressBlueprint")
//             .field("inner", &self.inner)
//             .finish()
//     }
// }
