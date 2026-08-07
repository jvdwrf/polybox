use super::*;
use polybox::type_sets::Set;
use tokio::sync::{mpsc, watch};

pub struct DynSpawnFn(Box<dyn FnMut() -> Child + Send>);

impl DynSpawnFn {
    pub fn new<R>(mut value: R) -> Self
    where
        R: ActorBlueprint + Send + 'static,
    {
        Self(Box::new(move || {
            value.build().map(|e| e.map(|_| ())).spawn().into_dyn()
        }))
    }

    pub fn from_fn<T: InboxKind>(
        mut spawn_fn: impl FnMut() -> Child<(), T> + Send + 'static,
    ) -> Self {
        Self(Box::new(move || spawn_fn().into_dyn().attached()))
    }

    pub fn call(&mut self) -> Child {
        (self.0)()
    }
}

impl<R> From<R> for DynSpawnFn
where
    R: ActorBlueprint + Send + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}

impl Debug for DynSpawnFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DynSpawnFn").finish()
    }
}

pub trait ActorSpawner {
    type Exit: Send + 'static;
    type Inbox: InboxKind;

    fn spawn_mut(&mut self) -> Child<Self::Exit, Self::Inbox>;
}

impl ActorSpawner for DynSpawnFn {
    type Exit = ();
    type Inbox = Dyn<Set![]>;

    fn spawn_mut(&mut self) -> Child {
        self.call()
    }
}

impl<T: ActorBlueprint> ActorSpawner for T {
    type Exit = <T::Runner as ActorRunner>::Exit;
    type Inbox = <T::Runner as ActorRunner>::Interface;

    fn spawn_mut(&mut self) -> Child<Self::Exit, Self::Inbox> {
        self.build().spawn()
    }
}

// pub trait ActorSpawnerExt: ActorSpawner + Sized {
// fn extract_address(
//     self,
// ) -> (
//     ExtractAddress<Self>,
//     mpsc::UnboundedReceiver<Address<Self::Inbox>>,
// ) {
//     let (tx, rx) = mpsc::unbounded_channel();
//     (ExtractAddress { inner: self, tx }, rx)
// }
// }
// impl<T: ActorSpawner> ActorSpawnerExt for T {}

// #[derive(Debug)]
// pub struct ExtractAddress<T: ActorSpawner> {
//     inner: T,
//     tx: watch::Sender<Address<T::Inbox>>,
// }

// impl<T: ActorSpawner> ActorSpawner for ExtractAddress<T> {
//     type Inbox = T::Inbox;
//     type Exit = T::Exit;

//     fn spawn_mut(&mut self) -> Child<Self::Exit, Self::Inbox> {
//         let Self { inner, tx } = self;
//         let child = inner.spawn_mut();
//         tx.send(child.address().clone()).ok();
//         child
//     }
// }

// impl<T: ActorSpawner + Clone> Clone for ExtractAddress<T> {
//     fn clone(&self) -> Self {
//         Self {
//             inner: self.inner.clone(),
//             tx: self.tx.clone(),
//         }
//     }
// }
