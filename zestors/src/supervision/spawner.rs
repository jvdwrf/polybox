use super::*;
use polybox::type_sets::Set;

pub struct DynSpawnFn(Box<dyn FnMut() -> Child + Send>);

impl DynSpawnFn {
    pub fn new<R>(mut value: R) -> Self
    where
        R: ActorBlueprint + Send + 'static,
    {
        Self(Box::new(move || {
            value
                .create_runner()
                .map(|e| e.map(|_| ()))
                .spawn()
                .into_dyn()
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
    type Inbox = <T::Runner as ActorRunner>::Inbox;

    fn spawn_mut(&mut self) -> Child<Self::Exit, Self::Inbox> {
        self.create_runner().spawn()
    }
}
