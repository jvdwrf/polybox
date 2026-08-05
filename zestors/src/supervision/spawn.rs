use super::*;

pub struct SpawnFn(Box<dyn FnMut() -> Child + Send>);

impl SpawnFn {
    pub fn new<R>(mut value: R) -> Self
    where
        R: InitActor + Send + 'static,
    {
        Self::from_inner(Box::new(move || {
            value
                .init()
                .map(|e| e.map(|_| ()))
                .spawn()
                .into_dyn_subset()
        }))
    }

    pub fn from_inner(f: Box<dyn FnMut() -> Child + Send>) -> Self {
        Self(f)
    }

    pub fn from_fn<T: InboxKind>(mut f: impl FnMut() -> Child<(), T> + Send + 'static) -> Self {
        Self::from_inner(Box::new(move || f().into_dyn_subset()))
    }

    pub fn spawn(&mut self) -> Child {
        (self.0)()
    }
}

impl<R> From<R> for SpawnFn
where
    R: InitActor + Send + 'static,
{
    fn from(value: R) -> Self {
        Self::new(value)
    }
}

impl Debug for SpawnFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SpawnFn").finish()
    }
}

pub trait InitActor {
    type Actor: Runnable;

    fn init(&mut self) -> Self::Actor;
}

impl<T> InitActor for T
where
    T: Runnable + Clone,
{
    type Actor = T;

    fn init(&mut self) -> Self::Actor {
        self.clone()
    }
}

// use crate::_prelude::*;
// use std::fmt::Debug;

// pub trait Spawn {
//     type Inbox: InboxKind;
//     type Exit: Send + 'static;

//     fn spawn(self) -> Child<Self::Exit, Self::Inbox>;
// }

// impl<T: Run> Spawn for T {
//     type Inbox = T::Interface;
//     type Exit = T::Exit;

//     fn spawn(self) -> Child<Self::Exit, Self::Inbox> {
//         crate::spawn(|stream, address| self.run(stream, address))
//     }
// }

// pub trait SpawnExt: Spawn + Sized {
//     fn map_spawn<F, E, I>(self, map_exit: F) -> MapSpawn<Self, F>
//     where
//         F: Fn(Child<Self::Exit, Self::Inbox>) -> Child<E, I> + Send + 'static,
//         I: InboxKind,
//         E: Send + 'static,
//     {
//         MapSpawn::new(self, map_exit)
//     }
// }
// impl<T: Spawn> SpawnExt for T {}

// #[derive(Clone)]
// pub struct MapSpawn<T, F> {
//     inner: T,
//     map_exit: F,
// }

// impl<T: Debug, F> Debug for MapSpawn<T, F> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("MapSpawn")
//             .field("inner", &self.inner)
//             .finish()
//     }
// }

// impl<T: Spawn, F> MapSpawn<T, F> {
//     pub fn new<E, I>(inner: T, map_exit: F) -> Self
//     where
//         F: Fn(Child<T::Exit, T::Inbox>) -> Child<E, I> + Send + 'static,
//         I: InboxKind,
//         E: Send + 'static,
//     {
//         Self { inner, map_exit }
//     }
// }

// impl<T: Spawn, F, E, I> Spawn for MapSpawn<T, F>
// where
//     F: Fn(Child<T::Exit, T::Inbox>) -> Child<E, I> + Send + 'static,
//     I: InboxKind,
//     E: Send + 'static,
// {
//     type Inbox = I;
//     type Exit = E;

//     fn spawn(self) -> Child<Self::Exit, Self::Inbox> {
//         let Self { inner, map_exit } = self;
//         map_exit(inner.spawn())
//     }
// }
