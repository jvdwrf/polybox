use crate::_prelude::*;

pub struct ActorState<I: Interface> {
    stream: EventStream<I>,
}

impl<I: Interface> ActorState<I> {
    pub(crate) fn new(stream: EventStream<I>) -> Self {
        Self { stream }
    }

    pub fn debug_state(&self, actor: impl Debug) -> DebugState {
        DebugState {
            status: self.status(),
            uptime: self.uptime().unwrap_or_default(),
            description: format!("{actor:?}"),
        }
    }

    pub async fn next(&mut self) -> Option<Event<I>> {
        self.stream.recv().await
    }
}

impl<I: Interface> AsActorRef for ActorState<I> {
    type QueueType = I;

    fn as_channel(&self) -> &Channel<Self::QueueType> {
        self.stream.as_channel()
    }
}

pub trait Actor: Send + Sized + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn run(
        self,
        state: ActorState<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, Report>> + Send + 'static;
}

pub trait ActorRunnerExt: Actor {
    fn map<F, R>(self, map_exit: F) -> MapRun<Self, F>
    where
        F: FnOnce(Result<Self::Exit, Report>) -> Result<R, Report> + Send + 'static,
        R: Send + 'static,
    {
        MapRun::new(self, map_exit)
    }

    fn tap_err_mut<F>(
        self,
        map_err: F,
    ) -> MapRun<
        Self,
        impl FnOnce(Result<Self::Exit, Report>) -> Result<Self::Exit, Report> + Send + 'static,
    >
    where
        F: FnOnce(&mut Report) + Send + 'static,
    {
        self.map(move |exit| match exit {
            Ok(value) => Ok(value),
            Err(mut e) => {
                map_err(&mut e);
                Err(e)
            }
        })
    }

    fn wrap<F, Fut, E>(self, mapper: F) -> WrapRun<Self, F>
    where
        F: FnOnce(Self, ActorState<Self::Interface>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<E, Report>> + Send + 'static,
        E: Send + 'static,
    {
        WrapRun::new(self, mapper)
    }

    fn spawn(self, pid: Pid) -> Child<Self::Exit, Self::Interface> {
        crate::spawn(pid, |state| self.run(state))
    }
}
impl<T: Actor> ActorRunnerExt for T {}

#[derive(Clone)]
pub struct MapRun<T, F> {
    inner: T,
    map_exit: F,
}

impl<T, F> Debug for MapRun<T, F>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapRun")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, F> MapRun<T, F> {
    pub fn new<R>(inner: T, map_exit: F) -> Self
    where
        T: Actor,
        F: FnOnce(Result<T::Exit, Report>) -> Result<R, Report> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, map_exit }
    }
}

impl<T, F, R> Actor for MapRun<T, F>
where
    T: Actor + Send + 'static,
    F: FnOnce(Result<T::Exit, Report>) -> Result<R, Report> + Send + 'static,
    R: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = R;

    fn run(
        self,
        state: ActorState<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, Report>> + Send + 'static {
        let Self { inner, map_exit } = self;

        async move { map_exit(inner.run(state).await) }
    }
}

#[derive(Clone)]
pub struct WrapRun<T, F> {
    inner: T,
    mapper: F,
}

impl<T, F> Debug for WrapRun<T, F>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrapRun")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, F> WrapRun<T, F> {
    pub fn new<R, Fut>(inner: T, mapper: F) -> Self
    where
        T: Actor,
        F: FnOnce(T, ActorState<T::Interface>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Report>> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, mapper }
    }
}

impl<T, F, Fut, E> Actor for WrapRun<T, F>
where
    T: Actor + Send + 'static,
    F: FnOnce(T, ActorState<T::Interface>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    E: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = E;

    fn run(
        self,
        state: ActorState<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, Report>> + Send + 'static {
        let Self { inner, mapper } = self;

        async move { mapper(inner, state).await }
    }
}

//
