use crate::_prelude::*;

pub trait Actor: Send + Sized + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn run(
        self,
        state: EventStream<Self::Interface>,
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
        F: FnOnce(Self, EventStream<Self::Interface>) -> Fut + Send + 'static,
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
        state: EventStream<Self::Interface>,
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
        F: FnOnce(T, EventStream<T::Interface>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, Report>> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, mapper }
    }
}

impl<T, F, Fut, E> Actor for WrapRun<T, F>
where
    T: Actor + Send + 'static,
    F: FnOnce(T, EventStream<T::Interface>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    E: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = E;

    fn run(
        self,
        state: EventStream<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, Report>> + Send + 'static {
        let Self { inner, mapper } = self;

        async move { mapper(inner, state).await }
    }
}

pub struct FnActor<F, Fut, I, E>
where
    F: FnOnce(EventStream<I>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    I: Interface,
    E: Send + 'static,
{
    f: F,
    _phantom: std::marker::PhantomData<fn() -> (I, E, Fut)>,
}

impl<F, Fut, I, E> FnActor<F, Fut, I, E>
where
    F: FnOnce(EventStream<I>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    I: Interface,
    E: Send + 'static,
{
    pub fn new(f: F) -> Self {
        Self {
            f,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<F, Fut, I, E> Actor for FnActor<F, Fut, I, E>
where
    F: FnOnce(EventStream<I>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    I: Interface,
    E: Send + 'static,
{
    type Interface = I;
    type Exit = E;

    fn run(
        self,
        state: EventStream<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, Report>> + Send + 'static {
        (self.f)(state)
    }
}

impl<F, Fut, I, E> Debug for FnActor<F, Fut, I, E>
where
    F: FnOnce(EventStream<I>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    I: Interface,
    E: Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnActor")
            .field("interface", &std::any::type_name::<I>())
            .field("exit", &std::any::type_name::<E>())
            .finish()
    }
}

impl<F, Fut, I, E> Clone for FnActor<F, Fut, I, E>
where
    F: Clone + FnOnce(EventStream<I>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    I: Interface,
    E: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

pub fn new_actor<F, Fut, I, E>(f: F) -> FnActor<F, Fut, I, E>
where
    F: FnOnce(EventStream<I>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, Report>> + Send + 'static,
    I: Interface,
    E: Send + 'static,
{
    FnActor::new(f)
}
