use crate::_prelude::*;

pub struct ActorState<I: Interface> {
    status: ActorStatus,
    shutdown_at: Option<tokio::time::Instant>,
    start_time: tokio::time::Instant,
    address: Address<I>,
    stream: EventStream<I>,
}

impl<I: Interface> ActorState<I> {
    pub(crate) fn new(stream: EventStream<I>, address: Address<I>) -> Self {
        Self {
            status: ActorStatus::Running,
            start_time: tokio::time::Instant::now(),
            address,
            stream,
            shutdown_at: None,
        }
    }

    pub fn status(&self) -> ActorStatus {
        self.status
    }

    pub fn address(&self) -> &Address<I> {
        &self.address
    }

    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    pub fn debug_state(&self, actor: impl Debug) -> DebugState {
        DebugState {
            status: self.status,
            uptime: self.uptime(),
            description: format!("{actor:?}"),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stream.is_empty()
    }

    pub async fn next(&mut self) -> Option<ActorEvent<I>> {
        loop {
            match self
                .stream
                .recv_enabled(self.status != ActorStatus::Suspended)
                .await?
            {
                Event::Message(msg) => break Some(ActorEvent::Message(msg)),
                Event::Signal(signal) => match signal {
                    SignalInterface::Shutdown(_) => {
                        self.status = ActorStatus::ShuttingDown;
                        if self.shutdown_at.is_none() {
                            self.shutdown_at = Some(tokio::time::Instant::now());
                        }
                        break Some(ActorEvent::Signal(SignalEvent::StatusUpdate(
                            ActorStatus::ShuttingDown,
                        )));
                    }
                    SignalInterface::Suspend(_) => {
                        if self.status.should_exit() {
                            tracing::warn!("Actor is exiting, cannot suspend");
                            break None;
                        }
                        self.status = ActorStatus::Suspended;
                        break Some(ActorEvent::Signal(SignalEvent::StatusUpdate(
                            ActorStatus::Suspended,
                        )));
                    }
                    SignalInterface::Resume(_) => {
                        if self.status != ActorStatus::Suspended {
                            tracing::warn!("Actor is not suspended, cannot resume");
                            break None;
                        }
                        self.status = ActorStatus::Running;
                        break Some(ActorEvent::Signal(SignalEvent::StatusUpdate(
                            ActorStatus::Running,
                        )));
                    }
                    SignalInterface::GetState((_, tx)) => {
                        break Some(ActorEvent::Signal(SignalEvent::GetState(tx)));
                    }
                    SignalInterface::GetChildren((_, tx)) => {
                        break Some(ActorEvent::Signal(SignalEvent::GetChildren(tx)));
                    }
                    SignalInterface::GetStatus((_, tx)) => {
                        let _ = tx.send(self.status);
                    }
                    SignalInterface::Ping((_, tx)) => {
                        let _ = tx.send(());
                    }
                },
            }
        }
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
        impl FnOnce(Result<Self::Exit, Report>) -> Result<Self::Exit, Report>
        + Send
        + 'static,
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
