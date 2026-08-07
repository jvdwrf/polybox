use std::{
    pin::{Pin, pin},
    sync::OnceLock,
    task::{Poll, ready},
};

use super::*;
use tokio::sync::{Notify, mpsc, watch};

pub trait ActorRunner: Send + Sized + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn run(
        self,
        stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static;
}

pub trait ActorRunnerExt: ActorRunner {
    fn map<F, R>(self, map_exit: F) -> MapRun<Self, F>
    where
        F: FnOnce(Result<Self::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        MapRun::new(self, map_exit)
    }

    fn tap_err_mut<F>(
        self,
        map_err: F,
    ) -> MapRun<
        Self,
        impl FnOnce(Result<Self::Exit, anyhow::Error>) -> Result<Self::Exit, anyhow::Error>
        + Send
        + 'static,
    >
    where
        F: FnOnce(&mut anyhow::Error) + Send + 'static,
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
        F: FnOnce(Self, EventStream<Self::Interface>, Address<Self::Interface>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<E, anyhow::Error>> + Send + 'static,
        E: Send + 'static,
    {
        WrapRun::new(self, mapper)
    }

    fn extract_address(self) -> (ExtractAddressRunnable<Self>, FutureAddress<Self::Interface>) {
        let (rx, tx) = FutureAddress::new();
        (ExtractAddressRunnable { inner: self, tx }, rx)
    }

    fn spawn(self) -> Child<Self::Exit, Self::Interface> {
        crate::spawn(|stream, address| self.run(stream, address))
    }
}
impl<T: ActorRunner> ActorRunnerExt for T {}

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
        T: ActorRunner,
        F: FnOnce(Result<T::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, map_exit }
    }
}

impl<T, F, R> ActorRunner for MapRun<T, F>
where
    T: ActorRunner + Send + 'static,
    F: FnOnce(Result<T::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
    R: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = R;

    fn run(
        self,
        stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        let Self { inner, map_exit } = self;

        async move { map_exit(inner.run(stream, address).await) }
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
        T: ActorRunner,
        F: FnOnce(T, EventStream<T::Interface>, Address<T::Interface>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, mapper }
    }
}

impl<T, F, Fut, E> ActorRunner for WrapRun<T, F>
where
    T: ActorRunner + Send + 'static,
    F: FnOnce(T, EventStream<T::Interface>, Address<T::Interface>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<E, anyhow::Error>> + Send + 'static,
    E: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = E;

    fn run(
        self,
        stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        let Self { inner, mapper } = self;

        async move { mapper(inner, stream, address).await }
    }
}

#[derive(Debug, Clone)]
pub struct ExtractAddressRunnable<T: ActorRunner> {
    pub(super) inner: T,
    pub(super) tx: FutureAddressSender<T::Interface>,
}

impl<T> ActorRunner for ExtractAddressRunnable<T>
where
    T: ActorRunner + Send + 'static,
{
    type Interface = T::Interface;
    type Exit = T::Exit;

    fn run(
        self,
        stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        let Self { inner, tx } = self;

        async move {
            tx.send(Some(address.clone())).ok();
            inner.run(stream, address).await
        }
    }
}

pub struct FutureAddress<T: InboxKind> {
    receiver: watch::Receiver<Option<Address<T>>>,
}

pub(crate) type FutureAddressSender<T> = watch::Sender<Option<Address<T>>>;

impl<T: InboxKind> FutureAddress<T> {
    pub(super) fn new() -> (Self, FutureAddressSender<T>) {
        let (tx, rx) = watch::channel(None);
        (Self { receiver: rx }, tx)
    }

    pub async fn get(&mut self) -> Option<Address<T>> {
        self.await
    }
}

impl<T: InboxKind> Debug for FutureAddress<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FutureAddress").finish()
    }
}

impl<T: InboxKind> Clone for FutureAddress<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}

impl<T: InboxKind> Future for FutureAddress<T> {
    type Output = Option<Address<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let res = ready!(pin!(self.receiver.wait_for(|x| x.is_some())).poll_unpin(cx));

        Poll::Ready(
            res.ok()
                .map(|x| x.clone().expect("Wait for address that is some")),
        )
    }
}
