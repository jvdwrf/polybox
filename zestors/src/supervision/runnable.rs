use super::*;
use tokio::sync::mpsc;

pub trait Runnable: Send + Sized + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn run(
        self,
        stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static;
}

pub trait RunnableExt: Runnable {
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

    fn extract_address(
        self,
    ) -> (
        ExtractAddressRunnable<Self>,
        mpsc::UnboundedReceiver<Address<Self::Interface>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ExtractAddressRunnable { inner: self, tx }, rx)
    }

    fn spawn(self) -> Child<Self::Exit, Self::Interface> {
        crate::spawn(|stream, address| self.run(stream, address))
    }
}
impl<T: Runnable> RunnableExt for T {}

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
        T: Runnable,
        F: FnOnce(Result<T::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, map_exit }
    }
}

impl<T, F, R> Runnable for MapRun<T, F>
where
    T: Runnable + Send + 'static,
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
        T: Runnable,
        F: FnOnce(T, EventStream<T::Interface>, Address<T::Interface>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, mapper }
    }
}

impl<T, F, Fut, E> Runnable for WrapRun<T, F>
where
    T: Runnable + Send + 'static,
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
pub struct ExtractAddressRunnable<T: Runnable> {
    inner: T,
    tx: mpsc::UnboundedSender<Address<T::Interface>>,
}

impl<T> Runnable for ExtractAddressRunnable<T>
where
    T: Runnable + Send + 'static,
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
            tx.send(address.clone()).ok();
            inner.run(stream, address).await
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Interface, ActorInterface, Debug)]
    #[interface(crate = "crate")]
    pub enum TestInterface {
        Num(Payload<u32>),
    }

    #[derive(Clone, Debug)]
    pub struct TestActor {
        number: u32,
    }

    impl Actor for TestActor {
        type Interface = TestInterface;
        type Exit = ();
        type Error = anyhow::Error;

        async fn exit(&mut self, _: ExitReason) -> Result<Self::Exit, Self::Error> {
            Ok(())
        }
    }

    impl HandleMessage<u32> for TestActor {
        async fn handle_message(
            &mut self,
            _state: &mut ActorState<Self>,
            msg: Payload<u32>,
        ) -> Result<(), Self::Error> {
            println!("Received message: {:?}", msg);
            self.number += msg;
            Ok(())
        }
    }

    pub struct TestActorStarter(u32);

    impl InitActor for TestActorStarter {
        type Actor = TestActor;

        fn init(&mut self) -> Self::Actor {
            let actor = TestActor { number: self.0 };
            self.0 += 1;
            actor
        }
    }

    #[tokio::test]
    async fn test_map_exit_runnable() {
        // let (runnable, address) = TestRunnable { number: 42 }.extract_address();

        let supervisor = Supervisor::new()
            .with_strategy(SupervisionStrategy::OneForOne)
            .with_intensity(RestartIntensity::default())
            .with_children([
                ChildSpec::new(
                    "ChildA",
                    TestActor { number: 42 }.map(|exit| exit.map(|()| 12)).wrap(
                        |inner, stream, address| async move {
                            inner.run(stream, address).await.map(|val| val.to_string())
                        },
                    ),
                ),
                ChildSpec::new("ChildB", TestActor { number: 42 })
                    .mode(RestartMode::Always)
                    .timeout(Duration::from_secs(10)),
                ChildSpec::new(
                    "ChildC",
                    SpawnFn::from_fn(|| TestActor { number: 12 }.spawn()),
                ),
                ChildSpec::new("ChildD", TestActorStarter(0)),
            ])
            .spawn();

        tokio::time::sleep(Duration::from_secs(1)).await;
        supervisor.signal_shutdown().await.ok();
        supervisor.await.unwrap();
    }
}
