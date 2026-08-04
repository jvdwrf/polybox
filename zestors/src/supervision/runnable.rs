use tokio::sync::mpsc;

use super::*;

pub struct Runnable(Box<dyn Fn() -> Child + Send>);

impl Runnable {
    pub fn new<T: Run + Clone>(value: T) -> Self {
        Self(Box::new(move || {
            value
                .clone()
                .map(|_exit| {
                    _exit.map(|_exit| {
                        ()
                        // TODO: do something with the exit value
                    })
                })
                .spawn()
                .into_dyn_subset()
        }))
    }

    pub fn from_inner(f: Box<dyn Fn() -> Child + Send>) -> Self {
        Self(f)
    }

    pub fn from_fn<T, F>(f: impl Fn(EventStream<T>, Address<T>) -> F + Send + 'static) -> Self
    where
        T: Interface,
        F: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
        F::Output: Send + 'static,
    {
        let spawn_fn = move || crate::spawn(|stream, address| f(stream, address)).into_dyn_subset();

        Self::from_inner(Box::new(spawn_fn))
    }

    pub fn spawn(&self) -> Child {
        (self.0)()
    }
}

impl<T> From<T> for Runnable
where
    T: Run + Clone,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl From<Box<dyn Fn() -> Child + Send>> for Runnable {
    fn from(value: Box<dyn Fn() -> Child + Send>) -> Self {
        Self::from_inner(value)
    }
}

pub trait Run: Send + Sized + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn run(
        self,
        stream: EventStream<Self::Interface>,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static;
}

pub trait RunnableExt: Run {
    fn map<F, R>(self, map_exit: F) -> MapRunnable<Self, F>
    where
        F: FnOnce(Result<Self::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        MapRunnable::new(self, map_exit)
    }

    // fn map_ok<F, R>(
    //     self,
    //     map_ok: F,
    // ) -> MapRunnable<
    //     Self,
    //     impl FnOnce(Result<Self::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
    // >
    // where
    //     F: FnOnce(Self::Exit) -> R + Send + 'static,
    //     R: Send + 'static,
    // {
    //     self.map(move |exit| match exit {
    //         Ok(value) => Ok(map_ok(value)),
    //         Err(e) => Err(e),
    //     })
    // }

    fn tap_err_mut<F>(
        self,
        map_err: F,
    ) -> MapRunnable<
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

    fn wrap<F, Fut, E>(self, mapper: F) -> WrapRunnable<Self, F>
    where
        F: FnOnce(Self, EventStream<Self::Interface>, Address<Self::Interface>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<E, anyhow::Error>> + Send + 'static,
        E: Send + 'static,
    {
        WrapRunnable::new(self, mapper)
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

    // fn supervise(self, supervisor: &mut Supervisor) -> Address<Self::Interface> {
    //     let (child, address) = self.spawn();
    //     supervisor.add_child(child);
    //     address
    // }

    fn spawn(self) -> Child<Self::Exit, Self::Interface> {
        crate::spawn(|stream, address| self.run(stream, address))
    }
}
impl<T: Run> RunnableExt for T {}

#[derive(Clone)]
pub struct MapRunnable<T, F> {
    inner: T,
    map_exit: F,
}

impl<T, F> Debug for MapRunnable<T, F>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedExitRunnable")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, F> MapRunnable<T, F> {
    pub fn new<R>(inner: T, map_exit: F) -> Self
    where
        T: Run,
        F: FnOnce(Result<T::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, map_exit }
    }
}

impl<T, F, R> Run for MapRunnable<T, F>
where
    T: Run + Send + 'static,
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
pub struct WrapRunnable<T, F> {
    inner: T,
    mapper: F,
}

impl<T, F> Debug for WrapRunnable<T, F>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreRunRunnable")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, F> WrapRunnable<T, F> {
    pub fn new<R, Fut>(inner: T, mapper: F) -> Self
    where
        T: Run,
        F: FnOnce(T, EventStream<T::Interface>, Address<T::Interface>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, mapper }
    }
}

impl<T, F, Fut, E> Run for WrapRunnable<T, F>
where
    T: Run + Send + 'static,
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
pub struct ExtractAddressRunnable<T: Run> {
    inner: T,
    tx: mpsc::UnboundedSender<Address<T::Interface>>,
}

impl<T> Run for ExtractAddressRunnable<T>
where
    T: Run + Send + 'static,
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
    use tokio::sync::mpsc;

    use super::*;

    #[derive(Interface, ActorInterface, Debug)]
    #[interface(crate = "crate")]
    pub enum TestInterface {
        Num(Payload<u32>),
    }

    #[derive(Clone, Debug)]
    pub struct TestRunnable {
        number: u32,
    }

    impl Actor for TestRunnable {
        type Interface = TestInterface;
        type Exit = ();
        type Error = anyhow::Error;

        async fn exit(&mut self, _: ExitReason) -> Result<Self::Exit, Self::Error> {
            Ok(())
        }
    }

    impl HandleMessage<u32> for TestRunnable {
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

    #[tokio::test]
    async fn test_map_exit_runnable() {
        let (runnable, address) = TestRunnable { number: 42 }.extract_address();

        let supervisor = Supervisor::new()
            .with_strategy(SupervisionStrategy::OneForOne)
            .with_intensity(RestartIntensity::default())
            .with_children([
                ChildSpec::new(
                    "ChildA",
                    TestRunnable { number: 42 }
                        .map(|exit| exit.map(|()| 12))
                        .wrap(|inner, stream, address| async move {
                            inner.run(stream, address).await.map(|val| val.to_string())
                        }),
                ),
                ChildSpec::new("ChildB", TestRunnable { number: 42 })
                    .mode(RestartMode::Always)
                    .timeout(Duration::from_secs(10)),
                ChildSpec::new(
                    "ChildC",
                    Runnable::from_fn(async move |mut stream: EventStream<TestInterface>, _| {
                        while let Some(msg) = stream.recv().await {
                            println!("Received message: {:?}", msg);
                        }

                        Ok(())
                    }),
                ),
            ])
            .spawn();

        tokio::time::sleep(Duration::from_secs(1)).await;
        supervisor.signal_shutdown().await.ok();
        supervisor.await.unwrap();
    }
}
