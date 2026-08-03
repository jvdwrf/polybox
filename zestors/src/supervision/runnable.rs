use super::*;

pub trait Runnable: Send + Sized + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn run(
        self,
        receiver: Receiver<Self::Interface>,
        signal_receiver: SignalReceiver,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static;
}

pub trait RunnableExt: Runnable {
    fn map<F, R>(self, map_exit: F) -> MapRunnable<Self, F>
    where
        F: FnOnce(Result<Self::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        MapRunnable::new(self, map_exit)
    }

    fn map_ok<F, R>(
        self,
        map_ok: F,
    ) -> MapRunnable<
        Self,
        impl FnOnce(Result<Self::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
    >
    where
        F: FnOnce(Self::Exit) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.map(move |exit| match exit {
            Ok(value) => Ok(map_ok(value)),
            Err(e) => Err(e),
        })
    }

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
        F: FnOnce(Self, Receiver<Self::Interface>, SignalReceiver, Address<Self::Interface>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<E, anyhow::Error>> + Send + 'static,
        E: Send + 'static,
    {
        WrapRunnable::new(self, mapper)
    }

    fn spawn(self) -> (Child<Self::Exit>, Address<Self::Interface>) {
        let (address, child) =
            crate::spawn(|rx, signal_rx, address| self.run(rx, signal_rx, address));

        (child, address)
    }
}
impl<T: Runnable> RunnableExt for T {}

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
        T: Runnable,
        F: FnOnce(Result<T::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, map_exit }
    }
}

impl<T, F, R> Runnable for MapRunnable<T, F>
where
    T: Runnable + Send + 'static,
    F: FnOnce(Result<T::Exit, anyhow::Error>) -> Result<R, anyhow::Error> + Send + 'static,
    R: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = R;

    fn run(
        self,
        receiver: Receiver<Self::Interface>,
        signal_receiver: SignalReceiver,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        let Self { inner, map_exit } = self;

        async move { map_exit(inner.run(receiver, signal_receiver, address).await) }
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
        T: Runnable,
        F: FnOnce(T, Receiver<T::Interface>, SignalReceiver, Address<T::Interface>) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
        R: Send + 'static,
    {
        Self { inner, mapper }
    }
}

impl<T, F, Fut, E> Runnable for WrapRunnable<T, F>
where
    T: Runnable + Send + 'static,
    F: FnOnce(T, Receiver<T::Interface>, SignalReceiver, Address<T::Interface>) -> Fut
        + Send
        + 'static,
    Fut: Future<Output = Result<E, anyhow::Error>> + Send + 'static,
    E: Send + 'static,
{
    type Interface = T::Interface;
    type Exit = E;

    fn run(
        self,
        receiver: Receiver<Self::Interface>,
        signal_receiver: SignalReceiver,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
        let Self { inner, mapper } = self;

        async move { mapper(inner, receiver, signal_receiver, address).await }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Interface, Debug)]
    #[interface(crate = "crate")]
    pub enum TestInterface {}

    pub struct TestRunnable;

    impl Runnable for TestRunnable {
        type Interface = TestInterface;
        type Exit = ();

        fn run(
            self,
            _receiver: Receiver<Self::Interface>,
            _signal_receiver: SignalReceiver,
            _address: Address<Self::Interface>,
        ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static {
            async move { Ok(()) }
        }
    }

    #[tokio::test]
    async fn test_map_exit_runnable() {
        let runnable = TestRunnable
            .map(|exit| match exit {
                Ok(_) => Ok(12u32),
                Err(_) => Err(anyhow::anyhow!("error")),
            })
            .wrap(|runnable, r, s, a| async move {
                runnable
                    .run(r, s, a)
                    .await
                    .map_err(|e| {
                        eprintln!("Error: {:?}", e);
                        e
                    })
                    .map(|val| val.to_string())
            });

        let (child, address) = runnable.spawn();
    }
}
