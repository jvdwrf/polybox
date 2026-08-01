use futures::{FutureExt, StreamExt as _, stream::FuturesUnordered};

use crate::_prelude::*;
use std::{
    any::Any,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Poll, ready},
    time::Duration,
};

#[derive(Debug)]
pub struct Supervisor {
    children: FuturesUnordered<SupervisedChild>,
}

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum SupervisorInterface {}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            children: Default::default(),
        }
    }

    pub fn add_child(&mut self, spec: ChildSpec) {
        let child = SupervisedChild {
            reference: None,
            spec,
        };
        self.children.push(child);
    }

    pub fn with_child(mut self, spec: ChildSpec) -> Self {
        self.add_child(spec);
        self
    }

    pub fn spawn(mut self) -> (AnyChild, DynAddress) {
        let (address, child) = crate::spawn(
            async move |mut rx: Receiver<SupervisorInterface>, mut signal_rx, _| {
                loop {
                    let next = tokio::select! {
                        biased;

                        Some(msg) = signal_rx.recv_with(&mut rx) => {
                            SupervisionSelect::Signal(msg)
                        }

                        Some(child_exit) = self.children.next() => {
                            SupervisionSelect::ChildExit(child_exit)
                        }

                        _ = futures::future::pending::<()>() => {
                            break;
                        }
                    };

                    match next {
                        SupervisionSelect::Signal(_) => todo!(),
                        SupervisionSelect::ChildExit((spec, result)) => todo!(),
                    }
                }

                Ok(())
            },
        );

        (child.into_any(), address.into_dyn_subset())
    }
}

enum SupervisionSelect {
    Signal(SignalOrMessage<SupervisorInterface>),
    ChildExit((ChildSpec, Result<Box<dyn Any + Send>, JoinError>)),
}

#[derive(Debug)]
struct SupervisedChild {
    reference: Option<ChildReference>,
    spec: ChildSpec,
}

impl Future for SupervisedChild {
    type Output = (ChildSpec, Result<Box<dyn Any + Send>, JoinError>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let res = self
            .reference
            .as_mut()
            .map(|reference| reference.child.poll_unpin(cx))
            .unwrap_or(Poll::Pending);

        match ready!(res) {
            Ok(value) => Poll::Ready((self.spec.clone(), Ok(value))),
            Err(err) => Poll::Ready((self.spec.clone(), Err(err))),
        }
    }
}

#[derive(Debug)]
struct ChildReference {
    child: AnyChild,
    address: DynAddress,
}

#[derive(Debug, Clone)]
pub struct ChildSpec {
    inner: Arc<dyn DynChildSpec + Send + Sync>,
}

impl ChildSpec {
    pub fn new<T: Supervisable>(supervisable: T) -> Self {
        Self {
            inner: Arc::new(supervisable),
        }
    }

    pub fn spawn(&self) -> (AnyChild, DynAddress) {
        self.inner.spawn()
    }

    pub fn id(&self) -> String {
        self.inner.id()
    }

    pub fn restart_mode(&self) -> RestartMode {
        self.inner.restart_mode()
    }

    pub fn abort_timeout(&self) -> Duration {
        self.inner.abort_timeout()
    }
}

pub trait Supervisable: Clone + Debug + Send + Sync + 'static {
    type Interface: Interface;
    type Exit: Send + 'static;

    fn id(&self) -> String;

    fn run(
        self,
        receiver: Receiver<Self::Interface>,
        signal_receiver: SignalReceiver,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static;

    fn restart_mode(&self) -> RestartMode;

    fn abort_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }
}

trait DynChildSpec: Debug {
    fn id(&self) -> String;
    fn restart_mode(&self) -> RestartMode;
    fn abort_timeout(&self) -> Duration;
    fn spawn(&self) -> (AnyChild, DynAddress);
}

impl<T: Supervisable> DynChildSpec for T {
    fn id(&self) -> String {
        <Self as Supervisable>::id(self)
    }

    fn restart_mode(&self) -> RestartMode {
        <Self as Supervisable>::restart_mode(self)
    }

    fn abort_timeout(&self) -> Duration {
        <Self as Supervisable>::abort_timeout(self)
    }

    fn spawn(&self) -> (AnyChild, DynAddress) {
        let (address, child) =
            crate::spawn(|rx, signal_rx, address| self.clone().run(rx, signal_rx, address));

        (child.into_any(), address.into_dyn_subset())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestartMode {
    Always,
    OnError,
    Never,
}
