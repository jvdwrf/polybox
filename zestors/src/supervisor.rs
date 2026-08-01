use futures::{FutureExt, Stream, StreamExt as _, future::join_all};
use indexmap::IndexMap;
use tokio::{select, time::Instant};

use crate::_prelude::*;
use std::{
    any::Any,
    collections::VecDeque,
    fmt::{Debug, Display},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

#[derive(Debug)]
pub struct Supervisor {
    supervisees: IndexMap<ChildId, Supervisee>,
    strategy: SupervisionStrategy,
    restart_intensity: RestartIntensity,
    restarts: VecDeque<Instant>,
}

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum SupervisorInterface {}

impl Supervisor {
    pub fn new(strategy: SupervisionStrategy, restart_intensity: RestartIntensity) -> Self {
        Self {
            supervisees: Default::default(),
            strategy,
            restart_intensity,
            restarts: VecDeque::new(),
        }
    }

    pub fn add_child(&mut self, spec: ChildSpec) {
        let supervisee = Supervisee::new(spec);
        self.supervisees.insert(supervisee.spec.id(), supervisee);
    }

    pub fn with_child(mut self, spec: ChildSpec) -> Self {
        self.add_child(spec);
        self
    }

    pub fn spawn(self) -> (AnyChild, DynAddress) {
        let (address, child) = crate::spawn(async move |rx, signal_rx, address| {
            self.run(rx, signal_rx, address).await?;
            Ok(())
        });

        (child.into_any(), address.into_dyn_subset())
    }

    async fn run(
        mut self,
        mut rx: Receiver<SupervisorInterface>,
        mut signal_rx: SignalReceiver,
        _address: Address<SupervisorInterface>,
    ) -> Result<(), SupervisorError> {
        let mut suspended = false;
        let start_time = Instant::now();

        loop {
            let msg = tokio::select! {
                biased;
                Some(msg) = signal_rx.recv_with(&mut rx) => {
                    Some(msg)
                }
                Some(item) = self.next() => {
                    self.handle_child_termination(item).await?;
                    None
                }
                _ = futures::future::pending::<()>() => {
                    unreachable!()
                }
            };

            let Some(msg) = msg else {
                continue;
            };

            match msg {
                SignalOrMessage::Signal(signal) => match signal {
                    Signal::Shutdown(_) | Signal::Exit(_) => {
                        self.shutdown().await;
                        break;
                    }
                    Signal::Suspend(_) => {
                        suspended = true;
                    }
                    Signal::Resume(_) => {
                        suspended = false;
                    }
                    Signal::GetStatus((_, tx)) => {
                        let status = if suspended {
                            signals::Status::Suspended
                        } else {
                            signals::Status::Running
                        };
                        tx.send(status).ok();
                    }
                    Signal::GetState((_, tx)) => {
                        tx.send(State {
                            status: if suspended {
                                signals::Status::Suspended
                            } else {
                                signals::Status::Running
                            },
                            uptime: start_time.elapsed(),
                            description: "Supervisor".to_string(),
                        })
                        .ok();
                    }
                    Signal::Ping((_, tx)) => {
                        tx.send(()).ok();
                    }
                },
                SignalOrMessage::Message(message) => match message {},
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) {
        Self::shutdown_children(&mut self.supervisees.values_mut().collect::<Vec<_>>()).await;
    }

    async fn handle_child_termination(
        &mut self,
        termination: ChildTermination,
    ) -> Result<(), RestartLimitReached> {
        let ChildTermination { id, exit, .. } = &termination;
        let spec = self
            .supervisees
            .get(id)
            .expect("child should be present")
            .spec
            .clone();

        // Check if a restart is required
        {
            let mode = spec.restart_mode();
            let ((RestartMode::Always, _) | (RestartMode::OnError, Err(_))) = (mode, exit) else {
                match exit {
                    Ok(_value) => {
                        tracing::info!("Child {id} exited successfully. (mode: {mode:?})");
                    }
                    Err(e) => {
                        tracing::warn!("Child {id} exited with error: {e:?}. (mode: {mode:?})",);
                    }
                };

                return Ok(());
            };
        }

        // Check if restart limit has been reached
        {
            if !self.allow_restart() {
                return Err(RestartLimitReached {
                    id: termination.id,
                    error: termination.exit.err(),
                });
            }
        }

        Self::restart_children(&mut self.affected_supervisees_mut(id)).await;

        Ok(())
    }

    async fn restart_children(supervisees: &mut [&mut Supervisee]) {
        // First, shutdown all affected children
        let _ = Self::shutdown_children(supervisees).await;

        // Now restart all affected children
        for supervisee in supervisees {
            let (child, _address) = supervisee.spec.spawn();
            supervisee.child = Some(child);
        }
    }

    async fn shutdown_children(
        supervisees: &mut [&mut Supervisee],
    ) -> Vec<Result<Box<dyn Any + Send>, JoinError>> {
        let shutdown_futures = supervisees.iter_mut().filter_map(|supervisee| {
            let abort_timeout = supervisee.spec.abort_timeout();

            supervisee
                .child
                .take()
                .map(|c| c.shutdown_abort(abort_timeout))
        });

        join_all(shutdown_futures).await
    }

    fn allow_restart(&mut self) -> bool {
        self.restart_intensity.allow_restart(&mut self.restarts)
    }

    fn affected_supervisees_mut<'a>(&'a mut self, id: &'a ChildId) -> Vec<&'a mut Supervisee> {
        match self.strategy {
            SupervisionStrategy::OneForOne => vec![
                self.supervisees
                    .get_mut(id)
                    .expect("child should be present"),
            ],
            SupervisionStrategy::OneForAll => self.supervisees.values_mut().collect(),
            SupervisionStrategy::RestForOne => {
                let mut affected = Vec::new();
                let mut found = false;

                for (child_id, supervisee) in self.supervisees.iter_mut() {
                    if child_id == id {
                        found = true;
                    }

                    if found {
                        affected.push(supervisee);
                    }
                }

                affected
            }
        }
    }

    fn handle_signal(&mut self, signal: Signal) -> Result<(), SupervisorError> {
        match signal {
            Signal::Shutdown(_) | Signal::Exit(_) => {
                for child in self.supervisees.values_mut() {
                    if let Some(child) = &mut child.child {
                        child.abort();
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn handle_message(&mut self, message: SupervisorInterface) -> Result<(), SupervisorError> {
        match message {
            // Handle messages here
        }
    }
}

impl Stream for Supervisor {
    type Item = ChildTermination;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for (id, supervisee) in self.supervisees.iter_mut() {
            if let Some(child) = &mut supervisee.child
                && let Poll::Ready(exit) = child.poll_unpin(cx)
            {
                let reference = supervisee
                    .child
                    .take()
                    .expect("reference should be present");

                return Poll::Ready(Some(ChildTermination {
                    id: id.clone(),
                    exit,
                    reference,
                }));
            }
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub struct ChildTermination {
    pub id: ChildId,
    pub exit: Result<Box<dyn Any + Send>, JoinError>,
    pub reference: AnyChild,
}

#[derive(Debug)]
struct Supervisee {
    child: Option<AnyChild>,
    spec: ChildSpec,
    restarts: VecDeque<Instant>,
}

impl Supervisee {
    fn new(spec: ChildSpec) -> Self {
        Self {
            child: None,
            spec,
            restarts: VecDeque::new(),
        }
    }
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

    pub fn id(&self) -> ChildId {
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

    fn id(&self) -> ChildId;

    fn run(
        self,
        receiver: Receiver<Self::Interface>,
        signal_receiver: SignalReceiver,
        address: Address<Self::Interface>,
    ) -> impl Future<Output = Result<Self::Exit, anyhow::Error>> + Send + 'static;

    fn restart_mode(&self) -> RestartMode {
        RestartMode::OnError
    }

    fn abort_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }
}

trait DynChildSpec: Debug {
    fn id(&self) -> ChildId;
    fn restart_mode(&self) -> RestartMode;
    fn abort_timeout(&self) -> Duration;
    fn spawn(&self) -> (AnyChild, DynAddress);
}

impl<T: Supervisable> DynChildSpec for T {
    fn id(&self) -> ChildId {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RestartIntensity {
    max_restarts: usize,
    within: Duration,
}

impl RestartIntensity {
    fn allow_restart(&self, restarts: &mut VecDeque<Instant>) -> bool {
        let now = Instant::now();

        while let Some(front) = restarts.front() {
            if now.duration_since(*front) > self.within {
                restarts.pop_front();
            } else {
                break;
            }
        }

        if restarts.len() >= self.max_restarts {
            return false;
        }

        restarts.push_back(now);
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChildId(Arc<str>);

impl Deref for ChildId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ChildId {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }
}

impl<T: Into<Arc<str>>> From<T> for ChildId {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Display for ChildId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SupervisionStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

#[derive(Debug, thiserror::Error)]
#[error("Terminal child error: {id}, error: {error:?}")]
pub struct RestartLimitReached {
    pub id: ChildId,
    pub error: Option<JoinError>,
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("Restart limit reached for child {0}")]
    RestartLimit(#[from] RestartLimitReached),
}
