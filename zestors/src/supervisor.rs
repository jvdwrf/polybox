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
        loop {
            tokio::select! {
                biased;

                Some(msg) = signal_rx.recv_with(&mut rx) => {
                    match msg {
                        SignalOrMessage::Signal(signal) => self.handle_signal(signal)?,
                        SignalOrMessage::Message(message) => self.handle_message(message)?,
                    }
                }

                Some(item) = self.next() => {
                    self.handle_child_termination(item).await?
                }

                _ = futures::future::pending::<()>() => {
                    unreachable!()
                }
            };
        }
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

        self.restart_children(id).await;

        Ok(())
    }

    async fn restart_children(&mut self, id: &ChildId) {
        let mut affected_children = self.affected_children(id);

        // First attempt to shutdown all affected children gracefully
        let successful_shutdown = {
            let abort_timeout = affected_children
                .iter()
                .map(|(_id, supervisee)| supervisee.spec.abort_timeout())
                .max()
                .expect("at least one affected child");

            for (_id, supervisee) in &affected_children {
                if let Some(child) = &supervisee.child {
                    child.shutdown().await.ok();
                }
            }

            let mut child_futures = join_all(
                affected_children
                    .iter_mut()
                    .filter_map(|c| c.1.child.as_mut()),
            );

            // Race between waiting for children to shutdown and the abort timeout
            select! {
                _ = tokio::time::sleep(abort_timeout) => {
                    tracing::warn!("Timeout reached while waiting for children to shutdown. Aborting remaining children.");
                    false
                }

                _ = &mut child_futures => {
                    tracing::info!("All affected children have shutdown.");
                    true
                }
            }
        };

        // If shutdown was not successful, abort all remaining children
        if !successful_shutdown {
            for (_id, supervisee) in &affected_children {
                if let Some(child) = &supervisee.child {
                    child.abort();
                }
            }

            join_all(
                affected_children
                    .iter_mut()
                    .filter_map(|c| c.1.child.as_mut()),
            )
            .await;
        }

        // Now restart all affected children
        for (_id, supervisee) in &mut affected_children {
            let (child, _address) = supervisee.spec.spawn();
            supervisee.child = Some(child);
        }
    }

    fn allow_restart(&mut self) -> bool {
        self.restart_intensity.allow_restart(&mut self.restarts)
    }

    fn affected_children<'a>(
        &'a mut self,
        id: &'a ChildId,
    ) -> Vec<(&'a ChildId, &'a mut Supervisee)> {
        match self.strategy {
            SupervisionStrategy::OneForOne => vec![(
                id,
                self.supervisees
                    .get_mut(id)
                    .expect("child should be present"),
            )],
            SupervisionStrategy::OneForAll => self.supervisees.iter_mut().collect(),
            SupervisionStrategy::RestForOne => {
                let mut affected = Vec::new();
                let mut found = false;

                for (child_id, supervisee) in self.supervisees.iter_mut() {
                    if child_id == id {
                        found = true;
                    }

                    if found {
                        affected.push((child_id, supervisee));
                    }
                }

                affected
            }
        }
    }

    fn handle_signal(&mut self, signal: Signal) -> Result<(), SupervisorError> {
        match signal {
            Signal::Shutdown(_) | Signal::Kill(_) => {
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
