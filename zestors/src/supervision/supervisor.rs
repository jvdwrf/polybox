use tokio::sync::{mpsc, watch};

use super::*;

#[derive(Debug)]
pub struct Supervisor {
    supervisees: IndexMap<ChildId, Supervisee>,
    strategy: SupervisionStrategy,
    restart_intensity: RestartIntensity,
    restarts: VecDeque<Instant>,
}

#[derive(Message, Debug)]
#[msg(path = crate)]
pub struct RegisterChild(ChildSpec);

#[derive(Message, Debug)]
#[msg(path = crate, reply = "Option<Supervisee>")]
pub struct DeregisterChild(ChildId);

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum SupervisorInterface {
    RegisterChild(Payload<RegisterChild>),
    DeregisterChild(Payload<DeregisterChild>),
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        Self {
            supervisees: Default::default(),
            strategy: SupervisionStrategy::default(),
            restart_intensity: RestartIntensity::default(),
            restarts: VecDeque::new(),
        }
    }

    pub fn with_strategy(mut self, strategy: SupervisionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_intensity(mut self, restart_intensity: RestartIntensity) -> Self {
        self.restart_intensity = restart_intensity;
        self
    }

    pub fn add_dyn_child(&mut self, spec: ChildSpec) {
        let supervisee = Supervisee::new(spec);
        self.supervisees
            .insert(supervisee.spec.id.clone(), supervisee);
    }

    pub fn add_dyn_children(&mut self, specs: impl IntoIterator<Item = ChildSpec>) {
        for spec in specs {
            self.add_dyn_child(spec);
        }
    }

    pub fn add_child<T>(
        &mut self,
        blueprint: ChildSpec<T>,
    ) -> watch::Receiver<Option<Address<T::Inbox>>>
    where
        T: ActorBlueprint + Send + 'static,
    {
        blueprint.supervise(self)
    }

    pub fn add_children<T>(
        &mut self,
        blueprints: impl IntoIterator<Item = ChildSpec<T>>,
    ) -> Vec<watch::Receiver<Option<Address<T::Inbox>>>>
    where
        T: ActorBlueprint + Send + 'static,
    {
        blueprints
            .into_iter()
            .map(|blueprint| blueprint.supervise(self))
            .collect()
    }

    pub fn with_child<T>(mut self, spec: ChildSpec<T>) -> Self
    where
        ChildSpec<T>: Into<ChildSpec>,
    {
        self.add_dyn_child(spec.into());
        self
    }

    pub fn with_children<T>(mut self, specs: impl IntoIterator<Item = ChildSpec<T>>) -> Self
    where
        ChildSpec<T>: Into<ChildSpec>,
    {
        for spec in specs {
            self.add_dyn_child(spec.into());
        }
        self
    }

    pub fn spawn(self) -> Child<(), SupervisorInterface> {
        crate::spawn(async move |stream, address| {
            self.run(stream, address).await?;
            Ok(())
        })
    }

    async fn run(
        mut self,
        mut stream: EventStream<SupervisorInterface>,
        _address: Address<SupervisorInterface>,
    ) -> Result<(), SupervisorError> {
        let mut suspended = false;
        let start_time = Instant::now();

        loop {
            let msg = tokio::select! {
                biased;
                Some(msg) = stream.recv_enabled(!suspended) => {
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
                Event::Signal(signal) => match signal {
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
                Event::Message(message) => match message {
                    SupervisorInterface::RegisterChild(RegisterChild(spec)) => {
                        tracing::trace!("Registering child: {:?}", spec.id);
                        self.add_dyn_child(spec);
                    }

                    SupervisorInterface::DeregisterChild((DeregisterChild(id), tx)) => {
                        tracing::trace!("Deregistering child: {:?}", id);
                        let supervisee = self.supervisees.shift_remove(&id);
                        tx.send(supervisee).ok();
                    }
                },
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) {
        ShutdownSupervisees {
            supervisees: &mut self.supervisees.values_mut().collect::<Vec<_>>(),
        }
        .await;
    }

    async fn handle_child_termination(
        &mut self,
        termination: ChildTermination,
    ) -> Result<(), RestartLimitReached> {
        let ChildTermination { id, exit, .. } = &termination;
        let spec = &self
            .supervisees
            .get(id)
            .expect("child should be present")
            .spec;

        // Check if a restart is required
        {
            let mode = spec.restart_mode;
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

    async fn restart_children<'a>(supervisees: &mut [&mut Supervisee]) {
        // First, shutdown all affected children
        let _ = ShutdownSupervisees { supervisees }.await;

        // Now restart all affected children
        for supervisee in supervisees {
            let child = supervisee.spec.spawn();
            supervisee.child = Some(child);
        }
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
    pub exit: Result<(), JoinError>,
    pub reference: Child<()>,
}

#[derive(Debug)]
pub struct Supervisee {
    pub child: Option<Child<()>>,
    pub spec: ChildSpec,
}

impl Supervisee {
    fn new(spec: ChildSpec) -> Self {
        Self { child: None, spec }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SupervisionStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

impl Default for SupervisionStrategy {
    fn default() -> Self {
        Self::OneForOne
    }
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

struct ShutdownSupervisees<'a, 'b> {
    supervisees: &'a mut [&'b mut Supervisee],
}

impl<'a, 'b> Future for ShutdownSupervisees<'a, 'b> {
    type Output = Vec<Result<(), JoinError>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut results = Vec::new();

        for supervisee in self.supervisees.iter_mut() {
            if let Some(child) = &mut supervisee.child
                && let Poll::Ready(exit) = child.poll_unpin(cx)
            {
                let _exited_child = supervisee.child.take().expect("child should be present");

                results.push(exit);
            }
        }

        if results.len() == self.supervisees.len() {
            Poll::Ready(results)
        } else {
            Poll::Pending
        }
    }
}
