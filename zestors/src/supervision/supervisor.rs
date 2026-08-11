use super::*;

#[derive(Debug)]
pub struct SupervisorBlueprint {
    supervisees: IndexMap<Pid, ChildSpec>,
    strategy: SupervisionStrategy,
    restart_intensity: RestartIntensity,
}

impl SupervisorBlueprint {
    pub fn new() -> Self {
        Self {
            supervisees: Default::default(),
            strategy: SupervisionStrategy::default(),
            restart_intensity: RestartIntensity::default(),
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

    pub fn with_child<T>(mut self, spec: ChildSpec<T>) -> Self
    where
        ChildSpec<T>: Into<ChildSpec>,
    {
        let spec = spec.into();
        self.supervisees.insert(spec.pid().clone(), spec);
        self
    }

    pub fn with_children<T>(mut self, specs: impl IntoIterator<Item = ChildSpec<T>>) -> Self
    where
        ChildSpec<T>: Into<ChildSpec>,
    {
        for spec in specs {
            let spec = spec.into();
            self.supervisees.insert(spec.pid().clone(), spec);
        }
        self
    }

    pub fn add_dyn_child(&mut self, spec: ChildSpec) {
        self.supervisees.insert(spec.pid().clone(), spec);
    }

    pub fn add_dyn_children(&mut self, specs: impl IntoIterator<Item = ChildSpec>) {
        for spec in specs {
            self.add_dyn_child(spec);
        }
    }

    pub fn add_child<T>(
        &mut self,
        spec: ChildSpec<T>,
    ) -> Address<<T::Runner as ActorRunner>::Interface>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        let address = spec.address();

        self.add_dyn_child(ChildSpec {
            restart_mode: spec.restart_mode,
            abort_timeout: spec.abort_timeout,
            spawner: DynSpawner::new(spec.spawner),
            data: spec.data,
        });

        address
    }

    pub fn add_children<T>(
        &mut self,
        specs: impl IntoIterator<Item = ChildSpec<T>>,
    ) -> Vec<Address<<T::Runner as ActorRunner>::Interface>>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        specs
            .into_iter()
            .map(|blueprint| self.add_child(blueprint))
            .collect()
    }
}

impl Blueprint for SupervisorBlueprint {
    type Runner = Supervisor;

    fn instantiate(&self) -> Self::Runner {
        Supervisor {
            supervisees: self
                .supervisees
                .iter()
                .map(|(id, spec)| (id.clone(), Supervisee::new(spec.clone())))
                .collect(),
            strategy: self.strategy,
            restart_intensity: self.restart_intensity.clone(),
            restarts: VecDeque::new(),
            registry: Registry::local(),
        }
    }
}

#[derive(Debug)]
pub struct Supervisor {
    supervisees: IndexMap<Pid, Supervisee>,
    strategy: SupervisionStrategy,
    restart_intensity: RestartIntensity,
    restarts: VecDeque<Instant>,
    registry: &'static Registry,
}

#[derive(Message, Debug)]
#[msg(path = crate)]
pub struct RegisterChild(ChildSpec);

#[derive(Message, Debug)]
#[msg(path = crate, reply = "Option<Supervisee>")]
pub struct DeregisterChild(Pid);

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
    fn new() -> Self {
        Self {
            supervisees: Default::default(),
            strategy: SupervisionStrategy::default(),
            restart_intensity: RestartIntensity::default(),
            restarts: VecDeque::new(),
            registry: Registry::local(),
        }
    }

    pub fn blueprint() -> SupervisorBlueprint {
        SupervisorBlueprint::new()
    }

    pub fn add_dyn_child(&mut self, spec: ChildSpec) {
        let supervisee = Supervisee::new(spec);
        self.supervisees
            .insert(supervisee.spec.pid().clone(), supervisee);
    }

    pub fn add_dyn_children(&mut self, specs: impl IntoIterator<Item = ChildSpec>) {
        for spec in specs {
            self.add_dyn_child(spec);
        }
    }

    pub fn add_child<T>(
        &mut self,
        spec: ChildSpec<T>,
    ) -> Address<<T::Runner as ActorRunner>::Interface>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        let address = spec.address();

        self.add_dyn_child(ChildSpec {
            restart_mode: spec.restart_mode,
            abort_timeout: spec.abort_timeout,
            spawner: DynSpawner::new(spec.spawner),
            data: spec.data,
        });

        address
    }

    pub fn add_children<T>(
        &mut self,
        specs: impl IntoIterator<Item = ChildSpec<T>>,
    ) -> Vec<Address<<T::Runner as ActorRunner>::Interface>>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        specs
            .into_iter()
            .map(|blueprint| self.add_child(blueprint))
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

    fn spawn_supervisee(
        supervisee: &mut Supervisee,
        registry: &Registry,
    ) -> Result<(), RegistryAddError> {
        // We only have to add the children to the registry the first time they are spawned,
        // since it will persist across restarts.
        let child = supervisee.spec.spawn();
        supervisee.child = Some(child);
        registry.register(supervisee.spec.data.clone())
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

        // No restart is required -> Deregister the child and return early
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

                let removed_child = self.registry.remove(id);

                if removed_child.is_none() {
                    tracing::warn!(
                        "Child {id} was not found in the registry during deregistration."
                    );
                }

                return Ok(());
            };
        }

        // Restart limit reached -> Return an error
        {
            if !self.allow_restart() {
                return Err(RestartLimitReached {
                    id: termination.id,
                    error: termination.exit.err(),
                });
            }
        }

        // Restart the affected children
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

    fn affected_supervisees_mut<'a>(&'a mut self, id: &'a Pid) -> Vec<&'a mut Supervisee> {
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

impl Unpin for Supervisor {}

impl ActorRunner for Supervisor {
    type Interface = SupervisorInterface;
    type Exit = ();

    async fn run(
        mut self,
        mut stream: EventStream<Self::Interface>,
        _address: Address<Self::Interface>,
    ) -> Result<Self::Exit, anyhow::Error> {
        let mut suspended = false;
        let start_time = Instant::now();

        for supervisee in self.supervisees.values_mut() {
            Self::spawn_supervisee(supervisee, &self.registry)?;
        }

        loop {
            let msg = tokio::select! {
                biased;
                Some(msg) = stream.recv_enabled(!suspended) => {
                    msg
                }
                Some(item) = self.next() => {
                    self.handle_child_termination(item).await?;
                    continue;
                }

                _ = futures::future::pending::<()>() => {
                    unreachable!()
                }
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
                        tracing::trace!("Registering child: {:?}", spec.pid());
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
}

#[derive(Debug)]
pub struct ChildTermination {
    pub id: Pid,
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
    pub id: Pid,
    pub error: Option<JoinError>,
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("Restart limit reached for child {0}")]
    RestartLimit(#[from] RestartLimitReached),

    #[error("Another process is already registered with the same pid")]
    RegistryAddError(
        #[source]
        #[from]
        RegistryAddError,
    ),
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
