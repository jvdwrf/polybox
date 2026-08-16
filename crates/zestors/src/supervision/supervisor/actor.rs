use super::*;

#[derive(Debug)]
pub struct Supervisor {
    supervisees: IndexMap<Pid, Supervisee>,
    strategy: SupervisionStrategy,
    restart_limiter: RestartLimiter,
    registry: &'static Registry,
}

impl Supervisor {
    pub(super) fn new(
        supervisees: IndexMap<Pid, ChildSpec>,
        strategy: SupervisionStrategy,
        restart_intensity: RestartIntensity,
    ) -> Self {
        Self {
            supervisees: supervisees
                .into_iter()
                .map(|(pid, spec)| (pid, Supervisee::new(spec)))
                .collect(),
            strategy,
            restart_limiter: RestartLimiter::new(restart_intensity),
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

    pub fn add_child<T>(&mut self, spec: ChildSpec<T>) -> Address<<T::Actor as Actor>::Interface>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        let address = spec.address().clone();

        self.add_dyn_child(spec.into_dyn());

        address
    }

    pub fn add_children<T>(
        &mut self,
        specs: impl IntoIterator<Item = ChildSpec<T>>,
    ) -> Vec<Address<<T::Actor as Actor>::Interface>>
    where
        T: Blueprint + Send + Sync + 'static,
    {
        specs
            .into_iter()
            .map(|blueprint| self.add_child(blueprint))
            .collect()
    }

    pub fn with_child<T: SpawnOn>(mut self, spec: ChildSpec<T>) -> Self
    where
        ChildSpec<T>: Into<ChildSpec>,
    {
        self.add_dyn_child(spec.into());
        self
    }

    pub fn with_children<T: SpawnOn>(
        mut self,
        specs: impl IntoIterator<Item = ChildSpec<T>>,
    ) -> Self
    where
        ChildSpec<T>: Into<ChildSpec>,
    {
        for spec in specs {
            self.add_dyn_child(spec.into());
        }
        self
    }

    fn spawn_supervisee(supervisee: &mut Supervisee, registry: &Registry) -> Result<(), Report> {
        // We only have to add the children to the registry the first time they are spawned,
        // since it will persist across restarts.
        let child = supervisee.spec.spawn().attach(format!(
            "Failed to spawn supervisee {}",
            supervisee.spec.pid()
        ))?;
        supervisee.child = Some(child);
        registry
            .register(supervisee.address().clone())
            .attach(format!(
                "Failed to register supervisee {}",
                supervisee.spec.pid()
            ))?;
        Ok(())
    }

    async fn shutdown(&mut self) {
        shutdown_supervisees(&mut self.supervisees.values_mut().collect::<Vec<_>>()).await;
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
            let mode = spec.cfg().restart_mode;
            let ((RestartMode::Always, _) | (RestartMode::OnError, Err(_))) = (mode, exit) else {
                match exit {
                    Ok(_value) => {
                        tracing::info!("Child {id} exited successfully. (mode: {mode:?})");
                    }
                    Err(e) => {
                        tracing::warn!(error = ?e, "Child {id} exited with error. (mode: {mode:?})");
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
        shutdown_supervisees(supervisees).await;

        // Now restart all affected children
        for supervisee in supervisees {
            let child = supervisee
                .spec
                .spawn()
                .expect("Children should all be shut-down now");
            supervisee.child = Some(child);
        }
    }

    async fn wait_for_children(&mut self) -> Result<(), Report> {
        let futures = self.supervisees.values().map(|supervisee| async move {
            tokio::select! {
                exit = supervisee.watch_initialization() => {
                    exit
                        .attach_with(|| {
                            format!("Child {} failed to start", supervisee.spec.pid())
                        })
                        .map_err(Into::into)
                }

                () = tokio::time::sleep(supervisee.spec.cfg().init_timeout) => {
                    Err(report!(
                        "Child {} failed to start within {:?}",
                        supervisee.spec.pid(),
                        supervisee.spec.cfg().init_timeout
                    ))
                }
            }
        });

        join_all(futures)
            .await
            .into_iter()
            .collect_reports()
            .context("One or more children failed to start within the specified timeout")
            .map_err(Into::into)
    }

    fn allow_restart(&mut self) -> bool {
        self.restart_limiter.allow_restart()
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

impl Actor for Supervisor {
    type Interface = SupervisorInterface;
    type Exit = ();

    async fn run(mut self, mut stream: EventStream<Self::Interface>) -> Result<Self::Exit, Report> {
        for supervisee in self.supervisees.values_mut() {
            if let Err(e) = Self::spawn_supervisee(supervisee, &self.registry) {
                tracing::error!(error = ?e, "Failed to spawn supervisee: {}", supervisee.spec.pid());

                return Err(report!(
                    "Supervisor failed to spawn supervisee: {}",
                    supervisee.spec.pid()
                ));
            }

            tracing::debug!("Spawned supervisee: {}", supervisee.spec.pid());
        }

        if let Err(e) = self.wait_for_children().await {
            tracing::error!(error = ?e,"Failed to initialize all children. Supervisor will now shutdown all children and exit.");

            self.shutdown().await;
            tracing::debug!("Finished shutting down children after failed initialization.");

            return Err(report!("Failed to initialize children."));
        }

        tracing::info!(
            "Supervisor started successfully with children: {}",
            self.supervisees
                .keys()
                .map(|pid| pid.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        loop {
            let msg = tokio::select! {
                biased;
                Some(msg) = stream.next() => {
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
                    SignalEvent::GetState(tx) => {
                        tx.send(DebugState {
                            status: stream.status(),
                            uptime: stream.uptime().unwrap_or_default(),
                            description: "Supervisor".to_string(),
                        })
                        .ok();
                    }
                    SignalEvent::GetChildren(tx) => {
                        let children = self
                            .supervisees
                            .iter()
                            .map(|(pid, supervisee)| ChildDescription {
                                pid: pid.clone(),
                                restart_mode: supervisee.spec.cfg().restart_mode,
                                abort_timeout: supervisee.spec.cfg().abort_timeout,
                            })
                            .collect::<Vec<_>>();
                        tx.send(children).ok();
                    }
                    SignalEvent::StatusUpdate(status) => match status {
                        StatusUpdateEvent::Shutdown => {
                            self.shutdown().await;
                            break;
                        }
                        StatusUpdateEvent::Resume => (),
                        StatusUpdateEvent::Suspend => (),
                    },
                },
                Event::Message(message) => match message {
                    SupervisorInterface::RegisterChild(RegisterChild(spec)) => {
                        tracing::trace!("Registering child: {:?}", spec.pid());
                        self.add_dyn_child(spec);
                    }

                    SupervisorInterface::DeregisterChild((DeregisterChild(id), tx)) => {
                        tracing::trace!("Deregistering child: {:?}", id);
                        let supervisee = self.supervisees.shift_remove(&id);
                        tx.send(supervisee.map(|s| s.spec)).ok();
                    }
                },
            }
        }

        Ok(())
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

async fn shutdown_supervisees<'a>(supervisees: &mut [&'a mut Supervisee]) {
    let futures = supervisees
        .iter_mut()
        .filter_map(|supervisee| supervisee.child.take().map(|s| (s, &supervisee.spec)))
        .map(|(child, spec)| async move {
            child
                .shutdown_abort(spec.cfg().abort_timeout)
                .await
                .attach(spec.pid().clone())
        });

    let res: Result<(), Report> = join_all(futures)
        .await
        .into_iter()
        .collect_reports()
        .context("One or more children failed to shut down within the specified timeout")
        .map_err(Into::into);

    if let Err(e) = res {
        tracing::error!(error = ?e, "Abnormal shutdown of children");
    }
}

#[derive(Debug)]
pub struct ChildTermination {
    pub id: Pid,
    pub exit: Result<(), JoinError>,
    pub reference: Child<()>,
}
