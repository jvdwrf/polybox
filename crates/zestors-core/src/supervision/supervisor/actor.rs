use smol_str::format_smolstr;

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

    async fn shutdown(
        &mut self,
        pids: Option<&[Pid]>,
        stream: &mut Inbox<SupervisorInterface>,
    ) {
        let child_descriptions = self.get_child_descriptions();

        let mut supervisees = match pids {
            Some(pids) => self
                .supervisees
                .iter_mut()
                .filter(|(pid, _)| pids.contains(pid))
                .map(|(_, supervisee)| supervisee)
                .collect::<Vec<_>>(),
            None => self.supervisees.values_mut().collect::<Vec<_>>(),
        };

        tokio::select! {
            _ = shutdown_supervisees(&mut supervisees) => {
                tracing::info!("All supervisees have been shut down");
            }

            _ = async {
                while let Some(msg) = stream.next().await {
                    match msg {
                        Event::Signal(signal) => match signal {
                            Signal::Shutdown | Signal::Resume | Signal::Suspend => {
                                tracing::debug!("Ignoring signal {:?} while shutting down supervisees", signal);
                            }
                        },
                        Event::Message(message) => match message {
                            SupervisorInterface::Children(env) => {
                                env.handle.send(child_descriptions.clone()).ok();
                            },
                            SupervisorInterface::Health(env) => todo!("Implement health check during shutdown"),
                        }
                    }
                }
            } => {
                tracing::info!("Shutdown signal received while shutting down supervisees");
            }
        }
    }

    async fn handle_child_termination(
        &mut self,
        termination: ChildTermination,
        stream: &mut Inbox<SupervisorInterface>,
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

        println!("RESTART REQUIRED: Child {} exited with {:?}", id, exit,);
        // Restart the affected children
        self.restart_affected_children(id, stream).await;

        Ok(())
    }

    async fn restart_affected_children<'a>(
        &mut self,
        pid: &Pid,
        stream: &mut Inbox<SupervisorInterface>,
    ) {
        let affected_pids = self.affected_pids(pid);

        self.shutdown(Some(&affected_pids), stream).await;

        let supervisees = self
            .supervisees
            .iter_mut()
            .filter(|(id, _)| affected_pids.contains(id))
            .map(|(_, supervisee)| supervisee)
            .collect::<Vec<_>>();

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
                init_result = supervisee.watch_initialization() => {
                    let Err(exit) = init_result else {
                        return Ok(());
                    };

                    if supervisee.spec.cfg().restart_mode.should_restart(&exit) {
                        init_result
                            .attach_with(|| {
                                format!("Child {} failed to start", supervisee.spec.pid())
                            })
                            .map_err(Into::into)
                    } else {
                        tracing::warn!(
                            "Child {} exited with {:?} during initialization, but restart is not required (mode: {:?})",
                            supervisee.spec.pid(),
                            init_result,
                            supervisee.spec.cfg().restart_mode
                        );
                        Ok(())
                    }
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

    fn affected_pids(&self, id: &Pid) -> Vec<Pid> {
        match self.strategy {
            SupervisionStrategy::OneForOne => vec![id.clone()],
            SupervisionStrategy::OneForAll => self.supervisees.keys().cloned().collect(),
            SupervisionStrategy::RestForOne => {
                let mut affected = Vec::new();
                let mut found = false;

                for child_id in self.supervisees.keys() {
                    if child_id == id {
                        found = true;
                    }

                    if found {
                        affected.push(child_id.clone());
                    }
                }

                affected
            }
        }
    }

    fn get_child_descriptions(&self) -> Vec<ChildDescription> {
        self.supervisees
            .iter()
            .map(|(pid, supervisee)| ChildDescription {
                pid: pid.clone(),
                cfg: supervisee.spec.cfg().clone(),
            })
            .collect()
    }
}

impl Actor for Supervisor {
    type Interface = SupervisorInterface;
    type Exit = ();

    async fn run(mut self, mut stream: Inbox<Self::Interface>) -> Result<Self::Exit, Report> {
        let child_descriptions = self.get_child_descriptions();

        // Run the initialization and shutdown signal handling concurrently.
        tokio::select! {
            initialization_result = async {
                // Spawn all supervisees
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

                // Wait for all supervisees to initialize
                if let Err(e) = self.wait_for_children().await {
                    tracing::error!(error = ?e,"Failed to initialize all children. Supervisor will now shutdown all children and exit.");

                    return Err(report!("Failed to initialize children."));
                } else {
                    Ok(())
                }
            } => {
                if let Err(e) = initialization_result {
                    self.shutdown(None, &mut stream).await;
                    return Err(e);
                }
            }

            _shutdown_signal_received = async {
                while let Some(msg) = stream.next().await {
                    match msg {
                        Event::Signal(signal) => match signal {
                            Signal::Shutdown => {
                                tracing::info!("Shutdown signal received during initialization. Shutting down supervisees.");
                                break;
                            }
                            Signal::Resume | Signal::Suspend => {
                                tracing::debug!("Ignoring signal {:?} while initializing", signal);
                            }
                        },
                        Event::Message(message) => match message {
                            SupervisorInterface::Children(env) => {
                                env.handle.send(child_descriptions.clone()).ok();
                            },
                            SupervisorInterface::Health(env) => todo!("Implement health check during initialization"),
                        }
                    }
                }
            } => {
                self.shutdown(None, &mut stream).await;
                return Ok(());
            }
        };

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
                    self.handle_child_termination(item, &mut stream).await?;
                    continue;
                }

                _ = futures::future::pending::<()>() => {
                    unreachable!()
                }
            };

            match msg {
                Event::Signal(signal) => match signal {
                    Signal::Shutdown => {
                        self.shutdown(None, &mut stream).await;
                        break;
                    }
                    Signal::Resume => (),
                    Signal::Suspend => (),
                },
                Event::Message(message) => match message {
                    // SupervisorInterface::RegisterChild(RegisterChild(spec)) => {
                    //     tracing::trace!("Registering child: {:?}", spec.pid());
                    //     self.add_dyn_child(spec);
                    // }

                    // SupervisorInterface::DeregisterChild((DeregisterChild(id), tx)) => {
                    //     tracing::trace!("Deregistering child: {:?}", id);
                    //     let supervisee = self.supervisees.shift_remove(&id);
                    //     tx.send(supervisee.map(|s| s.spec)).ok();
                    // }
                    SupervisorInterface::Children(env) => {
                        let children = self.get_child_descriptions();
                        env.handle.send(children).ok();
                    }

                    SupervisorInterface::Health(env) => {
                        // TODO: Check for recent restarts and determine health status accordingly
                        env.handle.send(HealthStatus::Healthy.into_health()).ok();
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

#[derive(Debug)]
pub struct ChildTermination {
    pub id: Pid,
    pub exit: Result<(), JoinError>,
    pub reference: Child<()>,
}

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
