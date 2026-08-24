use super::*;

#[derive(Debug)]
pub struct SupervisorBlueprint {
    supervisees: IndexMap<Pid, ChildSpec>,
    strategy: SupervisionStrategy,
    restart_intensity: RestartIntensity,
}

impl Default for SupervisorBlueprint {
    fn default() -> Self {
        Self::new()
    }
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

    pub fn with_child<T: Spawnable>(mut self, spec: ChildSpec<T>) -> Self {
        self.supervisees.insert(spec.pid().clone(), spec.into_dyn());
        self
    }

    pub fn with_children<T: Spawnable>(
        mut self,
        specs: impl IntoIterator<Item = ChildSpec<T>>,
    ) -> Self {
        for spec in specs {
            let spec = spec.into_dyn();
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
}

impl Blueprint for SupervisorBlueprint {
    type Actor = Supervisor;

    async fn instantiate(&self) -> rootcause::Result<Self::Actor> {
        Ok(Supervisor::new(
            self.supervisees.clone(),
            self.strategy,
            self.restart_intensity.clone(),
        ))
    }

    fn default_abort_timeout(&self) -> Duration {
        Duration::from_mins(5)
    }

    fn default_init_timeout(&self) -> Duration {
        Duration::from_mins(5)
    }
}
