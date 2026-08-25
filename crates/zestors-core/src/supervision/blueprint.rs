use super::*;
use std::future::Future;

pub trait Blueprint: Debug + Send + Sync + 'static {
    type Actor: Actor;

    fn instantiate(&self) -> impl Future<Output = rootcause::Result<Self::Actor>> + Send;

    fn default_instantiation_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_abort_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_init_timeout(&self) -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_restart_mode(&self) -> RestartMode {
        RestartMode::default()
    }
}

impl<T: Actor + Clone + Debug + Send + Sync + 'static> Blueprint for T {
    type Actor = T;

    async fn instantiate(&self) -> rootcause::Result<Self::Actor> {
        Ok(self.clone())
    }
}

pub trait BlueprintExt: Blueprint + Sized {
    fn into_spawn_fn(self) -> DynSpawner
    where
        Self: Send + Sync + 'static,
    {
        DynSpawner::new(self)
    }

    fn spawn(
        &self,
        pid: Pid,
    ) -> impl Future<
        Output = rootcause::Result<
            Child<<Self::Actor as Actor>::Exit, <Self::Actor as Actor>::Interface>,
        >,
    > + Send
    where
        Self: Send + Sync + 'static,
    {
        async { Ok(self.instantiate().await?.spawn_with(pid)?) }
    }

    fn generate_config(&self) -> ChildConfig {
        ChildConfig::new_for_blueprint(self)
    }

    fn with_pid(self, pid: impl Into<Pid>) -> Result<ChildSpec<Self>, DuplicatePidError> {
        ChildSpec::create(pid, self)
    }

    fn with_rand_pid(self) -> ChildSpec<Self> {
        ChildSpec::create_rand_pid(self)
    }
}
impl<T: Blueprint> BlueprintExt for T {}

pub struct FnBlueprint<F, A>
where
    F: Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    f: F,
}

impl<F, A> FnBlueprint<F, A>
where
    F: Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F, A> Blueprint for FnBlueprint<F, A>
where
    F: Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    type Actor = A;

    async fn instantiate(&self) -> rootcause::Result<Self::Actor> {
        Ok((self.f)())
    }
}

impl<F, A> Debug for FnBlueprint<F, A>
where
    F: Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnBlueprint")
            .field("actor", &std::any::type_name::<A>())
            .finish()
    }
}

impl<F, A> Clone for FnBlueprint<F, A>
where
    F: Clone + Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    fn clone(&self) -> Self {
        Self { f: self.f.clone() }
    }
}

pub fn blueprint<F, A>(f: F) -> FnBlueprint<F, A>
where
    F: Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    FnBlueprint::new(f)
}
