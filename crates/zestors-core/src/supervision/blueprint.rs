use super::*;
use std::future::Future;

pub trait Blueprint: Debug + Send + Sync + 'static {
    type Actor: Actor;

    fn build(&self) -> impl Future<Output = rootcause::Result<Self::Actor>> + Send;

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

    async fn build(&self) -> rootcause::Result<Self::Actor> {
        Ok(self.clone())
    }
}

pub trait BlueprintExt: Blueprint + Sized {
    fn into_spawn_fn(self) -> DynRepeatSpawner
    where
        Self: Send + Sync + 'static,
    {
        DynRepeatSpawner::new(self)
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
        async { Ok(self.build().await?.spawn(pid)) }
    }

    fn supervisee_cfg(&self) -> ChildConfig {
        ChildConfig::for_blueprint(self)
    }

    fn with_pid(self, pid: impl Into<Pid>) -> ChildSpec<Self> {
        ChildSpec::new(pid, self)
    }
}
impl<T: Blueprint> BlueprintExt for T {}

pub trait IntoBlueprint {
    type Blueprint: Blueprint;

    fn into_blueprint(self) -> Self::Blueprint;
}

impl<T: Blueprint> IntoBlueprint for T {
    type Blueprint = T;

    fn into_blueprint(self) -> Self::Blueprint {
        self
    }
}

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

    async fn build(&self) -> rootcause::Result<Self::Actor> {
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

pub fn new_blueprint<F, A>(f: F) -> FnBlueprint<F, A>
where
    F: Fn() -> A + Send + Sync + 'static,
    A: Actor,
{
    FnBlueprint::new(f)
}
