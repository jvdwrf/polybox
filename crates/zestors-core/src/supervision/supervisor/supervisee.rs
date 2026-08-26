use super::*;

#[derive(Debug)]
pub(super) struct Supervisee {
    pub child: Option<Child<()>>,
    pub spec: ChildSpec,
}

impl Supervisee {
    pub(super) fn new(spec: ChildSpec) -> Self {
        Self { child: None, spec }
    }
}

impl AsActorHandle for Supervisee {
    type Ctx = Set<()>;

    fn handle(&self) -> &ActorHandle<Self::Ctx> {
        &self.spec.handle()
    }
}
