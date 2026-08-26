use super::*;

#[derive(Debug)]
pub(super) struct Supervisee {
    pub child: Option<Child<()>>,
    pub spec: ChildSpec,
    pub dynamic: bool,
}

impl Supervisee {
    pub(super) fn new_static(spec: ChildSpec) -> Self {
        Self {
            child: None,
            spec,
            dynamic: false,
        }
    }

    pub(super) fn new_dynamic(spec: ChildSpec) -> Self {
        Self {
            child: None,
            spec,
            dynamic: true,
        }
    }

    pub(super) fn is_dynamic(&self) -> bool {
        self.dynamic
    }
}

impl ActorOps for Supervisee {
    type Ctx = Set<()>;

    fn handle(&self) -> &Channel<Self::Ctx> {
        &self.spec.handle()
    }
}
