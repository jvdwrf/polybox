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

impl AsActorRef for Supervisee {
    type ChannelSpec = Set<()>;

    fn channel_data(&self) -> &Channel<Self::ChannelSpec> {
        &self.spec.channel_data()
    }

    fn get_address(&self) -> Address<Self::ChannelSpec> {
        self.spec.get_address()
    }
}
