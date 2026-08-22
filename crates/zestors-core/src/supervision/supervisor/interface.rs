use super::*;

#[derive(Message, Debug)]
#[msg(path = crate)]
pub struct RegisterChild(pub ChildSpec);

#[derive(Message, Debug)]
#[msg(path = crate, reply = "Option<ChildSpec>")]
pub struct DeregisterChild(pub Pid);

#[derive(Interface, Debug)]
#[interface(path = "crate")]
pub enum SupervisorInterface {
    // RegisterChild(Envelope<RegisterChild>),
    // DeregisterChild(Envelope<DeregisterChild>),
    // Debug(Envelope<GetDebugInfo>),
    Children(Envelope<GetChildren>),
    Health(Envelope<GetHealth>),
}
