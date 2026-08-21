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
    RegisterChild(Payload<RegisterChild>),
    DeregisterChild(Payload<DeregisterChild>),
    Debug(Payload<GetDebugInfo>),
    Children(Payload<GetChildren>),
    Health(Payload<GetHealth>),
}
