use super::*;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub(crate) struct Shutdown;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub(crate) struct Suspend;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub(crate) struct Resume;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ActorStatus)]
pub(crate) struct GetStatus;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub(crate) struct GetState;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<ChildDescription>")]
pub(crate) struct GetChildren;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ())]
pub(crate) struct Ping;

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub(crate) enum SignalInterface {
    Shutdown(Payload<Shutdown>),
    Suspend(Payload<Suspend>),
    Resume(Payload<Resume>),
    GetStatus(Payload<GetStatus>),
    GetState(Payload<GetState>),
    Ping(Payload<Ping>),
    GetChildren(Payload<GetChildren>),
}
