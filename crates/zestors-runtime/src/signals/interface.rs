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
#[msg(reply = ())]
pub(crate) struct Ping;

#[derive(Interface, Debug)]
#[interface(path = "crate")]
pub(crate) enum SignalInterface {
    Shutdown(Envelope<Shutdown>),
    Suspend(Envelope<Suspend>),
    Resume(Envelope<Resume>),
    Ping(Envelope<Ping>),
}
