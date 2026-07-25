use crate::{signals::Signal, *};
use polybox::{Message, Payload};

pub trait HandleMessage<T: Message> {
    fn handle_message(&mut self, msg: Payload<T>) -> impl Future<Output = ()> + Send;
}

pub trait HandleSignal {
    fn handle_signal(&mut self, signal: Signal) -> impl Future<Output = ()> + Send;
}
