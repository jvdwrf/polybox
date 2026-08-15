use super::*;

pub trait Recv {
    type Interface: Interface;

    fn pop_msg(&self) -> Option<Self::Interface>;
    async fn recv_msg(&self) -> Option<Self::Interface>;
    fn pop_signal(&self) -> Option<SignalInterface>;
    async fn recv_signal(&self) -> Option<SignalInterface>;
}
