use std::future::Future;

use super::*;

// pub trait Recv {
//     type Interface: Interface;

//     fn pop_msg(&self) -> Option<Self::Interface>;
//     fn recv_msg(&self) -> impl Future<Output = Option<Self::Interface>> + Send;
//     fn pop_signal(&self) -> Option<SignalInterface>;
//     fn recv_signal(&self) -> impl Future<Output = Option<SignalInterface>> + Send;
// }
