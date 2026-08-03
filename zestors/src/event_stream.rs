use crate::_prelude::*;

#[derive(Debug)]
pub struct EventStream<T> {
    pub receiver: Receiver<T>,
    pub signal_receiver: SignalReceiver,
    pub address: Address<T>,
}

impl<T> EventStream<T> {
    pub fn new(
        receiver: Receiver<T>,
        signal_receiver: SignalReceiver,
        address: Address<T>,
    ) -> Self {
        Self {
            receiver,
            signal_receiver,
            address,
        }
    }

    pub async fn recv(&mut self) -> Option<SignalOrMessage<T>> {
        self.signal_receiver.recv_with(&mut self.receiver).await
    }

    pub async fn recv_enabled(&mut self, enabled: bool) -> Option<SignalOrMessage<T>> {
        self.signal_receiver
            .recv_with_enabled(&mut self.receiver, enabled)
            .await
    }
}
