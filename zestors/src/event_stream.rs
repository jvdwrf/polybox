use crate::_prelude::*;

#[derive(Debug)]
pub struct EventStream<T> {
    receiver: Receiver<T>,
    signal_receiver: SignalReceiver,
    address: Address<T>,
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

    pub fn receiver(&self) -> &Receiver<T> {
        &self.receiver
    }

    pub fn signal_receiver(&self) -> &SignalReceiver {
        &self.signal_receiver
    }

    pub fn address(&self) -> &Address<T> {
        &self.address
    }
}
