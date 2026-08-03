use crate::_prelude::*;

#[derive(Debug)]
pub struct EventStream<T> {
    pub receiver: Receiver<T>,
    pub signal_receiver: SignalReceiver,
}

impl<T> EventStream<T> {
    pub fn new(receiver: Receiver<T>, signal_receiver: SignalReceiver) -> Self {
        Self {
            receiver,
            signal_receiver,
        }
    }

    pub async fn recv(&mut self) -> Option<Event<T>> {
        self.signal_receiver.recv_with(&mut self.receiver).await
    }

    pub async fn recv_enabled(&mut self, enabled: bool) -> Option<Event<T>> {
        self.signal_receiver
            .recv_with_enabled(&mut self.receiver, enabled)
            .await
    }
}
