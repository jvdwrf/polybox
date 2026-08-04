use std::pin::Pin;

use futures::Stream;

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

// impl<T> Stream for EventStream<T> {
//     type Item = Event<T>;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         let receiver = &mut self.receiver;
//         let signal_receiver = &mut self.signal_receiver;

//         let mut receiver_fut = Box::pin(receiver.recv());
//         let mut signal_fut = Box::pin(signal_receiver.recv());

//         match futures::future::select(receiver_fut, signal_fut).poll_unpin(cx) {
//             std::task::Poll::Ready(futures::future::Either::Left((msg, _))) => {
//                 std::task::Poll::Ready(msg.map(Event::Message))
//             }
//             std::task::Poll::Ready(futures::future::Either::Right((signal, _))) => {
//                 std::task::Poll::Ready(signal.map(Event::Signal))
//             }
//             std::task::Poll::Pending => std::task::Poll::Pending,
//         }
//     }
// }
