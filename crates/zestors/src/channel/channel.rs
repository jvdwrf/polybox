use crate::{FromPayload, Message, MessageExt, TryIntoPayload};

use super::*;

const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;
const BACKPRESSURE_LIMIT: usize = 100;

struct Channel<T>(Arc<ChannelInner<T>>);

struct ChannelInner<T> {
    pid: Pid,
    msg_queue: ConcurrentQueue<T>,
    msg_notifier: Notify,
    msg_backpressure_limit: usize,
    signal_queue: ConcurrentQueue<SignalInterface>,
    signal_notifier: Notify,
    status: eyeball::SharedObservable<ActorStatus2>,
}

impl<T> Channel<T> {
    pub fn set_status(&self, status: ActorStatus2) {
        self.status.set(status);

        if !status.should_accept_messages() {
            self.msg_notifier.notify_waiters();
        }
    }

    pub fn new(pid: Pid, status: ActorStatus2) -> Self {
        let channel = ChannelInner {
            pid,
            msg_queue: ConcurrentQueue::bounded(MSG_QUEUE_CAPACITY),
            msg_notifier: Notify::new(),
            msg_backpressure_limit: BACKPRESSURE_LIMIT,
            signal_queue: ConcurrentQueue::bounded(SIGNAL_QUEUE_CAPACITY),
            signal_notifier: Notify::new(),
            status: eyeball::SharedObservable::new(status),
        };

        Channel(Arc::new(channel))
    }

    pub fn is_open(&self) -> bool {
        self.status.read().should_accept_messages()
    }

    pub fn pid(&self) -> &Pid {
        &self.pid
    }

    fn backpressure(&self) -> &BackPressure {
        BackPressure::default()
    }

    fn reached_backpressure(&self) -> bool {
        self.backpressure()
            .delay(self.msg_queue.len(), self.msg_backpressure_limit)
            .is_some()
    }

    async fn delay_for_backpressure(&self) {
        let len = self.msg_queue.len();
        let limit = self.msg_backpressure_limit;

        if let Some(delay) = self
            .backpressure()
            .delay(self.msg_queue.len(), self.msg_backpressure_limit)
        {
            tracing::warn!(
                "Backpressure applied: queue occupancy = {:.2}%, delay = {:?}",
                len as f32 / limit as f32 * 100.0,
                delay
            );
            tokio::time::sleep(delay).await;
        }
    }

    pub fn signal(&self, signal: SignalInterface) {
        match self.signal_queue.push(signal) {
            Ok(_) => {
                self.signal_notifier.notify_one();
            }
            Err(e) => {
                tracing::error!(
                    "Signal queue for {} contains more than {} signals. The signal has been lost. Error: {:?}",
                    std::any::type_name::<Self>(),
                    SIGNAL_QUEUE_CAPACITY,
                    e
                );
            }
        }
    }

    pub fn pop_msg(&self) -> Option<T> {
        match self.msg_queue.pop() {
            Ok(msg) => Some(msg),
            Err(e) => match e {
                PopError::Empty => None,
                PopError::Closed => unreachable!("Queue should never be closed"),
            },
        }
    }

    pub fn pop_signal(&self) -> Option<SignalInterface> {
        match self.signal_queue.pop() {
            Ok(signal) => Some(signal),
            Err(e) => match e {
                PopError::Empty => None,
                PopError::Closed => unreachable!("Queue should never be closed"),
            },
        }
    }

    pub async fn recv_msg(&self) -> Option<T> {
        let mut notify = pin!(self.msg_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Some(msg) = self.pop_msg() {
                return Some(msg);
            }

            notify.as_mut().await;

            notify.set(self.msg_notifier.notified());
        }
    }

    pub async fn recv_signal(&self) -> Option<SignalInterface> {
        let mut notify = pin!(self.signal_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Some(signal) = self.pop_signal() {
                return Some(signal);
            }

            notify.as_mut().await;

            notify.set(self.signal_notifier.notified());
        }
    }

    async fn send_interface(&self, msg: T) -> Result<(), Closed<T>> {
        self.delay_for_backpressure().await;
        self.send_interface_now(msg)?;
        Ok(())
    }

    fn try_send_interface(&self, msg: T) -> Result<(), ClosedOrFull<T>> {
        if self.reached_backpressure() {
            return Err(ClosedOrFull::Full(msg));
        }

        self.send_interface_now(msg)?;

        Ok(())
    }

    fn send_interface_now(&self, msg: T) -> Result<(), Closed<T>> {
        if !self.is_open() {
            return Err(Closed(msg));
        }

        self.force_send_interface(msg);

        Ok(())
    }

    fn force_send_interface(&self, msg: T) {
        match self.msg_queue.push(msg) {
            Ok(_) => {
                self.msg_notifier.notify_one();
            }
            Err(_msg) => {
                tracing::error!(
                    "Message queue for {} contains more than {} messages. The message has been lost.",
                    std::any::type_name::<Self>(),
                    MSG_QUEUE_CAPACITY,
                );
            }
        }
    }
}

impl<M, I> Sends<M> for Channel<I>
where
    M: Message,
    I: TryIntoPayload<M> + FromPayload<M> + Send,
{
    async fn send(&self, msg: M) -> Result<M::Output, Closed<M>> {
        let (payload, output) = <M as MessageExt>::build_payload(msg);
        let interface = I::from_payload(payload);

        match self.send_interface(interface).await {
            Ok(()) => Ok(output),
            Err(e) => {
                let msg = <M as MessageExt>::destroy_payload(
                    e.0.try_into_payload()
                        .map_err(|_| ())
                        .expect("Failed to convert payload back"),
                );

                Err(Closed(msg))
            }
        }
    }

    fn try_send(&self, msg: M) -> Result<M::Output, ClosedOrFull<M>> {
        let (payload, output) = <M as MessageExt>::build_payload(msg);
        let interface = I::from_payload(payload);

        match self.try_send_interface(interface) {
            Ok(()) => Ok(output),
            Err(e) => match e {
                ClosedOrFull::Closed(interface) => {
                    let msg = <M as MessageExt>::destroy_payload(
                        interface
                            .try_into_payload()
                            .map_err(|_| ())
                            .expect("Failed to convert payload back"),
                    );

                    Err(ClosedOrFull::Closed(msg))
                }
                ClosedOrFull::Full(interface) => {
                    let msg = <M as MessageExt>::destroy_payload(
                        interface
                            .try_into_payload()
                            .map_err(|_| ())
                            .expect("Failed to convert payload back"),
                    );

                    Err(ClosedOrFull::Full(msg))
                }
            },
        }
    }

    fn send_now(&self, msg: M) -> Result<M::Output, Closed<M>> {
        let (payload, output) = <M as MessageExt>::build_payload(msg);
        let interface = I::from_payload(payload);

        match self.send_interface_now(interface) {
            Ok(()) => Ok(output),
            Err(e) => {
                let msg = <M as MessageExt>::destroy_payload(
                    e.0.try_into_payload()
                        .map_err(|_| ())
                        .expect("Failed to convert payload back"),
                );

                Err(Closed(msg))
            }
        }
    }

    fn force_send(&self, msg: M) -> M::Output {
        let (payload, output) = <M as MessageExt>::build_payload(msg);
        let interface = I::from_payload(payload);
        self.force_send_interface(interface);
        output
    }
}

impl<T> Deref for Channel<T> {
    type Target = ChannelInner<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel(self.0.clone())
    }
}
