use crate::Message;
use type_sets::Set;

use super::*;

const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;
const BACKPRESSURE_LIMIT: usize = 100;

pub trait QueueType {
    type Queue: Queue + ?Sized;
}

impl<I: Interface> QueueType for I {
    type Queue = ConcurrentQueue<I>;
}

impl<S> QueueType for Set<S> {
    type Queue = DynQueue<Set<S>>;
}

struct Channel<Q: QueueType = Set!()>(Arc<ChannelInner<Q::Queue>>);

struct ChannelInner<Q: ?Sized> {
    pid: Pid,
    signal_queue: ConcurrentQueue<SignalInterface>,
    signal_notifier: Notify,
    status: eyeball::SharedObservable<ActorStatus2>,
    msg_notifier: Notify,
    msg_backpressure_limit: usize,
    msg_queue: Q,
}

impl<Q: QueueType> Channel<Q> {
    pub fn new(pid: Pid, status: ActorStatus2) -> Self
    where
        Q::Queue: Sized,
    {
        let channel = ChannelInner {
            pid,
            msg_notifier: Notify::new(),
            msg_backpressure_limit: BACKPRESSURE_LIMIT,
            signal_queue: ConcurrentQueue::bounded(SIGNAL_QUEUE_CAPACITY),
            signal_notifier: Notify::new(),
            status: eyeball::SharedObservable::new(status),
            msg_queue: <Q::Queue as Queue>::new(MSG_QUEUE_CAPACITY),
        };

        Channel(Arc::new(channel))
    }

    pub fn set_status(&self, status: ActorStatus2) {
        self.status.set(status);

        if !status.should_accept_messages() {
            self.msg_notifier.notify_waiters();
        }
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

    pub fn pop_msg(&self) -> Option<<Q::Queue as Queue>::Item> {
        match self.msg_queue.pop_item() {
            Ok(msg) => Some(msg),
            Err(e) => match e {
                PopError::Empty => None,
                PopError::Closed => unreachable!("Queue should never be closed"),
            },
        }
    }

    pub async fn recv_msg(&self) -> Option<<Q::Queue as Queue>::Item> {
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

    pub fn pop_signal(&self) -> Option<SignalInterface> {
        match self.signal_queue.pop() {
            Ok(signal) => Some(signal),
            Err(e) => match e {
                PopError::Empty => None,
                PopError::Closed => unreachable!("Queue should never be closed"),
            },
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
}

impl<M, Q: QueueType> Sends<M> for Channel<Q>
where
    M: Message,
    Q::Queue: Queue + Pushes<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        self.delay_for_backpressure().await;
        self.send_now(msg)
    }

    fn try_send(&self, msg: M) -> Result<M::Output, TrySendError<M>> {
        if self.reached_backpressure() {
            return Err(TrySendError::Full(msg));
        }

        self.send_now(msg).map_err(Into::into)
    }

    fn send_now(&self, msg: M) -> Result<M::Output, SendError<M>> {
        if !self.is_open() {
            return Err(SendError(msg));
        }

        Ok(self.force_send(msg))
    }

    fn force_send(&self, msg: M) -> M::Output {
        match self.msg_queue.push_msg(msg) {
            Ok(output) => {
                self.msg_notifier.notify_one();
                output
            }
            Err(_msg) => {
                panic!(
                    "Message queue for {} contains more than {} messages. The message has been lost.",
                    std::any::type_name::<Self>(),
                    MSG_QUEUE_CAPACITY
                );
            }
        }
    }
}

impl<Q: QueueType> Deref for Channel<Q> {
    type Target = ChannelInner<Q::Queue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Q: QueueType> Clone for Channel<Q> {
    fn clone(&self) -> Self {
        Channel(self.0.clone())
    }
}
