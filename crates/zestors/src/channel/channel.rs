use std::{any::TypeId, marker::PhantomData};

use super::*;
use crate::{FromPayload, Message, MessageExt, TryIntoPayload};
use type_sets::{Contains, Set, SubsetOf};

const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;
const BACKPRESSURE_LIMIT: usize = 100;

pub trait QueueType: 'static {
    type Set: 'static;
}

impl<I: Interface> QueueType for I {
    type Set = <I as Interface>::Set;
}

impl<S: 'static> QueueType for Set<S> {
    type Set = Set<S>;
}

#[repr(transparent)]
struct Channel<Q: QueueType = Set!()> {
    inner: Arc<ChannelInner<dyn IsDynQueue>>,
    _marker: PhantomData<fn() -> Q>,
}

struct ChannelInner<Q: ?Sized> {
    pid: Pid,
    signal_queue: ConcurrentQueue<SignalInterface>,
    signal_notifier: Notify,
    status: eyeball::SharedObservable<ActorStatus2>,
    msg_notifier: Notify,
    msg_backpressure_limit: usize,
    msg_queue: Q,
}

impl<S: QueueType> Channel<S> {
    pub fn new(pid: Pid, status: ActorStatus2) -> Self
    where
        S: Interface,
    {
        let inner: Arc<ChannelInner<dyn IsDynQueue>> = Arc::new(ChannelInner {
            pid,
            msg_notifier: Notify::new(),
            msg_backpressure_limit: BACKPRESSURE_LIMIT,
            signal_queue: ConcurrentQueue::bounded(SIGNAL_QUEUE_CAPACITY),
            signal_notifier: Notify::new(),
            status: eyeball::SharedObservable::new(status),
            msg_queue: ConcurrentQueue::<S>::new(MSG_QUEUE_CAPACITY),
        });

        Channel {
            inner,
            _marker: PhantomData,
        }
    }

    pub fn into_dyn<S2>(self) -> Channel<S2>
    where
        S2: QueueType + SubsetOf<S>,
    {
        self.into_dyn_unchecked()
    }

    pub fn into_dyn_unchecked<S2>(self) -> Channel<S2>
    where
        S2: QueueType,
    {
        Channel {
            inner: self.inner,
            _marker: PhantomData,
        }
    }

    pub fn as_dyn<S2>(&self) -> &Channel<S2>
    where
        S2: QueueType + SubsetOf<S>,
    {
        self.as_dyn_unchecked()
    }

    pub fn as_dyn_unchecked<S2>(&self) -> &Channel<S2>
    where
        S2: QueueType,
    {
        // Sound because #[repr(transparent)] guarantees Channel<S>
        // and Channel<S2> share the exact layout of Arc<...>.
        unsafe { &*(self as *const Channel<S> as *const Channel<S2>) }
    }

    pub fn downcast<I>(self) -> Result<Channel<I>, Self>
    where
        I: Interface,
    {
        if self.is_interface::<I>() {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }

    pub fn raw_queue(&self) -> Option<&ConcurrentQueue<S>> {
        self.msg_queue.as_any().downcast_ref::<ConcurrentQueue<S>>()
    }

    pub fn downcast_ref<I>(&self) -> Option<&Channel<I>>
    where
        I: Interface,
    {
        if self.is_interface::<I>() {
            // Sound because #[repr(transparent)] guarantees Channel<S>
            // and Channel<I> share the exact layout of Arc<...>.
            Some(unsafe { &*(self as *const Channel<S> as *const Channel<I>) })
        } else {
            None
        }
    }

    pub fn is_interface<I: Interface>(&self) -> bool {
        self.msg_queue.type_id() == TypeId::of::<ConcurrentQueue<I>>()
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

    pub fn pop_msg(&self) -> Option<<dyn IsDynQueue as Queue>::Item> {
        match self.msg_queue.pop_item() {
            Ok(msg) => Some(msg),
            Err(e) => match e {
                PopError::Empty => None,
                PopError::Closed => unreachable!("Queue should never be closed"),
            },
        }
    }

    pub async fn recv_msg(&self) -> Option<<dyn IsDynQueue as Queue>::Item> {
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

impl<M, T> Sends<M> for Channel<Set<T>>
where
    M: Message,
    T: 'static,
    Set<T>: Contains<M>,
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
        match self.msg_queue.try_push_msg(msg) {
            Ok(output) => {
                self.msg_notifier.notify_one();
                output
            }
            Err(NotAccepted(_)) => {
                panic!(
                    "Message type {} not accepted by channel {}",
                    std::any::type_name::<M>(),
                    std::any::type_name::<Self>(),
                );
            }
        }
    }
}

impl<M, I> Sends<M> for Channel<I>
where
    M: Message,
    I: Interface + TryIntoPayload<M> + FromPayload<M> + Send + 'static,
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
        // Raw-queue could just be a raw pointer cast, but that requires that a
        // Channel<I: Interface> always has a ConcurrentQueue<I> as its msg_queue,
        // This is currently not guaranteed by the type system.
        if let Some(queue) = self.raw_queue() {
            let (payload, output) = <M as MessageExt>::build_payload(msg);
            let interface = <I as FromPayload<M>>::from_payload(payload);

            if let Err(_e) = queue.push_item(interface) {
                panic!("Queue was full or empty {}", std::any::type_name::<Self>());
            }

            output
        } else {
            match self.msg_queue.try_push_msg(msg) {
                Ok(output) => {
                    self.msg_notifier.notify_one();
                    output
                }
                Err(NotAccepted(_)) => {
                    panic!(
                        "Message type {} not accepted by channel {}",
                        std::any::type_name::<M>(),
                        std::any::type_name::<Self>(),
                    );
                }
            }
        }
    }
}

impl<Q: QueueType> Deref for Channel<Q> {
    type Target = ChannelInner<dyn IsDynQueue>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Q: QueueType> Clone for Channel<Q> {
    fn clone(&self) -> Self {
        Channel {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}
