use std::{any::TypeId, marker::PhantomData};

use super::*;
use crate::{
    FromPayload, Message, MessageExt, Rx, TryIntoPayload, new_request,
    signals::{self, ActorStatus},
};
use type_sets::{Contains, Set, SubsetOf};

const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;
pub(super) const BACKPRESSURE_LIMIT: usize = 100;

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
pub(crate) struct Channel<Q: QueueType = Set!()> {
    pub(super) inner: Arc<ChannelInner<dyn IsDynQueue>>,
    _marker: PhantomData<fn() -> Q>,
}

pub(crate) struct ChannelInner<Q: ?Sized> {
    pid: Pid,
    signal_queue: ConcurrentQueue<SignalInterface>,
    signal_notifier: Notify,
    status: eyeball::SharedObservable<ActorStatus>,
    msg_notifier: Notify,
    msg_backpressure_limit: usize,
    msg_queue: Q,
}

impl<T: QueueType> Channel<T> {
    pub(super) fn clone(&self) -> Self {
        Channel {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }

    pub(super) fn new(pid: Pid, status: ActorStatus) -> Self
    where
        T: Interface,
    {
        let inner: Arc<ChannelInner<dyn IsDynQueue>> = Arc::new(ChannelInner {
            pid,
            msg_notifier: Notify::new(),
            msg_backpressure_limit: BACKPRESSURE_LIMIT,
            signal_queue: ConcurrentQueue::bounded(SIGNAL_QUEUE_CAPACITY),
            signal_notifier: Notify::new(),
            status: eyeball::SharedObservable::new(status),
            msg_queue: ConcurrentQueue::<T>::new(MSG_QUEUE_CAPACITY),
        });

        Channel {
            inner,
            _marker: PhantomData,
        }
    }

    pub(super) fn raw_queue(&self) -> Option<&ConcurrentQueue<T>>
    where
        T: Interface,
    {
        if self.is_interface::<T>() {
            Some(unsafe {
                &*(&self.msg_queue as *const dyn IsDynQueue as *const ConcurrentQueue<T>)
            })
        } else {
            None
        }
    }

    pub(super) fn set_status(&self, status: ActorStatus) {
        self.status.set(status);

        if !status.should_accept_messages() {
            self.msg_notifier.notify_waiters();
        }
    }

    pub(super) fn is_open(&self) -> bool {
        self.status.read().should_accept_messages()
    }

    pub(super) fn backpressure(&self) -> &BackPressure {
        BackPressure::default()
    }

    pub(super) async fn delay_for_backpressure(&self) {
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

    fn signal(&self, signal: SignalInterface) {
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
}

impl<M, T> Sends<M> for Channel<Set<T>>
where
    M: Message,
    T: 'static,
    Set<T>: Contains<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        match self.send_checked(msg).await {
            Ok(output) => Ok(output),
            Err(SendCheckedError::Closed(msg)) => Err(SendError(msg)),
            Err(SendCheckedError::NotAccepted(_)) => {
                panic!(
                    "Message type {} not accepted by channel {}",
                    std::any::type_name::<M>(),
                    std::any::type_name::<Self>(),
                );
            }
        }
    }

    fn try_send(&self, msg: M) -> Result<M::Output, TrySendError<M>> {
        match self.try_send_checked(msg) {
            Ok(output) => Ok(output),
            Err(TrySendCheckedError::Closed(msg)) => Err(TrySendError::Closed(msg)),
            Err(TrySendCheckedError::Full(msg)) => Err(TrySendError::Full(msg)),
            Err(TrySendCheckedError::NotAccepted(_)) => {
                panic!(
                    "Message type {} not accepted by channel {}",
                    std::any::type_name::<M>(),
                    std::any::type_name::<Self>(),
                );
            }
        }
    }

    fn send_now(&self, msg: M) -> Result<M::Output, SendError<M>> {
        match self.send_now_checked(msg) {
            Ok(output) => Ok(output),
            Err(SendCheckedError::Closed(msg)) => Err(SendError(msg)),
            Err(SendCheckedError::NotAccepted(_)) => {
                panic!(
                    "Message type {} not accepted by channel {}",
                    std::any::type_name::<M>(),
                    std::any::type_name::<Self>(),
                );
            }
        }
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
            match self.force_send_checked(msg) {
                Ok(output) => output,
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

impl<Q: QueueType> ActorRef for Channel<Q> {
    type Set = Q::Set;

    async fn send_checked<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>> {
        self.delay_for_backpressure().await;
        self.send_now_checked(msg)
    }

    fn try_send_checked<M: Message>(&self, msg: M) -> Result<M::Output, TrySendCheckedError<M>> {
        if self.reached_backpressure() {
            return Err(TrySendCheckedError::Full(msg));
        }

        self.send_now_checked(msg).map_err(Into::into)
    }

    fn send_now_checked<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>> {
        if !self.is_open() {
            return Err(SendCheckedError::Closed(msg));
        }

        self.force_send_checked(msg).map_err(Into::into)
    }

    fn force_send_checked<M: Message>(&self, msg: M) -> Result<M::Output, NotAccepted<M>> {
        match self.msg_queue.try_push_msg(msg) {
            Ok(output) => {
                self.msg_notifier.notify_one();
                Ok(output)
            }
            Err(e) => Err(e),
        }
    }

    fn pid(&self) -> &Pid {
        &self.pid
    }

    fn status(&self) -> ActorStatus {
        self.status.get()
    }

    fn members(&self) -> &'static [TypeId] {
        self.msg_queue.members()
    }

    fn reached_backpressure(&self) -> bool {
        self.backpressure()
            .delay(self.msg_queue.len(), self.msg_backpressure_limit)
            .is_some()
    }

    fn signal_shutdown(&self) {
        self.signal(SignalInterface::Shutdown(signals::Shutdown));
    }

    fn signal_suspend(&self) {
        self.signal(SignalInterface::Suspend(signals::Suspend));
    }

    fn signal_resume(&self) {
        self.signal(SignalInterface::Resume(signals::Resume));
    }

    fn get_status(&self) -> Rx<ActorStatus> {
        let (tx, rx) = new_request();
        self.signal(SignalInterface::GetStatus((signals::GetStatus, tx)));
        rx
    }

    fn get_debug_state(&self) -> Rx<signals::DebugState> {
        let (tx, rx) = new_request();
        self.signal(SignalInterface::GetState((signals::GetState, tx)));
        rx
    }

    fn ping(&self) -> Rx<()> {
        let (tx, rx) = new_request();
        self.signal(SignalInterface::Ping((signals::Ping, tx)));
        rx
    }

    fn get_children(&self) -> Rx<Vec<signals::ChildDescription>> {
        let (tx, rx) = new_request();
        self.signal(SignalInterface::GetChildren((signals::GetChildren, tx)));
        rx
    }

    fn is_interface<I: Interface>(&self) -> bool {
        self.msg_queue.type_id() == TypeId::of::<ConcurrentQueue<I>>()
    }
}

impl<Q: QueueType> IntoDyn for Channel<Q> {
    type Ref<T: QueueType> = Channel<T>;

    fn into_dyn_unchecked<S>(self) -> Channel<S>
    where
        S: QueueType,
    {
        Channel {
            inner: self.inner,
            _marker: PhantomData,
        }
    }
}

impl<Q: QueueType> AsDyn for Channel<Q> {
    fn as_dyn_unchecked<S>(&self) -> &Channel<S>
    where
        S: QueueType,
    {
        // Sound because #[repr(transparent)] guarantees Channel<S>
        // and Channel<S2> share the exact layout of Arc<...>.
        unsafe { &*(self as *const Channel<Q> as *const Channel<S>) }
    }
}

impl<I: Interface> Channel<I> {
    pub(super) async fn recv_msg(&self) -> Option<I> {
        let raw_queue = self
            .raw_queue()
            .expect("Channel is not of the expected interface type");

        let mut notify = pin!(self.msg_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Ok(msg) = raw_queue.pop() {
                return Some(msg);
            }

            notify.as_mut().await;
            notify.set(self.msg_notifier.notified());
        }
    }

    pub(super) fn pop_msg(&self) -> Option<I> {
        let raw_queue = self
            .raw_queue()
            .expect("Channel is not of the expected interface type");

        raw_queue.pop().ok()
    }

    pub(super) async fn recv_signal(&self) -> Option<SignalInterface> {
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

    pub(super) fn pop_signal(&self) -> Option<SignalInterface> {
        match self.signal_queue.pop() {
            Ok(signal) => Some(signal),
            Err(e) => match e {
                PopError::Empty => None,
                PopError::Closed => unreachable!("Queue should never be closed"),
            },
        }
    }
}

impl<Q: QueueType> Deref for Channel<Q> {
    type Target = ChannelInner<dyn IsDynQueue>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
