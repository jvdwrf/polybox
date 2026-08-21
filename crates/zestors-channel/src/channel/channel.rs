use super::*;
use crate::signals::{self, Event};
use eyeball::SharedObservable;
use jiff::{SignedDuration, Timestamp, Zoned, tz::TimeZone};
use std::{any::TypeId, fmt::Debug, hash::Hash, marker::PhantomData, sync::RwLock};
use tokio::{select, time::Instant};
use type_sets::{Contains, Set, TypeSet};

const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;
pub(super) const BACKPRESSURE_LIMIT: usize = 100;
const KEEP_N_SPAWNS: usize = 5;
const KEEP_N_EXITS: usize = 5;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelSnapshot {
    pub pid: Pid,
    pub status: ActorStatus,
    pub signal_len: usize,
    pub msg_len: usize,
    pub spawns: Vec<Zoned>,
    pub exits: Vec<(Zoned, Exit)>,
    pub created_at: Zoned,
}

pub trait ChannelKind: 'static {
    type Set: TypeSet + 'static;
}

impl<I: Interface> ChannelKind for I {
    type Set = <I as Interface>::Set;
}

impl<S: TypeSet + 'static> ChannelKind for Set<S> {
    type Set = S;
}

#[repr(transparent)]
pub struct Channel<C: ChannelKind = Set!()> {
    pub(super) inner: Arc<ChannelInner<dyn IsDynQueue>>,
    _marker: PhantomData<fn() -> C>,
}

pub(crate) struct ChannelInner<Q: ?Sized> {
    pid: Pid,
    signal_queue: ConcurrentQueue<SignalInterface>,
    signal_notifier: Notify,
    status_observer: SharedObservable<ActorStatus>,
    msg_notifier: Notify,
    msg_backpressure_limit: usize,
    created_at: Instant,
    spawns: RwLock<Vec<Instant>>,
    exits: RwLock<Vec<(Instant, Result<(), ExitError>)>>,
    msg_queue: Q,
}

impl<T: ChannelKind> Channel<T> {
    pub fn new(pid: Pid) -> Self
    where
        T: Interface,
    {
        let inner: Arc<ChannelInner<dyn IsDynQueue>> = Arc::new(ChannelInner {
            pid,
            msg_notifier: Notify::new(),
            msg_backpressure_limit: BACKPRESSURE_LIMIT,
            signal_queue: ConcurrentQueue::bounded(SIGNAL_QUEUE_CAPACITY),
            signal_notifier: Notify::new(),
            status_observer: SharedObservable::new(ActorStatus::Dead(Exit::Normal)),
            msg_queue: ConcurrentQueue::<T>::new(MSG_QUEUE_CAPACITY),
            created_at: Instant::now(),
            spawns: Default::default(),
            exits: Default::default(),
        });

        Channel {
            inner,
            _marker: PhantomData,
        }
    }

    pub(super) fn set_status(&self, status: ActorStatus) {
        self.inner.status_observer.set(status);
    }

    pub(super) fn register_spawn(&self) {
        tracing::debug!("Process initializing");

        let mut spawned_at = self.inner.spawns.write().unwrap();

        if spawned_at.len() > KEEP_N_SPAWNS {
            _ = spawned_at.remove(0);
        }

        spawned_at.push(Instant::now());
        self.set_status(ActorStatus::Initializing);
    }

    pub(super) fn register_exit(&self, reason: Result<(), ExitError>) {
        match &reason {
            Ok(()) => tracing::debug!("Process exited normally"),
            Err(err) => tracing::warn!("Process exited with error: {:?}", err),
        }

        let mut exited_at = self.inner.exits.write().unwrap();

        if exited_at.len() > KEEP_N_EXITS {
            _ = exited_at.remove(0);
        }

        exited_at.push((Instant::now(), reason));
        self.set_status(ActorStatus::Dead(Exit::from_result(reason)));
    }

    pub(super) fn register_initialized(&self) {
        tracing::debug!("Process started");
        self.set_status(ActorStatus::Running);
    }

    pub(super) fn register_suspend(&self) {
        tracing::debug!("Process suspended");
        self.set_status(ActorStatus::Suspended);
    }

    pub(super) fn register_resume(&self) {
        tracing::debug!("Process resumed");
        self.set_status(ActorStatus::Running);
    }

    pub(super) fn register_shutdown(&self) {
        tracing::debug!("Process shutting down");
        self.set_status(ActorStatus::Exiting);
    }

    pub(super) fn raw_queue(&self) -> Option<&ConcurrentQueue<T>>
    where
        T: Interface,
    {
        if self.is_interface::<T>() {
            // SAFETY: We just checked that the channel's message queue is of type `ConcurrentQueue<T>`.
            Some(unsafe { self.raw_queue_unchecked() })
        } else {
            None
        }
    }

    /// # Safety
    /// This function is unsafe because it assumes that the channel's message queue is of type
    /// `ConcurrentQueue<T>`. If this assumption is incorrect, it may lead to undefined behavior.
    unsafe fn raw_queue_unchecked(&self) -> &ConcurrentQueue<T>
    where
        T: Interface,
    {
        unsafe { &*(&self.inner.msg_queue as *const dyn IsDynQueue as *const ConcurrentQueue<T>) }
    }

    // fn update_status(&self, f: impl FnOnce(ActorStatus) -> Option<ActorStatus>) {
    //     self.inner.status_observer.update_if(|status| {
    //         let new_status = f(status.clone());

    //         if let Some(new_status) = new_status {
    //             *status = new_status;
    //             true
    //         } else {
    //             false
    //         }
    //     });

    //     if !self.status().accepts_messages() {
    //         self.inner.msg_notifier.notify_waiters();
    //     }
    // }

    pub(super) fn backpressure(&self) -> &BackPressure {
        BackPressure::default()
    }

    pub(super) async fn delay_for_backpressure(&self) {
        let len = self.inner.msg_queue.len();
        let limit = self.inner.msg_backpressure_limit;

        if let Some(delay) = self.backpressure().delay(
            self.inner.msg_queue.len(),
            self.inner.msg_backpressure_limit,
        ) {
            tracing::warn!(
                "Backpressure applied: queue occupancy = {:.2}%, delay = {:?}",
                len as f32 / limit as f32 * 100.0,
                delay
            );
            tokio::time::sleep(delay).await;
        }
    }

    fn signal(&self, signal: SignalInterface) {
        match self.inner.signal_queue.push(signal) {
            Ok(_) => {
                self.inner.signal_notifier.notify_one();
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

impl<I: Interface> Channel<I> {
    pub(crate) async fn recv_msg(&self) -> Option<I> {
        let raw_queue = self
            .raw_queue()
            .expect("Channel is not of the expected interface type");

        let mut notify = pin!(self.inner.msg_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Ok(msg) = raw_queue.pop() {
                return Some(msg);
            }

            notify.as_mut().await;
            notify.set(self.inner.msg_notifier.notified());
        }
    }

    pub(crate) fn pop_msg(&self) -> Option<I> {
        let raw_queue = self
            .raw_queue()
            .expect("Channel is not of the expected interface type");

        raw_queue.pop().ok()
    }

    pub(crate) async fn recv_signal(&self) -> Option<SignalEvent> {
        let mut notify = pin!(self.inner.signal_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Some(signal) = self.pop_signal() {
                return Some(signal);
            }

            notify.as_mut().await;

            notify.set(self.inner.signal_notifier.notified());
        }
    }

    pub(crate) fn pop_signal(&self) -> Option<SignalEvent> {
        loop {
            match self.inner.signal_queue.pop() {
                Ok(signal) => match self.handle_signal(signal) {
                    Some(event) => return Some(event),
                    None => continue,
                },
                Err(e) => {
                    return match e {
                        PopError::Empty => None,
                        PopError::Closed => unreachable!("Queue should never be closed"),
                    };
                }
            }
        }
    }

    fn handle_signal(&self, signal: SignalInterface) -> Option<SignalEvent> {
        match signal {
            SignalInterface::Shutdown(_) => {
                self.register_shutdown();
                Some(SignalEvent::Shutdown)
            }
            SignalInterface::Suspend(_) => {
                if self.status() == ActorStatus::Exiting {
                    tracing::warn!("Actor is exiting, cannot suspend");
                    None
                } else {
                    self.register_suspend();
                    Some(SignalEvent::Suspend)
                }
            }
            SignalInterface::Resume(_) => {
                if self.status() != ActorStatus::Suspended {
                    tracing::warn!("Actor is not suspended, cannot resume");
                    None
                } else {
                    self.register_resume();
                    Some(SignalEvent::Resume)
                }
            }
            SignalInterface::Ping((_, tx)) => {
                let _ = tx.send(());
                None
            }
        }
    }

    pub(crate) async fn next(&self) -> Option<Event<I>> {
        if self.status() == ActorStatus::Suspended {
            return self.recv_signal().await.map(Event::Signal);
        }

        select! {
            biased;

            Some(msg) = self.recv_msg() => Some(Event::Message(msg)),
            Some(signal) = self.recv_signal() => Some(Event::Signal(signal)),
            else => None,
        }
    }
}

impl<M, T> Sends<M> for Channel<Set<T>>
where
    M: Message,
    T: TypeSet + Contains<M> + 'static,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        match self.send_dyn(msg).await {
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
        match self.try_send_dyn(msg) {
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
        match self.send_now_dyn(msg) {
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
        match self.inner.msg_queue.try_push_msg(msg) {
            Ok(output) => {
                self.inner.msg_notifier.notify_one();
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
        if !self.status().accepts_messages() {
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
            match self.force_send_dyn(msg) {
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

impl<C: ChannelKind> ActorRef for Channel<C> {
    type ChannelKind = C;
    type Set = C::Set;

    async fn send_dyn<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>> {
        self.delay_for_backpressure().await;
        self.send_now_dyn(msg)
    }

    fn try_send_dyn<M: Message>(&self, msg: M) -> Result<M::Output, TrySendCheckedError<M>> {
        if self.reached_backpressure() {
            return Err(TrySendCheckedError::Full(msg));
        }

        self.send_now_dyn(msg).map_err(Into::into)
    }

    fn send_now_dyn<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>> {
        if !self.status().accepts_messages() {
            return Err(SendCheckedError::Closed(msg));
        }

        self.force_send_dyn(msg).map_err(Into::into)
    }

    fn force_send_dyn<M: Message>(&self, msg: M) -> Result<M::Output, NotAccepted<M>> {
        match self.inner.msg_queue.try_push_msg(msg) {
            Ok(output) => {
                self.inner.msg_notifier.notify_one();
                Ok(output)
            }
            Err(e) => Err(e),
        }
    }

    async fn request_dyn<M: Message>(&self, msg: M) -> Result<M::Reply, RequestCheckedError<M>> {
        Ok(self.send_dyn(msg).await?.receive().await?)
    }

    fn pid(&self) -> &Pid {
        &self.inner.pid
    }

    fn status(&self) -> ActorStatus {
        self.inner.status_observer.get()
    }

    fn members(&self) -> &'static [TypeId] {
        self.inner.msg_queue.members()
    }

    fn reached_backpressure(&self) -> bool {
        self.backpressure()
            .delay(
                self.inner.msg_queue.len(),
                self.inner.msg_backpressure_limit,
            )
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

    fn ping(&self) -> Rx<()> {
        let (tx, rx) = new_request();
        self.signal(SignalInterface::Ping((signals::Ping, tx)));
        rx
    }

    fn is_interface<I: Interface>(&self) -> bool {
        self.inner.msg_queue.type_id() == TypeId::of::<ConcurrentQueue<I>>()
    }

    fn address(&self) -> &Address<Self::ChannelKind> {
        Address::from_ref(self)
    }

    fn len(&self) -> usize {
        self.inner.msg_queue.len()
    }

    async fn watch_initialization(&self) -> Result<(), Exit> {
        loop {
            let mut subscriber = self.inner.status_observer.subscribe();
            let status = self.status();

            match status {
                ActorStatus::Running => return Ok(()),
                ActorStatus::Dead(exit) => {
                    return Err(exit);
                }
                _ => {}
            }

            let status = subscriber.next().await;

            match status {
                Some(ActorStatus::Running) => return Ok(()),
                Some(ActorStatus::Dead(exit)) => {
                    return Err(exit);
                }
                _ => {}
            }
        }
    }

    async fn watch_exit(&self) -> Result<(), ExitError> {
        loop {
            let mut subscriber = self.inner.status_observer.subscribe();
            let status = self.status();

            if let ActorStatus::Dead(exit) = status {
                return exit.into_result();
            }

            let status = subscriber.next().await;

            if let Some(ActorStatus::Dead(exit)) = status {
                return exit.into_result();
            }
        }
    }

    fn created_at(&self) -> Instant {
        self.inner.created_at
    }

    fn spawned_at(&self) -> Vec<Instant> {
        let spawned_at = self.inner.spawns.read().unwrap();
        spawned_at.clone()
    }

    fn last_spawned_at(&self) -> Option<Instant> {
        let spawned_at = self.inner.spawns.read().unwrap();
        spawned_at.last().cloned()
    }

    fn snapshot(&self) -> ChannelSnapshot {
        let clock = Clock::now();
        let channel = &self.inner;

        ChannelSnapshot {
            pid: channel.pid.clone(),
            status: channel.status_observer.get(),
            signal_len: channel.signal_queue.len(),
            msg_len: channel.msg_queue.len(),
            spawns: channel
                .spawns
                .read()
                .unwrap()
                .iter()
                .map(|instant| clock.zoned_at(instant.clone()))
                .collect(),
            exits: channel
                .exits
                .read()
                .unwrap()
                .iter()
                .map(|(instant, res)| {
                    (
                        clock.zoned_at(instant.clone()),
                        Exit::from_result(res.clone()),
                    )
                })
                .collect(),
            created_at: clock.zoned_at(channel.created_at),
        }
    }
}

impl<Q: ChannelKind> IntoDyn for Channel<Q> {
    type Ref<T: ChannelKind> = Channel<T>;

    fn into_dyn_unchecked<S>(self) -> Channel<S>
    where
        S: ChannelKind,
    {
        Channel {
            inner: self.inner,
            _marker: PhantomData,
        }
    }
}

impl<Q: ChannelKind> AsDyn for Channel<Q> {
    fn as_dyn_unchecked<S>(&self) -> &Channel<S>
    where
        S: ChannelKind,
    {
        // Sound because #[repr(transparent)] guarantees Channel<S>
        // and Channel<S2> share the exact layout of Arc<...>.
        unsafe { &*(self as *const Channel<Q> as *const Channel<S>) }
    }
}

impl<T: ChannelKind> Debug for Channel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("pid", &self.inner.pid)
            .field("status", &self.inner.status_observer.get())
            .field("len", &self.inner.msg_queue.len())
            .finish()
    }
}

impl<T: ChannelKind> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: ChannelKind> PartialEq for Channel<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.pid == other.inner.pid
    }
}
impl<T: ChannelKind> Eq for Channel<T> {}
impl<T: ChannelKind> Hash for Channel<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.pid.hash(state);
    }
}

#[derive(Clone, Copy)]
struct Clock {
    instant: Instant,
    timestamp: Timestamp,
}

impl Clock {
    fn now() -> Self {
        Self {
            instant: Instant::now(),
            timestamp: Timestamp::now(),
        }
    }

    fn timestamp_at(self, instant: Instant) -> Timestamp {
        let elapsed = instant.duration_since(self.instant);

        self.timestamp + SignedDuration::from_nanos(elapsed.as_nanos() as i64)
    }

    fn zoned_at(self, instant: Instant) -> Zoned {
        self.timestamp_at(instant).to_zoned(TimeZone::UTC)
    }
}
