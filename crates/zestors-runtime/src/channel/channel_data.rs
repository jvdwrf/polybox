use super::*;
use crate::{
    registry::Registry,
    signals::{self, Event},
};
use eyeball::SharedObservable;
use jiff::{SignedDuration, Timestamp, Zoned, tz::TimeZone};
use std::{
    any::TypeId,
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    sync::{
        RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{select, time::Instant};
use type_sets::{Contains, Set, TypeSet};

/// The `ChannelData` contained in either:
/// - [`Address`]: A weak reference to the channel data.
/// - [`Channel`]: A strong reference to the channel data, which can be used to spawn
/// a new actor on the same channel.
/// - [`Inbox`]: A strong reference to the channel data, which can be used to
/// receive messages
///
/// Once all strong references to the channel data are dropped, the actor will be
/// deregistered from the local registry and the channel data is dropped.
///
/// This means, that in order restart an actor, the [`Channel`] handle must be kept
/// alive.
#[repr(transparent)]
pub struct ChannelData<C: ChannelSpec = Set!()> {
    inner: Arc<ChannelDataInner<dyn Queue>>,
    _marker: PhantomData<fn() -> C>,
}

pub(crate) struct ChannelDataInner<Q: ?Sized> {
    pid: Pid,
    signal_queue: ConcurrentQueue<SignalInterface>,
    signal_notifier: Notify,
    status_observer: SharedObservable<ActorStatus>,
    msg_notifier: Notify,
    msg_backpressure_limit: usize,
    created_at: Instant,
    spawns: RwLock<Vec<Instant>>,
    exits: RwLock<Vec<(Instant, Result<(), ExitError>)>>,
    strong_count: AtomicUsize,
    msg_queue: Q,
}

impl<C: ChannelSpec> ChannelData<C> {
    pub(crate) fn clone_channel(&self) -> Self {
        Self {
            _marker: PhantomData,
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn decr_strong_count(&self) {
        let prev_count = self.inner().strong_count.fetch_sub(1, Ordering::Release);

        // fetch_sub returns the PREVIOUS value. If it was 1, it is now 0.
        if prev_count == 1 {
            // Synchronize memory access from other threads before cleaning up
            std::sync::atomic::fence(Ordering::Acquire);

            let removed_address = Registry::local().remove(self.pid());

            if removed_address.is_none() {
                if cfg!(debug_assertions) {
                    panic!(
                        "Channel {} was not found in the registry when dropping the last strong reference",
                        self.pid()
                    );
                } else {
                    tracing::error!(
                        "Channel {} was not found in the registry when dropping the last strong reference",
                        self.pid()
                    );
                }
            }
        }
    }

    pub(crate) fn incr_strong_count(&self) {
        // Relaxed is sufficient because the caller already owns a strong reference
        let prev_count = self.inner().strong_count.fetch_add(1, Ordering::Relaxed);

        // Prevent integer overflow attack/bug
        if prev_count > usize::MAX / 2 {
            std::process::abort();
        }
    }

    fn inner(&self) -> &ChannelDataInner<dyn Queue> {
        &self.inner
    }

    pub(super) fn set_status(&self, status: ActorStatus) {
        self.inner().status_observer.set(status);
    }

    pub(super) fn register_spawn(&self) {
        tracing::debug!("Process spawned");

        let mut spawned_at = self.inner().spawns.write().unwrap();

        if spawned_at.len() > KEEP_N_SPAWNS {
            _ = spawned_at.remove(0);
        }

        spawned_at.push(Instant::now());
        self.set_status(ActorStatus::Initializing);
    }

    #[expect(unused)]
    pub(super) fn pop_dyn(&self) -> Result<DynEnvelope, PopError> {
        self.inner().msg_queue.pop_dyn()
    }

    pub(super) fn register_exit(&self, reason: Result<(), ExitError>) {
        match &reason {
            Ok(()) => tracing::debug!("Process exited normally"),
            Err(err) => tracing::warn!("Process exited with error: {:?}", err),
        }

        let mut exited_at = self.inner().exits.write().unwrap();

        if exited_at.len() > KEEP_N_EXITS {
            _ = exited_at.remove(0);
        }

        exited_at.push((Instant::now(), reason));
        self.set_status(ActorStatus::Exited(ExitStatus::from_result(reason)));
    }

    pub(super) fn register_initialized(&self) {
        tracing::debug!("Process initialized");
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
        self.set_status(ActorStatus::ShuttingDown);
    }

    pub(super) fn raw_queue(&self) -> Option<&ConcurrentQueue<C>>
    where
        C: Interface,
    {
        if self.is_interface::<C>() {
            // SAFETY: We just checked that the channel's message queue is of type `ConcurrentQueue<C>`.
            Some(unsafe { self.raw_queue_unchecked() })
        } else {
            None
        }
    }

    /// # Safety
    /// This function is unsafe because it assumes that the channel's message queue is of type
    /// `ConcurrentQueue<T>`. If this assumption is incorrect, it may lead to undefined behavior.
    unsafe fn raw_queue_unchecked(&self) -> &ConcurrentQueue<C>
    where
        C: Interface,
    {
        unsafe { &*(&self.inner().msg_queue as *const dyn Queue as *const ConcurrentQueue<C>) }
    }

    pub(super) fn backpressure(&self) -> &BackPressure {
        BackPressure::default()
    }

    pub(super) async fn delay_for_backpressure(&self) {
        let len = self.inner().msg_queue.len();
        let limit = self.inner().msg_backpressure_limit;

        if let Some(delay) = self.backpressure().delay(
            self.inner().msg_queue.len(),
            self.inner().msg_backpressure_limit,
        ) {
            tracing::warn!(
                "Backpressure applied: queue occupancy = {:.2}%, delay = {:?}",
                len as f32 / limit as f32 * 100.0,
                delay
            );
            tokio::time::sleep(delay).await;
        }
    }

    fn _signal(&self, signal: SignalInterface) -> bool {
        if matches!(
            self.status(),
            ActorStatus::Exited(_) | ActorStatus::ShuttingDown
        ) {
            return false;
        }

        match self.inner().signal_queue.push(signal) {
            Ok(_) => {
                self.inner().signal_notifier.notify_one();
            }
            Err(e) => {
                tracing::error!(
                    "Signal queue for {} contains more than {} signals. The signal has been lost. Error: {:?}",
                    std::any::type_name::<Self>(),
                    SIGNAL_QUEUE_CAPACITY,
                    e
                );
                return false;
            }
        }

        true
    }
}

impl<I: Interface> ChannelData<I> {
    pub(super) fn new(pid: Pid, strong_count: usize) -> Self {
        let inner: Arc<ChannelDataInner<dyn Queue>> = Arc::new(ChannelDataInner {
            pid,
            msg_notifier: Notify::new(),
            msg_backpressure_limit: BACKPRESSURE_LIMIT,
            signal_queue: ConcurrentQueue::bounded(SIGNAL_QUEUE_CAPACITY),
            signal_notifier: Notify::new(),
            status_observer: SharedObservable::new(ActorStatus::Exited(ExitStatus::Normal)),
            msg_queue: ConcurrentQueue::<I>::bounded(MSG_QUEUE_CAPACITY),
            created_at: Instant::now(),
            spawns: Default::default(),
            exits: Default::default(),
            strong_count: AtomicUsize::new(strong_count),
        });

        Self {
            inner: inner,
            _marker: PhantomData,
        }
    }

    pub(crate) async fn recv_msg(&self) -> Option<I> {
        let raw_queue = self
            .raw_queue()
            .expect("Channel is not of the expected interface type");

        let mut notify = pin!(self.inner().msg_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Ok(msg) = raw_queue.pop() {
                return Some(msg);
            }

            notify.as_mut().await;
            notify.set(self.inner().msg_notifier.notified());
        }
    }

    pub(crate) fn pop_msg(&self) -> Option<I> {
        let raw_queue = self
            .raw_queue()
            .expect("Channel is not of the expected interface type");

        raw_queue.pop().ok()
    }

    pub(crate) fn drain_messages_and_signals(&self) {
        while let Some(msg) = self.pop_msg() {
            drop(msg);
        }

        while let Some(signal) = self.pop_signal() {
            drop(signal);
        }
    }

    pub(crate) async fn recv_signal(&self) -> Option<Signal> {
        let mut notify = pin!(self.inner().signal_notifier.notified());

        loop {
            notify.as_mut().enable();

            if let Some(signal) = self.pop_signal() {
                return Some(signal);
            }

            notify.as_mut().await;

            notify.set(self.inner().signal_notifier.notified());
        }
    }

    pub(crate) fn pop_signal(&self) -> Option<Signal> {
        loop {
            match self.inner().signal_queue.pop() {
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

    fn handle_signal(&self, signal: SignalInterface) -> Option<Signal> {
        match signal {
            SignalInterface::Shutdown(_) => {
                self.register_shutdown();
                Some(Signal::Shutdown)
            }
            SignalInterface::Suspend(_) => {
                if self.status() == ActorStatus::ShuttingDown {
                    tracing::warn!("Actor is exiting, cannot suspend");
                    None
                } else {
                    self.register_suspend();
                    Some(Signal::Suspend)
                }
            }
            SignalInterface::Resume(_) => {
                if self.status() != ActorStatus::Suspended {
                    tracing::warn!("Actor is not suspended, cannot resume");
                    None
                } else {
                    self.register_resume();
                    Some(Signal::Resume)
                }
            }
            SignalInterface::Ping(envelope) => {
                let _ = envelope.handle.send(());
                None
            }
        }
    }

    pub(crate) async fn next(&self) -> Option<Event<I>> {
        if self.status() == ActorStatus::Suspended {
            return self.recv_signal().await.map(Event::Signal);
        }

        // If the actor is shutting down and there are no more messages, return None
        if self.status() == ActorStatus::ShuttingDown && self.msgs_is_empty() {
            return None;
        }

        select! {
            biased;

            Some(msg) = self.recv_msg() => Some(Event::Message(msg)),
            Some(signal) = self.recv_signal() => Some(Event::Signal(signal)),
            else => None,
        }
    }
}

impl<C: ChannelSpec> Debug for ChannelData<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelData")
            .field("pid", &self.inner().pid)
            .field("status", &self.inner().status_observer.get())
            .field("len", &self.inner().msg_queue.len())
            .finish()
    }
}

impl<C: ChannelSpec> Eq for ChannelData<C> {}
impl<C: ChannelSpec> PartialEq for ChannelData<C> {
    fn eq(&self, other: &Self) -> bool {
        self.inner().pid == other.inner().pid
    }
}
impl<C: ChannelSpec> Hash for ChannelData<C> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner().pid.hash(state);
    }
}

impl<C: ChannelSpec> ActorRef for ChannelData<C> {
    type ChannelSpec = C;
    type Set = C::Set;

    async fn send_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, SendCheckedError<M>> {
        self.delay_for_backpressure().await;
        self.send_now_dyn(msg)
    }

    fn try_send_dyn<M: Message>(
        &self,
        msg: M,
    ) -> Result<MessageReceipt<M>, TrySendCheckedError<M>> {
        if self.reached_backpressure() {
            return Err(TrySendCheckedError::Full(msg));
        }

        self.send_now_dyn(msg).map_err(Into::into)
    }

    fn send_now_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, SendCheckedError<M>> {
        if !self.status().accepts_messages() {
            return Err(SendCheckedError::Closed(msg));
        }

        match self.inner().msg_queue.try_push_msg(msg) {
            Ok(output) => {
                self.inner().msg_notifier.notify_one();
                Ok(output)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn request_dyn<M: Message>(&self, msg: M) -> Result<M::Outcome, RequestCheckedError<M>> {
        Ok(self.send_dyn(msg).await?.wait().await?)
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

    fn signal(&self, signal: Signal) -> bool {
        let interface = match signal {
            Signal::Shutdown => SignalInterface::Shutdown(Envelope::new(signals::Shutdown, ())),
            Signal::Suspend => SignalInterface::Suspend(Envelope::new(signals::Suspend, ())),
            Signal::Resume => SignalInterface::Resume(Envelope::new(signals::Resume, ())),
        };

        self._signal(interface)
    }

    fn ping(&self) -> Rx<()> {
        let (tx, rx) = new_request();
        self._signal(SignalInterface::Ping(Envelope::new(signals::Ping, tx)));
        rx
    }

    fn is_interface<I: Interface>(&self) -> bool {
        self.inner.msg_queue.type_id() == TypeId::of::<ConcurrentQueue<I>>()
    }

    fn msg_len(&self) -> usize {
        self.inner.msg_queue.len()
    }

    async fn watch_initialization(&self) -> Result<(), ExitStatus> {
        let mut subscriber = self.inner.status_observer.subscribe();

        loop {
            let status = self.status();

            match status {
                ActorStatus::Running => return Ok(()),
                ActorStatus::Exited(exit) => {
                    return Err(exit);
                }
                _ => {}
            }

            let status = subscriber.next().await;

            match status {
                Some(ActorStatus::Running) => return Ok(()),
                Some(ActorStatus::Exited(exit)) => {
                    return Err(exit);
                }
                _ => {}
            }
        }
    }

    async fn watch_exit(&self) -> Result<(), ExitError> {
        let mut subscriber = self.inner.status_observer.subscribe();

        loop {
            let status = self.status();

            if let ActorStatus::Exited(exit) = status {
                return exit.into_result();
            }

            let status = subscriber.next().await;

            if let Some(ActorStatus::Exited(exit)) = status {
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
                        ExitStatus::from_result(res.clone()),
                    )
                })
                .collect(),
            created_at: clock.zoned_at(channel.created_at),
        }
    }

    fn get_address(&self) -> Address<Self::ChannelSpec> {
        Address::new(self)
    }

    fn strong_count(&self) -> usize {
        self.inner().strong_count.load(Ordering::Relaxed)
    }

    fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    fn address(&self) -> &Address<Self::ChannelSpec> {
        Address::new_ref(self)
    }
}

impl<M, T> Sends<M> for ChannelData<Set<T>>
where
    M: Message,
    T: TypeSet + Contains<M> + 'static,
{
    async fn send(&self, msg: M) -> Result<MessageReceipt<M>, SendError<M>> {
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

    fn try_send(&self, msg: M) -> Result<MessageReceipt<M>, TrySendError<M>> {
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

    fn send_now(&self, msg: M) -> Result<MessageReceipt<M>, SendError<M>> {
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

    // fn force_send(&self, msg: M) -> MessageReceipt<M> {
    //     match self.inner.msg_queue.try_push_msg(msg) {
    //         Ok(output) => {
    //             self.inner.msg_notifier.notify_one();
    //             output
    //         }
    //         Err(NotAccepted(_)) => {
    //             panic!(
    //                 "Message type {} not accepted by channel {}",
    //                 std::any::type_name::<M>(),
    //                 std::any::type_name::<Self>(),
    //             );
    //         }
    //     }
    // }
}

impl<M, I> Sends<M> for ChannelData<I>
where
    M: Message,
    I: Interface + TryInto<Envelope<M>> + From<Envelope<M>> + Send + 'static,
{
    async fn send(&self, msg: M) -> Result<MessageReceipt<M>, SendError<M>> {
        self.delay_for_backpressure().await;
        self.send_now(msg)
    }

    fn try_send(&self, msg: M) -> Result<MessageReceipt<M>, TrySendError<M>> {
        if self.reached_backpressure() {
            return Err(TrySendError::Full(msg));
        }

        self.send_now(msg).map_err(Into::into)
    }

    fn send_now(&self, msg: M) -> Result<MessageReceipt<M>, SendError<M>> {
        if !self.status().accepts_messages() {
            return Err(SendError(msg));
        }

        if let Some(queue) = self.raw_queue() {
            let (envelope, receipt) = Envelope::new_pair(msg);
            let interface = I::from(envelope);

            if let Err(_e) = queue.push(interface) {
                panic!("Queue was full or empty {}", std::any::type_name::<Self>());
            }

            Ok(receipt)
        } else {
            match self.send_now_dyn(msg) {
                Err(SendCheckedError::NotAccepted(_)) => {
                    panic!(
                        "Message type {} not accepted by channel {}",
                        std::any::type_name::<M>(),
                        std::any::type_name::<Self>(),
                    );
                }
                Err(SendCheckedError::Closed(msg)) => Err(SendError(msg)),
                Ok(output) => Ok(output),
            }
        }
    }

    // fn force_send(&self, msg: M) -> MessageReceipt<M> {
    //     // Raw-queue could just be a raw pointer cast, but that requires that a
    //     // Channel<I: Interface> always has a ConcurrentQueue<I> as its msg_queue,
    //     // This is currently not guaranteed by the type system.
    //     if let Some(queue) = self.raw_queue() {
    //         let (envelope, receipt) = Envelope::new_pair(msg);
    //         let interface = I::from(envelope);

    //         if let Err(_e) = queue.push(interface) {
    //             panic!("Queue was full or empty {}", std::any::type_name::<Self>());
    //         }

    //         receipt
    //     } else {
    //         match self.force_send_dyn(msg) {
    //             Ok(output) => output,
    //             Err(NotAccepted(_)) => {
    //                 panic!(
    //                     "Message type {} not accepted by channel {}",
    //                     std::any::type_name::<M>(),
    //                     std::any::type_name::<Self>(),
    //                 );
    //             }
    //         }
    //     }
    // }
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
