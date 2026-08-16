use super::*;
use crate::{
    FromPayload, Message, MessageExt, Rx, TryIntoPayload,
    address::Address,
    new_request,
    signals::{self, ActorStatus, Event},
};
use eyeball::SharedObservable;
use futures::FutureExt as _;
use std::{any::TypeId, fmt::Debug, marker::PhantomData, panic::AssertUnwindSafe, sync::RwLock};
use tokio::{select, time::Instant};
use type_sets::{Contains, Set, TypeSet};

const SIGNAL_QUEUE_CAPACITY: usize = 1_000_000;
const MSG_QUEUE_CAPACITY: usize = 1_000_000;
pub(super) const BACKPRESSURE_LIMIT: usize = 100;
const KEEP_N_SPAWNS: usize = 5;
const KEEP_N_EXITS: usize = 5;

pub trait ChannelKind: 'static {
    type Set: TypeSet + 'static;
}

impl<I: Interface> ChannelKind for I {
    type Set = <I as Interface>::Set;
}

impl<S> ChannelKind for Set<S>
where
    Set<S>: TypeSet + 'static,
{
    type Set = Set<S>;
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
    exits: RwLock<Vec<(Instant, ExitResult)>>,
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
            status_observer: SharedObservable::new(ActorStatus::Exiting),
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

    fn add_spawned_now(&self) {
        let mut spawned_at = self.inner.spawns.write().unwrap();

        if spawned_at.len() > KEEP_N_SPAWNS {
            _ = spawned_at.remove(0);
        }

        spawned_at.push(Instant::now());
    }

    fn add_exited_now(&self, reason: ExitResult) {
        let mut exited_at = self.inner.exits.write().unwrap();

        if exited_at.len() > KEEP_N_EXITS {
            _ = exited_at.remove(0);
        }

        exited_at.push((Instant::now(), reason));
    }

    pub fn spawn_ref<R, F>(&self, f: impl FnOnce(EventStream<T>) -> F) -> Child<R, T>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, Report>> + Send + 'static,
        F::Output: Send + 'static,
    {
        self.clone().spawn(f)
    }

    pub fn spawn<R, F>(self, f: impl FnOnce(EventStream<T>) -> F) -> Child<R, T>
    where
        T: Interface,
        R: Send + 'static,
        F: Future<Output = Result<R, Report>> + Send + 'static,
        F::Output: Send + 'static,
    {
        let state = EventStream::new(self.clone());
        let spawned_future = AssertUnwindSafe(f(state)).catch_unwind();
        let pid = self.pid().clone();
        let this = self.clone();

        let handle = tokio::spawn(async move {
            // Notify that the process is alive
            tracing::debug!(pid = ?pid, "Process started");
            self.alert(ActorStatus::Running);

            // Run the future and catch any panics that occur
            let exit_value = spawned_future.await;

            // Depending on the exit_value, set the correct ExitSignal
            match exit_value {
                Ok(val) => {
                    match &val {
                        Ok(_) => {
                            tracing::debug!(pid = ?pid, "Process exited normally");
                            self.alert(ActorStatus::Exiting);
                            self.add_exited_now(Ok(()));
                        }
                        Err(_) => {
                            tracing::error!(pid = ?pid, "Process exited with error");
                            self.alert(ActorStatus::Exiting);
                            self.add_exited_now(Err(ExitError::UnhandledError));
                        }
                    };
                    val
                }
                Err(boxed) => {
                    tracing::error!(pid = ?pid, "Process panicked");
                    self.alert(ActorStatus::Exiting);
                    self.add_exited_now(Err(ExitError::Panic));
                    std::panic::resume_unwind(boxed);
                }
            }
        });

        this.add_spawned_now();
        Child::new(handle, Address::new(this))
    }

    pub(super) fn raw_queue(&self) -> Option<&ConcurrentQueue<T>>
    where
        T: Interface,
    {
        if self.is_interface::<T>() {
            Some(unsafe {
                &*(&self.inner.msg_queue as *const dyn IsDynQueue as *const ConcurrentQueue<T>)
            })
        } else {
            None
        }
    }

    pub(crate) fn alert(&self, status: ActorStatus) {
        self.inner.status_observer.set(status);

        if !status.should_accept_messages() {
            self.inner.msg_notifier.notify_waiters();
        }
    }

    pub(super) fn is_open(&self) -> bool {
        self.inner.status_observer.read().should_accept_messages()
    }

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
                self.inner.status_observer.set(ActorStatus::Exiting);
                Some(SignalEvent::StatusUpdate(ActorStatus::Exiting))
            }
            SignalInterface::Suspend(_) => {
                if self.status() == ActorStatus::Exiting {
                    tracing::warn!("Actor is exiting, cannot suspend");
                    None
                } else {
                    self.inner.status_observer.set(ActorStatus::Suspended);
                    Some(SignalEvent::StatusUpdate(ActorStatus::Suspended))
                }
            }
            SignalInterface::Resume(_) => {
                if self.status() != ActorStatus::Suspended {
                    tracing::warn!("Actor is not suspended, cannot resume");
                    None
                } else {
                    self.inner.status_observer.set(ActorStatus::Running);
                    Some(SignalEvent::StatusUpdate(ActorStatus::Running))
                }
            }
            SignalInterface::GetState((_, tx)) => Some(SignalEvent::GetState(tx)),
            SignalInterface::GetChildren((_, tx)) => Some(SignalEvent::GetChildren(tx)),
            SignalInterface::GetStatus((_, tx)) => {
                let _ = tx.send(self.status());
                None
            }
            SignalInterface::Ping((_, tx)) => {
                let _ = tx.send(());
                None
            }
        }
    }

    pub(crate) async fn recv(&self) -> Option<Event<I>> {
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
    T: 'static,
    Set<T>: TypeSet + Contains<M>,
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

impl<C: ChannelKind> ActorRef for Channel<C> {
    type ChannelKind = C;
    type Set = C::Set;

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
        match self.inner.msg_queue.try_push_msg(msg) {
            Ok(output) => {
                self.inner.msg_notifier.notify_one();
                Ok(output)
            }
            Err(e) => Err(e),
        }
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
        self.inner.msg_queue.type_id() == TypeId::of::<ConcurrentQueue<I>>()
    }

    fn address(&self) -> &Address<Self::ChannelKind> {
        Address::from_ref(self)
    }

    fn len(&self) -> usize {
        self.inner.msg_queue.len()
    }

    async fn watch_start(&self) {
        loop {
            let mut subscriber = self.inner.status_observer.subscribe();
            let status = self.status();

            if status == ActorStatus::Running {
                return;
            }

            let status = subscriber.next().await;
            if status != Some(ActorStatus::Exiting) {
                return;
            }
        }
    }

    async fn watch_exit(&self) {
        loop {
            let mut subscriber = self.inner.status_observer.subscribe();
            let status = self.status();

            if status == ActorStatus::Running {
                return;
            }

            let status = subscriber.next().await;
            if status == Some(ActorStatus::Exiting) {
                return;
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
