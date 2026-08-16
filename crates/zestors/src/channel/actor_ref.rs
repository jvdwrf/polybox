use super::*;
use crate::{
    Message, Rx,
    address::Address,
    signals::{ActorStatus, ChildDescription, DebugState},
};
use std::{any::TypeId, future::Future};
use tokio::time::Instant;
use type_sets::{SubsetOf, TypeSet};

/// Provides message-sending operations for a channel.
///
/// `Sends` exposes four levels of delivery semantics:
///
/// - [`Sends::send`] applies backpressure and asynchronously waits when the
///   channel is under load.
/// - [`Sends::try_send`] applies backpressure but never waits, returning
///   [`ClosedOrFull::Full`] when the channel is under backpressure.
/// - [`Sends::send_now`] checks whether the channel is open but ignores
///   backpressure.
/// - [`Sends::force_send`] ignores both backpressure and the channel status.
pub trait Sends<M: Message>: Sync {
    /// Sends a message, applying backpressure when the channel is under load.
    ///
    /// This method waits asynchronously while backpressure is active. It
    /// returns [`Closed`] if the channel is closed.
    ///
    /// Unlike [`Sends::try_send`], this method waits rather than returning
    /// immediately when backpressure is active.
    fn send(&self, msg: M) -> impl Future<Output = Result<M::Output, SendError<M>>> + Send;

    /// Attempts to send a message without waiting.
    ///
    /// Returns [`ClosedOrFull::Full`] if the channel has reached its
    /// backpressure limit, or [`ClosedOrFull::Closed`] if the channel is
    /// closed.
    ///
    /// Unlike [`Sends::send`], this method never waits for backpressure to
    /// subside.
    fn try_send(&self, msg: M) -> Result<M::Output, TrySendError<M>>;

    /// Sends a message immediately if the channel is open.
    ///
    /// This method ignores backpressure, but still checks whether the channel
    /// is accepting messages. It returns [`Closed`] if the channel is closed.
    ///
    /// Use [`Sends::force_send`] when the channel status should also be
    /// ignored.
    fn send_now(&self, msg: M) -> Result<M::Output, SendError<M>>;

    /// Sends a message immediately, ignoring backpressure and channel status.
    ///
    /// This is the lowest-level sending operation. The message is queued even
    /// when the channel is closed.
    ///
    /// If the underlying queue is at capacity, the message is dropped and the
    /// implementation may log the overflow.
    fn force_send(&self, msg: M) -> M::Output;
}

pub trait ActorRef {
    type QueueType: ChannelKind<Set = Self::Set>;
    type Set: 'static;

    /// Same as [`Sends::send`], but checks whether the message type is accepted by the channel.
    fn send_checked<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Output, SendCheckedError<M>>> + Send;

    /// Same as [`Sends::try_send`], but checks whether the message type is accepted by the channel.
    fn try_send_checked<M: Message>(&self, msg: M) -> Result<M::Output, TrySendCheckedError<M>>;

    /// Same as [`Sends::send_now`], but checks whether the message type is accepted by the channel.
    fn send_now_checked<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>>;

    /// Same as [`Sends::force_send`], but checks whether the message type is accepted by the channel.
    fn force_send_checked<M: Message>(&self, msg: M) -> Result<M::Output, NotAccepted<M>>;

    fn pid(&self) -> &Pid;

    fn status(&self) -> ActorStatus;

    fn watch_start(&self) -> impl Future<Output = ()> + Send;
    fn watch_exit(&self) -> impl Future<Output = ()> + Send;

    fn members(&self) -> &'static [TypeId];

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn can_send(&self, type_id: TypeId) -> bool {
        self.members().contains(&type_id)
    }

    fn is_superset_of(&self, type_ids: &[TypeId]) -> bool {
        type_ids.iter().all(|id| self.can_send(*id))
    }
    fn is_interface<I: Interface>(&self) -> bool;

    fn reached_backpressure(&self) -> bool;

    fn signal_shutdown(&self);
    fn signal_suspend(&self);
    fn signal_resume(&self);
    fn get_status(&self) -> Rx<ActorStatus>;
    fn get_debug_state(&self) -> Rx<DebugState>;
    fn ping(&self) -> Rx<()>;
    fn get_children(&self) -> Rx<Vec<ChildDescription>>;
    fn address(&self) -> &Address<Self::QueueType>;
    fn created_at(&self) -> Instant;
    fn last_spawned_at(&self) -> Option<Instant>;
    fn spawned_at(&self) -> Vec<Instant>;
    fn uptime(&self) -> Option<Duration> {
        self.last_spawned_at().map(|instant| instant.elapsed())
    }
}

pub trait AsActorRef {
    type QueueType: ChannelKind;

    fn as_channel(&self) -> &Channel<Self::QueueType>;
}

impl<T: AsActorRef> ActorRef for T {
    type Set = <T::QueueType as ChannelKind>::Set;
    type QueueType = T::QueueType;

    fn send_checked<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Output, SendCheckedError<M>>> + Send {
        self.as_channel().send_checked(msg)
    }

    fn try_send_checked<M: Message>(&self, msg: M) -> Result<M::Output, TrySendCheckedError<M>> {
        self.as_channel().try_send_checked(msg)
    }

    fn send_now_checked<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>> {
        self.as_channel().send_now_checked(msg)
    }

    fn force_send_checked<M: Message>(&self, msg: M) -> Result<M::Output, NotAccepted<M>> {
        self.as_channel().force_send_checked(msg)
    }

    fn pid(&self) -> &Pid {
        self.as_channel().pid()
    }

    fn status(&self) -> ActorStatus {
        self.as_channel().status()
    }

    fn reached_backpressure(&self) -> bool {
        self.as_channel().reached_backpressure()
    }

    fn signal_shutdown(&self) {
        self.as_channel().signal_shutdown()
    }

    fn signal_suspend(&self) {
        self.as_channel().signal_suspend()
    }

    fn signal_resume(&self) {
        self.as_channel().signal_resume()
    }

    fn get_status(&self) -> Rx<ActorStatus> {
        self.as_channel().get_status()
    }

    fn get_debug_state(&self) -> Rx<DebugState> {
        self.as_channel().get_debug_state()
    }

    fn ping(&self) -> Rx<()> {
        self.as_channel().ping()
    }

    fn get_children(&self) -> Rx<Vec<ChildDescription>> {
        self.as_channel().get_children()
    }

    fn members(&self) -> &'static [TypeId] {
        self.as_channel().members()
    }

    fn is_interface<I: Interface>(&self) -> bool {
        self.as_channel().is_interface::<I>()
    }

    fn address(&self) -> &Address<Self::QueueType> {
        Address::from_ref(self.as_channel())
    }

    fn len(&self) -> usize {
        self.as_channel().len()
    }

    fn watch_start(&self) -> impl Future<Output = ()> + Send {
        self.as_channel().watch_start()
    }

    fn watch_exit(&self) -> impl Future<Output = ()> + Send {
        self.as_channel().watch_exit()
    }

    fn created_at(&self) -> Instant {
        self.as_channel().created_at()
    }

    fn spawned_at(&self) -> Vec<Instant> {
        self.as_channel().spawned_at()
    }

    fn uptime(&self) -> Option<Duration> {
        self.as_channel().uptime()
    }

    fn last_spawned_at(&self) -> Option<Instant> {
        self.as_channel().last_spawned_at()
    }
}

impl<M, T> Sends<M> for T
where
    T: AsActorRef + Sync,
    M: Message,
    Channel<T::QueueType>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<<M as Message>::Output, SendError<M>> {
        self.as_channel().send(msg).await
    }

    fn try_send(&self, msg: M) -> Result<<M as Message>::Output, TrySendError<M>> {
        self.as_channel().try_send(msg)
    }

    fn send_now(&self, msg: M) -> Result<<M as Message>::Output, SendError<M>> {
        self.as_channel().send_now(msg)
    }

    fn force_send(&self, msg: M) -> <M as Message>::Output {
        self.as_channel().force_send(msg)
    }
}

pub trait IntoDyn: ActorRef + Sized {
    type Ref<T: ChannelKind>;

    fn into_dyn_unchecked<S>(self) -> Self::Ref<S>
    where
        S: ChannelKind;

    fn into_dyn<S>(self) -> Self::Ref<S>
    where
        S: ChannelKind + SubsetOf<Self::Set>,
    {
        self.into_dyn_unchecked()
    }

    fn into_dyn_checked<S>(self) -> Result<Self::Ref<S>, Self>
    where
        S: TypeSet + ChannelKind,
    {
        if self.is_superset_of(S::members()) {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }

    fn downcast<I>(self) -> Result<Self::Ref<I>, Self>
    where
        I: Interface,
    {
        if self.is_interface::<I>() {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }
}

pub trait AsDyn: IntoDyn {
    fn as_dyn_unchecked<S>(&self) -> &Self::Ref<S>
    where
        S: ChannelKind;

    fn as_dyn<S>(&self) -> &Self::Ref<S>
    where
        S: ChannelKind + SubsetOf<Self::Set>,
    {
        self.as_dyn_unchecked()
    }

    fn as_dyn_checked<S>(&self) -> Option<&Self::Ref<S>>
    where
        S: TypeSet + ChannelKind,
    {
        if self.is_superset_of(S::members()) {
            Some(self.as_dyn_unchecked())
        } else {
            None
        }
    }

    fn downcast_ref<I>(&self) -> Option<&Self::Ref<I>>
    where
        I: Interface,
    {
        if self.is_interface::<I>() {
            Some(self.as_dyn_unchecked())
        } else {
            None
        }
    }
}
