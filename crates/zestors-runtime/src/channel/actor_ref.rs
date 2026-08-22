use super::*;
use std::{any::TypeId, future::Future};
use tokio::time::Instant;

pub trait ActorRef {
    type ChannelKind: ChannelKind<Set = Self::Set>;
    type Set: 'static;

    /// Same as [`Sends::send`], but checks whether the message type is accepted by the channel.
    fn send_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Receipt, SendCheckedError<M>>> + Send;

    /// Same as [`Sends::try_send`], but checks whether the message type is accepted by the channel.
    fn try_send_dyn<M: Message>(&self, msg: M) -> Result<M::Receipt, TrySendCheckedError<M>>;

    /// Same as [`Sends::send_now`], but checks whether the message type is accepted by the channel.
    fn send_now_dyn<M: Message>(&self, msg: M) -> Result<M::Receipt, SendCheckedError<M>>;

    /// Same as [`Sends::force_send`], but checks whether the message type is accepted by the channel.
    fn force_send_dyn<M: Message>(&self, msg: M) -> Result<M::Receipt, NotAccepted<M>>;

    fn request_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Output, RequestCheckedError<M>>> + Send;

    fn pid(&self) -> &Pid;

    fn status(&self) -> ActorStatus;

    fn snapshot(&self) -> ChannelSnapshot;

    fn watch_start(&self) -> impl Future<Output = ()> + Send
    where
        Self: Sync,
    {
        async move {
            loop {
                if let Ok(_) = self.watch_initialization().await {
                    return;
                }
            }
        }
    }

    fn watch_initialization(&self) -> impl Future<Output = Result<(), Exit>> + Send;

    fn watch_exit(&self) -> impl Future<Output = Result<(), ExitError>> + Send;

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

    // fn get_debug_state(
    //     &self,
    // ) -> impl Future<Output = Result<DebugState, RequestCheckedError<GetDebug>>> + Send;

    // fn get_children(
    //     &self,
    // ) -> impl Future<Output = Result<Vec<ChildDescription>, RequestCheckedError<GetChildren>>> + Send;

    fn ping(&self) -> Rx<()>;

    fn address(&self) -> &Address<Self::ChannelKind>;

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
    type ChannelKind = T::QueueType;

    fn send_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Receipt, SendCheckedError<M>>> + Send {
        self.as_channel().send_dyn(msg)
    }

    fn try_send_dyn<M: Message>(&self, msg: M) -> Result<M::Receipt, TrySendCheckedError<M>> {
        self.as_channel().try_send_dyn(msg)
    }

    fn send_now_dyn<M: Message>(&self, msg: M) -> Result<M::Receipt, SendCheckedError<M>> {
        self.as_channel().send_now_dyn(msg)
    }

    fn force_send_dyn<M: Message>(&self, msg: M) -> Result<M::Receipt, NotAccepted<M>> {
        self.as_channel().force_send_dyn(msg)
    }

    fn request_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Output, RequestCheckedError<M>>> + Send {
        self.as_channel().request_dyn(msg)
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

    fn ping(&self) -> Rx<()> {
        self.as_channel().ping()
    }

    fn members(&self) -> &'static [TypeId] {
        self.as_channel().members()
    }

    fn is_interface<I: Interface>(&self) -> bool {
        self.as_channel().is_interface::<I>()
    }

    fn address(&self) -> &Address<Self::ChannelKind> {
        Address::from_ref(self.as_channel())
    }

    fn len(&self) -> usize {
        self.as_channel().len()
    }

    fn watch_initialization(&self) -> impl Future<Output = Result<(), Exit>> + Send {
        self.as_channel().watch_initialization()
    }

    fn watch_exit(&self) -> impl Future<Output = Result<(), ExitError>> + Send {
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

    fn snapshot(&self) -> ChannelSnapshot {
        self.as_channel().snapshot()
    }
}
