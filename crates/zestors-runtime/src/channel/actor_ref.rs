use super::*;
use std::{any::TypeId, future::Future};
use tokio::time::Instant;

pub trait ActorRef {
    type ChannelSpec: ChannelSpec<Set = Self::Set>;
    type Set: 'static;

    /// Same as [`Sends::send`], but checks whether the message type is accepted by the channel.
    fn send_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<MessageReceipt<M>, SendCheckedError<M>>> + Send;

    /// Same as [`Sends::try_send`], but checks whether the message type is accepted by the channel.
    fn try_send_dyn<M: Message>(&self, msg: M)
    -> Result<MessageReceipt<M>, TrySendCheckedError<M>>;

    /// Same as [`Sends::send_now`], but checks whether the message type is accepted by the channel.
    fn send_now_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, SendCheckedError<M>>;

    // /// Same as [`Sends::force_send`], but checks whether the message type is accepted by the channel.
    // fn force_send_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, NotAccepted<M>>;

    fn request_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Outcome, RequestCheckedError<M>>> + Send;

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

    fn watch_initialization(&self) -> impl Future<Output = Result<(), ExitStatus>> + Send;

    fn watch_exit(&self) -> impl Future<Output = Result<(), ExitError>> + Send;

    fn members(&self) -> &'static [TypeId];

    fn msg_len(&self) -> usize;

    fn msgs_is_empty(&self) -> bool {
        self.msg_len() == 0
    }

    fn can_send(&self, type_id: TypeId) -> bool {
        self.members().contains(&type_id)
    }

    fn is_superset_of(&self, type_ids: &[TypeId]) -> bool {
        type_ids.iter().all(|id| self.can_send(*id))
    }
    fn is_interface<I: Interface>(&self) -> bool;

    fn reached_backpressure(&self) -> bool;

    fn signal_shutdown(&self) -> bool;

    fn signal_suspend(&self) -> bool;

    fn signal_resume(&self) -> bool;

    fn ping(&self) -> Rx<()>;

    // fn address(&self) -> &Address<Self::ChannelSpec>;

    fn created_at(&self) -> Instant;

    fn last_spawned_at(&self) -> Option<Instant>;

    fn spawned_at(&self) -> Vec<Instant>;

    fn uptime(&self) -> Option<Duration> {
        self.last_spawned_at().map(|instant| instant.elapsed())
    }
}

pub trait AsActorRef {
    type ChannelSpec: ChannelSpec;

    fn channel_data(&self) -> &ChannelData<Self::ChannelSpec>;
    fn get_address(&self) -> Address<Self::ChannelSpec>;
}

impl<T: AsActorRef + Sync> ActorRef for T {
    type Set = <T::ChannelSpec as ChannelSpec>::Set;
    type ChannelSpec = T::ChannelSpec;

    async fn send_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, SendCheckedError<M>> {
        self.channel_data().send_dyn(msg).await
    }

    fn try_send_dyn<M: Message>(
        &self,
        msg: M,
    ) -> Result<MessageReceipt<M>, TrySendCheckedError<M>> {
        self.channel_data().try_send_dyn(msg)
    }

    fn send_now_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, SendCheckedError<M>> {
        self.channel_data().send_now_dyn(msg)
    }

    // fn force_send_dyn<M: Message>(&self, msg: M) -> Result<MessageReceipt<M>, NotAccepted<M>> {
    //     self.channel_data().force_send_dyn(msg)
    // }

    fn request_dyn<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Outcome, RequestCheckedError<M>>> + Send {
        self.channel_data().request_dyn(msg)
    }

    fn pid(&self) -> &Pid {
        self.channel_data().pid()
    }

    fn status(&self) -> ActorStatus {
        self.channel_data().status()
    }

    fn reached_backpressure(&self) -> bool {
        self.channel_data().reached_backpressure()
    }

    fn signal_shutdown(&self) -> bool {
        self.channel_data().signal_shutdown()
    }

    fn signal_suspend(&self) -> bool {
        self.channel_data().signal_suspend()
    }

    fn signal_resume(&self) -> bool {
        self.channel_data().signal_resume()
    }

    fn ping(&self) -> Rx<()> {
        self.channel_data().ping()
    }

    fn members(&self) -> &'static [TypeId] {
        self.channel_data().members()
    }

    fn is_interface<I: Interface>(&self) -> bool {
        self.channel_data().is_interface::<I>()
    }

    // fn address(&self) -> &Address<Self::ChannelSpec> {
    //     Address::from_ref(self.channel_data())
    // }

    fn msg_len(&self) -> usize {
        self.channel_data().msg_len()
    }

    fn watch_initialization(&self) -> impl Future<Output = Result<(), ExitStatus>> + Send {
        self.channel_data().watch_initialization()
    }

    fn watch_exit(&self) -> impl Future<Output = Result<(), ExitError>> + Send {
        self.channel_data().watch_exit()
    }

    fn created_at(&self) -> Instant {
        self.channel_data().created_at()
    }

    fn spawned_at(&self) -> Vec<Instant> {
        self.channel_data().spawned_at()
    }

    fn uptime(&self) -> Option<Duration> {
        self.channel_data().uptime()
    }

    fn last_spawned_at(&self) -> Option<Instant> {
        self.channel_data().last_spawned_at()
    }

    fn snapshot(&self) -> ChannelSnapshot {
        self.channel_data().snapshot()
    }
}
