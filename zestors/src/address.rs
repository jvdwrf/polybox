use super::*;
use crate::signals::{Observable, SignalSender};
use polybox::{
    errors::SendError,
    type_sets::{Members, Set},
    *,
};
use std::{any::Any, fmt::Debug, marker::PhantomData};

pub struct Address<T: InboxKind> {
    inbox: T::Inbox,
    signal_inbox: SignalSender,
}

pub type DynAddress<T = Set![]> = Address<Dyn<T>>;

impl<T: InboxKind, M: Message> Sends<M> for Address<T>
where
    T::Inbox: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<Output<M>, SendError<M>> {
        self.inbox.send(msg).await
    }
}

impl<T: InboxKind> Observable for Address<T> {
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        this.signal_inbox.send(signal).await
    }
}

impl<T: InboxKind> DynPolyBox for Address<T> {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> futures::prelude::future::BoxFuture<'_, Result<(), errors::SendCheckedError<BoxedPayload>>>
    {
        self.inbox._send_boxed_payload_checked(msg)
    }
}

impl<T: InboxKind> PolyBox for Address<T> {
    type Set = T::Set;
    type Dyn<R: Members> = Address<Dyn<R>>;

    fn into_dyn_unchecked<R: Members>(self) -> Address<Dyn<R>> {
        Address {
            inbox: T::map_inbox_into_dyn_unchecked(self.inbox),
            signal_inbox: self.signal_inbox,
        }
    }
}

impl<T: InboxKind> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            signal_inbox: self.signal_inbox.clone(),
        }
    }
}

impl<T: InboxKind> Address<T> {
    pub(super) fn new(inbox: T::Inbox, signal_inbox: SignalSender) -> Self {
        Self {
            inbox,
            signal_inbox,
        }
    }
}

impl<T: InboxKind> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("inbox", &self.inbox)
            .field("signal_inbox", &self.signal_inbox)
            .finish()
    }
}

pub trait InboxKind {
    type Inbox: PolyBox + Clone + Debug + Unpin;
    type Set: Members;
    type Stream: Send + Sync + 'static;

    fn map_inbox_into_dyn_unchecked<R: Members>(address: Self::Inbox) -> DynInbox<R>;
}

pub struct Dyn<T>(PhantomData<fn() -> T>);

impl<T: Members> InboxKind for Dyn<T> {
    type Inbox = DynInbox<T>;
    type Set = T;
    type Stream = Box<dyn Any + Send + Sync>;

    fn map_inbox_into_dyn_unchecked<R: Members>(inbox: Self::Inbox) -> DynInbox<R> {
        inbox.into_dyn_unchecked()
    }
}

impl<T: Interface> InboxKind for T {
    type Inbox = Inbox<T>;
    type Set = T::Set;
    type Stream = EventStream<T>;

    fn map_inbox_into_dyn_unchecked<R: Members>(inbox: Self::Inbox) -> DynInbox<R> {
        inbox.into_dyn_unchecked()
    }
}
