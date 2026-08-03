use std::fmt::Debug;

use super::*;
use crate::signals::{Observable, SignalSender};
use polybox::{errors::SendError, type_sets::Set, *};

pub struct Address<T> {
    inbox: Inbox<T>,
    signal_inbox: SignalSender,
}

impl<T: Interface, M: Message> Sends<M> for Address<T>
where
    Inbox<T>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<Output<M>, SendError<M>> {
        self.inbox.send(msg).await
    }
}

impl<T: Interface> Observable for Address<T> {
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        this.signal_inbox.send(signal).await
    }
}

impl<T: Interface> DynPolyBox for Address<T> {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> futures::prelude::future::BoxFuture<'_, Result<(), errors::SendCheckedError<BoxedPayload>>>
    {
        self.inbox._send_boxed_payload_checked(msg)
    }
}

impl<T: Interface> PolyBox for Address<T> {
    type Set = T::Set;
    type AsDyn<R> = DynAddress<R>;

    fn into_dyn_unchecked<R>(self) -> Self::AsDyn<R> {
        DynAddress {
            inbox: self.inbox.into_dyn_unchecked(),
            signal_inbox: self.signal_inbox,
        }
    }
}

impl<T> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            signal_inbox: self.signal_inbox.clone(),
        }
    }
}

impl<T: Interface> Address<T> {
    pub(super) fn new(inbox: Inbox<T>, signal_inbox: SignalSender) -> Self {
        Self {
            inbox,
            signal_inbox,
        }
    }

    pub fn into_dyn(self) -> DynAddress<T::Set> {
        DynAddress {
            inbox: self.inbox.into_dyn(),
            signal_inbox: self.signal_inbox,
        }
    }
}

impl<T> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("inbox", &self.inbox)
            .field("signal_inbox", &self.signal_inbox)
            .finish()
    }
}

pub struct DynAddress<T = Set![]> {
    inbox: DynInbox<T>,
    signal_inbox: SignalSender,
}

impl<T: Interface, M: Message> Sends<M> for DynAddress<T>
where
    DynInbox<T>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<Output<M>, SendError<M>> {
        self.inbox.send(msg).await
    }
}

impl<T> Observable for DynAddress<T> {
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        this.signal_inbox.send(signal).await
    }
}

impl<T> DynPolyBox for DynAddress<T> {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> futures::prelude::future::BoxFuture<'_, Result<(), errors::SendCheckedError<BoxedPayload>>>
    {
        self.inbox._send_boxed_payload_checked(msg)
    }
}

impl<T: Interface> PolyBox for DynAddress<T> {
    type Set = T::Set;
    type AsDyn<R> = DynAddress<R>;

    fn into_dyn_unchecked<R>(self) -> Self::AsDyn<R> {
        DynAddress {
            inbox: self.inbox.into_dyn_unchecked(),
            signal_inbox: self.signal_inbox,
        }
    }
}

impl<T> Clone for DynAddress<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            signal_inbox: self.signal_inbox.clone(),
        }
    }
}

impl<T> Debug for DynAddress<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynAddress")
            .field("inbox", &std::any::type_name::<DynInbox<T>>())
            .field("signal_inbox", &self.signal_inbox)
            .finish()
    }
}
