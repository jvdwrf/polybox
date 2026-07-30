use crate::signals::{Observable, Shutdown, SignalSender};
use polybox::{errors::SendError, *};

use super::*;

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

pub struct DynAddress<T> {
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

impl<T: Interface> Observable for DynAddress<T> {
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        this.signal_inbox.send(signal).await
    }
}
