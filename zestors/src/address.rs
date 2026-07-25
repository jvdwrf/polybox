use crate::signals::{Observable, Shutdown};

use super::*;

pub struct Address<T> {
    inbox: TokioInbox<T>,
    signal_inbox: TokioInbox<Signal>,
}

impl<T: Interface, M: Message> Sends<M> for Address<T>
where
    TokioInbox<T>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<Output<M>, SendError<M>> {
        self.inbox.send(msg).await
    }
}

impl<T> Observable for Address<T>
where
    T: Interface,
{
    async fn send_signal_payload(this: &Self, signal: Signal) -> Result<(), SendError<Signal>> {
        this.signal_inbox.send(signal).await
    }
}

impl<T: Interface> Address<T> {
    pub(super) fn new(inbox: TokioInbox<T>, signal_inbox: TokioInbox<Signal>) -> Self {
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
    signal_inbox: TokioInbox<Signal>,
}

impl<T: Interface, M: Message> Sends<M> for DynAddress<T>
where
    DynInbox<T>: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<Output<M>, SendError<M>> {
        self.inbox.send(msg).await
    }
}
