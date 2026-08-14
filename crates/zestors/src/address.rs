use crate::_prelude::*;
use crate::signals::{Observable, SignalSender};
use polybox::{errors::SendError, type_sets::Set};
use std::{any::Any, fmt::Debug, sync::Arc};

pub struct Address<T: InboxKind = Dyn<Set![]>> {
    inbox: T::Inbox,
    signal_inbox: SignalSender,
    process_watcher: ProcessWatcher,
    pid: Pid,
}

pub type DynAddress<T = Set![]> = Address<Dyn<T>>;

impl<T: InboxKind, M: Message> Sends<M> for Address<T>
where
    T::Inbox: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        if !self.is_alive() {
            return Err(SendError(msg));
        }

        self.inbox.send(msg).await
    }
}

impl<T: InboxKind> Observable for Address<T> {
    async fn send_signal(&self, signal: SignalInterface) -> Result<(), SendError<SignalInterface>> {
        self.signal_inbox.send(signal).await
    }
}

impl<T: InboxKind> DynPolyBox for Address<T> {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> futures::prelude::future::BoxFuture<'_, Result<(), errors::SendCheckedError<BoxedPayload>>>
    {
        if !self.is_alive() {
            return futures::future::ready(Err(errors::SendCheckedError::Closed(msg))).boxed();
        }

        self.inbox._send_boxed_payload_checked(msg)
    }
}

impl<T: InboxKind> PolyBox for Address<T> {
    type Set = T::Set;
    type Dyn<R: TypeSet + 'static> = Address<Dyn<R>>;

    fn into_dyn_unchecked<R: TypeSet + 'static>(self) -> Self::Dyn<R> {
        Address {
            inbox: T::map_inbox_into_dyn_unchecked(self.inbox),
            signal_inbox: self.signal_inbox,
            process_watcher: self.process_watcher,
            pid: self.pid,
        }
    }
}

impl<T: InboxKind> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            signal_inbox: self.signal_inbox.clone(),
            process_watcher: self.process_watcher.clone(),
            pid: self.pid.clone(),
        }
    }
}

impl<T: InboxKind> Address<T> {
    pub(super) fn new(
        inbox: T::Inbox,
        signal_inbox: SignalSender,
        process_watcher: ProcessWatcher,
        pid: Pid,
    ) -> Self {
        Self {
            inbox,
            signal_inbox,
            process_watcher,
            pid,
        }
    }

    pub fn into_dyn_unchecked_test(self) -> Address {
        todo!()
    }

    pub fn exit(&self) -> &ProcessWatcher {
        &self.process_watcher
    }

    pub fn exit_mut(&mut self) -> &mut ProcessWatcher {
        &mut self.process_watcher
    }

    pub fn is_alive(&self) -> bool {
        self.process_watcher.is_alive()
    }

    pub fn is_same_process<R: InboxKind>(&self, other: &Address<R>) -> bool {
        self.pid == other.pid
    }

    pub fn pid(&self) -> &Pid {
        &self.pid
    }

    pub async fn watch_exit(&mut self) {
        self.process_watcher.watch_exit().await
    }

    pub async fn watch_start(&mut self) {
        self.process_watcher.watch_start().await
    }
}

impl<T: TypeSet + 'static> Address<Dyn<T>> {
    pub fn downcast_ref<R: Interface>(&self) -> Option<Address<R>> {
        let inbox = self.inbox.downcast_ref::<R>()?.clone();

        Some(Address {
            inbox,
            signal_inbox: self.signal_inbox.clone(),
            process_watcher: self.process_watcher.clone(),
            pid: self.pid.clone(),
        })
    }
}

impl<T: InboxKind> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("inbox", &self.inbox)
            .field("signal_inbox", &self.signal_inbox)
            .field("exit_watcher", &self.process_watcher)
            .finish()
    }
}

pub trait InboxKind {
    type Inbox: PolyBox + Clone + Debug + Unpin;
    type Set: TypeSet + 'static;
    type Receiver: Clone + Send + Sync + 'static;

    fn map_inbox_into_dyn_unchecked<R: TypeSet + 'static>(address: Self::Inbox) -> DynSender<R>;
    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync>;
}

// pub trait DynInboxKind: InboxKind<Inbox = DynInbox<Self>> + Sized + 'static {}
// impl<T: InboxKind<Inbox = DynInbox<T>> + Sized + 'static> DynInboxKind for T {}

pub struct Dyn<T: ?Sized>(T);

impl<T: TypeSet + 'static> InboxKind for Dyn<T> {
    type Inbox = DynSender<T>;
    type Set = T;
    type Receiver = Arc<dyn Any + Send + Sync>;

    fn map_inbox_into_dyn_unchecked<R: TypeSet + 'static>(inbox: Self::Inbox) -> DynSender<R> {
        inbox.into_dyn_unchecked()
    }

    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync> {
        receiver
    }
}

impl InboxKind for Set![] {
    type Inbox = DynSender<Set![]>;
    type Set = Set![];
    type Receiver = Arc<dyn Any + Send + Sync>;

    fn map_inbox_into_dyn_unchecked<R: TypeSet + 'static>(inbox: Self::Inbox) -> DynSender<R> {
        inbox.into_dyn_unchecked()
    }

    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync> {
        receiver
    }
}

impl<T: Interface> InboxKind for T {
    type Inbox = Sender<T>;
    type Set = T::Set;
    type Receiver = Receiver<T>;

    fn map_inbox_into_dyn_unchecked<R: TypeSet + 'static>(inbox: Self::Inbox) -> DynSender<R> {
        inbox.into_dyn_unchecked()
    }

    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync> {
        Arc::new(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Interface, HandlerInterface)]
    #[interface(crate = "crate")]
    pub enum MyInterface {
        A(Payload<u32>),
    }

    #[tokio::test]
    async fn test_address_downcast_ref() {
        let child = crate::spawn(
            Pid::rand(),
            |_: ActorState<MyInterface>| async move { Ok(()) },
        );
        let address = child.address().clone().into_dyn::<Set![]>();

        address
            .downcast_ref::<MyInterface>()
            .expect("Should downcast to MyInterface");
    }
}
