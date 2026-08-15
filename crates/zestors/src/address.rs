use crate::_prelude::*;
use crate::signals::{Observable, SignalSender};
use polybox::{errors::SendError, type_sets::Set};
use std::{any::Any, fmt::Debug, sync::Arc};

pub struct Address<T: SenderKind = Set!()> {
    inbox: T::Sender,
    data: SharedProcessData,
}

impl<T: SenderKind, M: Message> Sends<M> for Address<T>
where
    T::Sender: Sends<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
        if !self.is_alive() {
            return Err(SendError(msg));
        }

        self.inbox.send(msg).await
    }
}

impl<T: SenderKind> Observable for Address<T> {
    async fn send_signal(&self, signal: SignalInterface) -> Result<(), SendError<SignalInterface>> {
        self.data.send_signal(signal).await
    }
}

impl<T: SenderKind> DynPolySender for Address<T> {
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

    fn members(&self) -> &'static [std::any::TypeId]
    where
        Self: 'static,
    {
        T::members()
    }
}

impl<T: SenderKind> PolySender for Address<T>
where
    <T::Sender as TypeSet>::Set: 'static,
{
    type DynVariant<R: DynSenderKind> = Address<R>;

    fn into_dyn_unchecked<R: DynSenderKind>(self) -> Self::DynVariant<R> {
        Address {
            inbox: T::map_inbox_into_dyn_unchecked(self.inbox),
            data: self.data,
        }
    }
}

impl<T: SenderKind> TypeSet for Address<T> {
    type Set = <T::Sender as TypeSet>::Set;

    fn members() -> &'static [std::any::TypeId]
    where
        Self: 'static,
    {
        <T::Sender as TypeSet>::members()
    }
}

impl<T: SenderKind> Clone for Address<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            data: self.data.clone(),
        }
    }
}

impl<T: SenderKind> Address<T> {
    pub(super) fn new(inbox: T::Sender, data: SharedProcessData) -> Self {
        Self { inbox, data }
    }

    pub fn into_dyn_unchecked_test(self) -> Address {
        todo!()
    }

    pub fn status_stream(&self) -> &StatusStream {
        self.data.status_stream()
    }

    pub fn is_alive(&self) -> bool {
        self.data.is_alive()
    }

    pub fn is_same_process<R: SenderKind>(&self, other: &Address<R>) -> bool {
        self.data.pid() == other.data.pid()
    }

    pub fn pid(&self) -> &Pid {
        self.data.pid()
    }

    pub async fn watch_exit(&mut self) {
        self.data.watch_exit().await
    }

    pub async fn watch_start(&mut self) {
        self.data.watch_start().await
    }
}

impl<T> Address<Set<T>>
where
    Set<T>: TypeSet + 'static,
{
    pub fn downcast_ref<R: Interface>(&self) -> Option<Address<R>> {
        let inbox = self.inbox.downcast_ref::<R>()?.clone();

        Some(Address {
            inbox,
            data: self.data.clone(),
        })
    }
}

impl<T: SenderKind> Debug for Address<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Address")
            .field("inbox", &self.inbox)
            .field("data", &self.data)
            .finish()
    }
}

pub trait SenderKind: 'static {
    type Sender: PolySender + Clone + Debug + Unpin;
    type Receiver: Clone + Send + Sync + 'static;

    fn map_inbox_into_dyn_unchecked<R: DynSenderKind>(address: Self::Sender) -> DynSender<R>;
    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync>;
    fn members() -> &'static [std::any::TypeId]
    where
        Self: 'static;
}

impl<E> SenderKind for Set<E>
where
    Set<E>: TypeSet + 'static,
{
    type Sender = DynSender<Set<E>>;
    type Receiver = Arc<dyn Any + Send + Sync>;

    fn map_inbox_into_dyn_unchecked<R: DynSenderKind>(sender: Self::Sender) -> DynSender<R> {
        sender.into_dyn_unchecked()
    }

    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync> {
        receiver
    }

    fn members() -> &'static [std::any::TypeId]
    where
        Self: 'static,
    {
        <Set<E> as TypeSet>::members()
    }
}

impl<I: Interface> SenderKind for I {
    type Sender = Sender<I>;
    type Receiver = Receiver<I>;

    fn map_inbox_into_dyn_unchecked<R: DynSenderKind>(sender: Self::Sender) -> DynSender<R> {
        sender.into_dyn_unchecked()
    }

    fn map_receiver_into_any(receiver: Self::Receiver) -> Arc<dyn Any + Send + Sync> {
        Arc::new(receiver)
    }

    fn members() -> &'static [std::any::TypeId]
    where
        Self: 'static,
    {
        <I::Set as TypeSet>::members()
    }
}

pub trait DynSenderKind:
    TypeSet
    + SenderKind<Sender = DynSender<Self>, Receiver = Arc<dyn Any + Send + Sync>>
    + Sized
    + 'static
{
}
impl<
    T: TypeSet
        + SenderKind<Sender = DynSender<T>, Receiver = Arc<dyn Any + Send + Sync>>
        + Sized
        + 'static,
> DynSenderKind for T
{
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
