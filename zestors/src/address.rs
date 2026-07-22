use crate::*;
use std::sync::Arc;

pub struct Inbox<T: InboxSpecifier> {
    inner: T::Sender,
}

impl<T: InboxSpecifier> Clone for Inbox<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: InboxSpecifier> Inbox<T> {
    pub fn into_subset<R>(self) -> Inbox<R>
    where
        T: 'static,
        R: SubsetOf<T> + InboxSpecifier<Sender = DynInboxRef<R>>,
    {
        Inbox {
            inner: self.inner.into_any_unchecked(),
        }
    }

    pub fn into_dyn(self) -> Inbox<T::Set>
    where
        T: AsSet<Set: InboxSpecifier<Sender = DynInboxRef<T::Set>>>,
    {
        Inbox {
            inner: self.inner.into_any_unchecked(),
        }
    }

    pub(crate) fn new_for_testing() -> Self
    where
        T: std::fmt::Debug + InboxSpecifier<Sender = ActorRef<T>> + Send + 'static,
    {
        let (msg_sender, msg_receiver) = tokio::sync::mpsc::channel(1);

        let actor_ref = ActorRef::<T> { sender: msg_sender };

        let address = Inbox {
            inner: actor_ref.clone(),
        };

        // Spawn a task to handle messages and stop signals
        tokio::spawn(async move {
            let mut msg_receiver = msg_receiver;

            loop {
                tokio::select! {
                    Some(msg) = msg_receiver.recv() => {
                        // Handle the message (for testing, we can just print it)
                        println!("Received message: {:?}", msg);
                    }
                }
            }
        });

        address
    }
}

impl<T, R> Sends<T> for Inbox<R>
where
    T: Message,
    R: Accepts<T>,
{
    async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        self.inner.send(msg).await
    }
}

pub trait Accepts<T: Message>: InboxSpecifier<Sender: Sends<T> + Sync> {}
impl<T, R> Accepts<T> for R
where
    T: Message,
    R: InboxSpecifier<Sender: Sends<T> + Sync>,
{
}

pub trait InboxSpecifier {
    type Sender: InboxReference;
}

pub trait InboxReference: Clone {
    fn into_any_unchecked<R>(self) -> DynInboxRef<R>;
}

trait DynActorReference: Send + Sync {
    fn send_any_payload_checked(
        &self,
        msg: AnyPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<AnyPayload>>>;
}

mod dynamic {
    use super::*;
    use std::{marker::PhantomData, sync::Arc};
    use type_sets::Set;

    pub struct DynInboxRef<T> {
        inner: Arc<dyn DynActorReference>,
        _t: PhantomData<fn() -> T>,
    }

    impl<T> Clone for DynInboxRef<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                _t: PhantomData,
            }
        }
    }

    impl<T> DynInboxRef<T> {
        pub(super) fn new(inner: Arc<dyn DynActorReference>) -> Self {
            Self {
                inner,
                _t: PhantomData,
            }
        }
    }

    impl<T: ?Sized> InboxSpecifier for Set<T> {
        type Sender = DynInboxRef<Set<T>>;
    }

    impl<T> InboxReference for DynInboxRef<T> {
        fn into_any_unchecked<R>(self) -> DynInboxRef<R> {
            DynInboxRef::new(self.inner)
        }
    }

    impl<T, R> Sends<T> for DynInboxRef<R>
    where
        T: Message<Kind: MessageSpecifier<T, Output: Send, Payload: Send>>,
        R: Contains<T>,
    {
        async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
            let (payload, output) = T::into_payload(msg);
            let payload = AnyPayload::new::<T>(payload);

            match self.inner.send_any_payload_checked(payload).await {
                Ok(()) => Ok(output),
                Err(SendCheckedError::Closed(payload)) => {
                    let payload = payload
                        .downcast::<T>()
                        .expect("Failed to convert payload back");

                    Err(SendError(T::from_payload(payload)))
                }
                Err(SendCheckedError::NotAccepted(_payload)) => {
                    panic!(
                        "Payload was not accepted, this should not happen if the type system is used correctly"
                    )
                }
            }
        }
    }
}

mod static_ {
    use super::*;
    use type_sets::SubsetOf;

    pub struct ActorRef<T> {
        pub(super) sender: tokio::sync::mpsc::Sender<T>,
    }

    impl<T, R> Sends<T> for ActorRef<R>
    where
        T: Message<Kind: MessageSpecifier<T, Output: Send, Payload: Into<R>>>,
        R: TryInto<Payload<T>> + Send,
    {
        async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
            let (payload, output) = T::into_payload(msg);
            let payload = payload.into();

            match self.sender.send(payload).await {
                Ok(()) => Ok(output),
                Err(e) => Err(SendError(T::from_payload(
                    e.0.try_into()
                        .map_err(|_| ())
                        .expect("Failed to convert payload back"),
                ))),
            }
        }
    }

    impl<T> Clone for ActorRef<T> {
        fn clone(&self) -> Self {
            Self {
                sender: self.sender.clone(),
            }
        }
    }

    impl<T> InboxSpecifier for T
    where
        T: Interface,
    {
        type Sender = ActorRef<T>;
    }

    impl<T: Interface> InboxReference for ActorRef<T> {
        fn into_any_unchecked<R>(self) -> DynInboxRef<R> {
            DynInboxRef::new(Arc::new(self))
        }
    }

    impl<T: Interface> DynActorReference for ActorRef<T> {
        fn send_any_payload_checked(
            &self,
            msg: AnyPayload,
        ) -> BoxFuture<'_, Result<(), SendCheckedError<AnyPayload>>> {
            Box::pin(async move {
                let payload = T::try_from_any_payload(msg)
                    .map_err(|payload| SendCheckedError::NotAccepted(payload))?;

                self.send(payload).await.map_err(|SendError(payload)| {
                    SendCheckedError::Closed(T::into_any_payload(payload))
                })
            })
        }
    }

    impl<T> ActorRef<T> {
        pub fn into_any<R>(self) -> DynInboxRef<R>
        where
            T: Interface,
            R: SubsetOf<T>,
        {
            DynInboxRef::new(Arc::new(self))
        }
    }
}

pub use dynamic::*;
use futures::future::BoxFuture;
pub use static_::*;
use type_sets::SubsetOf;

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Interface, Debug)]
    #[zestors(crate = "crate")]
    pub enum MyInterface {
        A(Payload<u32>),
        B(Payload<u64>),
    }

    #[tokio::test]
    async fn creating_address() {
        let address = Inbox::<MyInterface>::new_for_testing();

        address.send(50u32).await.unwrap();
        address.send(50u64).await.unwrap();
        address.send(MyInterface::A(10)).await.unwrap();

        let address = address.clone().into_dyn();
        let address = address.into_subset::<<MyInterface as AsSet>::Set>();
        let address = address.into_subset::<Set![u64, u32]>();
        // let address = address.into_any::<Set![String]>();

        address.send(50u64).await.unwrap();
        address.send(50u32).await.unwrap();
        accepting(address.clone()).await;
        // address.send("hello").await.unwrap();
    }

    async fn accepting(a: Inbox<impl Accepts<u32>>) {
        a.send(50u32).await.unwrap();
        // a.into_subset::<Set![u32]>();
    }
}
