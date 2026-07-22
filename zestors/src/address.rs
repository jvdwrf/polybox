use crate::*;
use std::{any::TypeId, future::Future, sync::Arc};

mod dynamic_inbox;
mod tokio_inbox;

pub trait Inbox: SendsDynPayload + Clone {
    type Set: Members;

    /// Converts into a dynamic inbox without checking if the types are compatible.
    ///
    /// Avoid using this method unless you are sure that the types are compatible, as it can lead to runtime errors. Instead, consider using `into_dyn_checked` or `into_dyn_subset` for safer conversions.
    ///
    /// # Safety
    /// This method is not marked as unsafe, because violating the type system can
    /// only lead to runtime errors, not undefined behavior.
    fn into_dyn_unchecked<T>(self) -> DynInbox<T>;
}

pub trait InboxExt: Inbox {
    /// Converts into a dynamic inbox with a subset of the original types.
    ///
    /// This conversion is type-safe, and entirely at compile-time.
    fn into_dyn_subset<T>(self) -> DynInbox<T>
    where
        T: SubsetOf<Self::Set>,
    {
        self.into_dyn_unchecked()
    }

    /// Converts into a dynamic inbox with the full set of original types.
    fn into_dyn(self) -> DynInbox<Self::Set> {
        self.into_dyn_unchecked()
    }

    /// Converts into a dynamic inbox, checking at runtime if the types are compatible.
    fn into_dyn_checked<T: Members>(self) -> Result<DynInbox<T>, Self> {
        if self.accepts_msgs(&T::members()) {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }

    #[must_use]
    fn accepts_msg(&self, id: TypeId) -> bool {
        <Self::Set as Members>::members().contains(&id)
    }

    #[must_use]
    fn accepts_msgs(&self, ids: &[TypeId]) -> bool {
        ids.iter()
            .all(|id| <Self::Set as Members>::members().contains(id))
    }

    /// Send any message, checking at runtime if the message is accepted or not.
    fn send_checked<T: Message>(
        &self,
        msg: T,
    ) -> impl Future<Output = Result<Output<T>, SendCheckedError<T>>> + Send {
        async {
            let (payload, output) = T::into_payload(msg);
            let payload = AnyPayload::new::<T>(payload);

            match self._send_any_payload_checked(payload).await {
                Ok(()) => Ok(output),
                Err(SendCheckedError::Closed(payload)) => {
                    let payload = payload
                        .downcast::<T>()
                        .expect("Failed to convert payload back");

                    Err(SendCheckedError::Closed(T::from_payload(payload)))
                }
                Err(SendCheckedError::NotAccepted(payload)) => {
                    Err(SendCheckedError::NotAccepted(T::from_payload(
                        payload
                            .downcast::<T>()
                            .expect("Failed to convert payload back"),
                    )))
                }
            }
        }
    }

    fn send_checked_blocking<T: Message>(&self, msg: T) -> Result<Output<T>, SendCheckedError<T>> {
        let (payload, output) = T::into_payload(msg);
        let payload = AnyPayload::new::<T>(payload);

        match self._send_any_payload_checked_blocking(payload) {
            Ok(()) => Ok(output),
            Err(SendCheckedError::Closed(payload)) => {
                let payload = payload
                    .downcast::<T>()
                    .expect("Failed to convert payload back");

                Err(SendCheckedError::Closed(T::from_payload(payload)))
            }
            Err(SendCheckedError::NotAccepted(payload)) => {
                Err(SendCheckedError::NotAccepted(T::from_payload(
                    payload
                        .downcast::<T>()
                        .expect("Failed to convert payload back"),
                )))
            }
        }
    }
}
impl<T: Inbox> InboxExt for T {}

pub trait SendsDynPayload: Send + Sync {
    fn _send_any_payload_checked(
        &self,
        msg: AnyPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<AnyPayload>>>;

    fn _send_any_payload_checked_blocking(
        &self,
        msg: AnyPayload,
    ) -> Result<(), SendCheckedError<AnyPayload>> {
        futures::executor::block_on(self._send_any_payload_checked(msg))
    }
}

pub use dynamic_inbox::*;
use futures::future::BoxFuture;
pub use tokio_inbox::*;
use type_sets::SubsetOf;

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Message, Debug)]
    #[zestors(crate = "crate")]
    #[msg(request = i32)]
    pub struct MyMessage;

    #[derive(Interface, Debug)]
    #[zestors(crate = "crate")]
    pub enum MyInterface {
        A(Payload<u32>),
        B(Payload<u64>),
        C(Payload<MyMessage>),
    }

    #[tokio::test]
    async fn creating_address() {
        let (inbox, mut receiver) = TokioInbox::<MyInterface>::new(1000);

        tokio::task::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                match msg {
                    MyInterface::A(payload) => {
                        println!("Received A: {}", payload);
                    }
                    MyInterface::B(payload) => {
                        println!("Received B: {}", payload);
                    }
                    MyInterface::C((payload, tx)) => {
                        println!("Received C: {:?}", payload);
                        tx.send(42).unwrap();
                    }
                }
            }
        });

        inbox.send(50u32).await.unwrap();
        inbox.send(50u64).await.unwrap();
        let _: i32 = inbox.send(MyMessage).await.unwrap().await.unwrap();
        let _: i32 = inbox.request(MyMessage).await.unwrap();
        inbox.send(MyInterface::A(10)).await.unwrap();

        inbox.send_checked("hello").await.unwrap_err();
        inbox.send_checked(30u32).await.unwrap();

        let address = inbox.clone().into_dyn();
        let address = address.into_dyn_subset::<<MyInterface as AsSet>::Set>();
        let address = address.into_dyn_subset::<Set![u64, u32]>();
        // let address = address.into_any::<Set![String]>();

        address.send(50u64).await.unwrap();
        address.send(50u32).await.unwrap();

        address.send_checked("hello").await.unwrap_err();
        address.send_checked(30u32).await.unwrap();

        assert!(address.accepts_msg(TypeId::of::<u64>()));
        accepting(address.clone()).await;
        // address.send("hello").await.unwrap();
    }

    async fn accepting(a: impl Sends<u32>) {
        a.send(50u32).await.unwrap();
        // a.into_subset::<Set![u32]>();
    }
}
