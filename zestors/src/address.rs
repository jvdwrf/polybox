use crate::*;
use std::sync::Arc;

pub trait Inbox: Clone {
    type Set;

    fn into_dyn_unchecked<T>(self) -> DynInbox<T>;
}

pub trait InboxExt: Inbox {
    fn into_dyn_subset<T>(self) -> DynInbox<T>
    where
        T: SubsetOf<Self::Set>,
    {
        self.into_dyn_unchecked()
    }

    fn into_dyn(self) -> DynInbox<Self::Set> {
        self.into_dyn_unchecked()
    }
}
impl<T: Inbox> InboxExt for T {}

pub trait SendsPayload: Send + Sync {
    fn _send_any_payload_checked(
        &self,
        msg: AnyPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<AnyPayload>>>;
}

mod dynamic;

mod tokio_inbox;

pub use dynamic::*;
use futures::future::BoxFuture;
pub use tokio_inbox::*;
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
        let (inbox, mut receiver) = TokioInbox::<MyInterface>::new(10);

        inbox.send(50u32).await.unwrap();
        inbox.send(50u64).await.unwrap();
        inbox.send(MyInterface::A(10)).await.unwrap();

        let address = inbox.clone().into_dyn();
        let address = address.into_dyn_subset::<<MyInterface as AsSet>::Set>();
        let address = address.into_dyn_subset::<Set![u64, u32]>();
        // let address = address.into_any::<Set![String]>();

        address.send(50u64).await.unwrap();
        address.send(50u32).await.unwrap();
        accepting(address.clone()).await;
        // address.send("hello").await.unwrap();
    }

    async fn accepting(a: impl Sends<u32>) {
        a.send(50u32).await.unwrap();
        // a.into_subset::<Set![u32]>();
    }
}
