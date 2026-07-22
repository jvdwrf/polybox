use crate::*;
use std::{any::TypeId, future::Future, sync::Arc};

mod dynamic_inbox;

pub trait PolyBox: DynPolyBox + Clone {
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

pub trait PolyboxExt: PolyBox {
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
            let (payload, output) = T::build_payload(msg);
            let payload = BoxedPayload::new::<T>(payload);

            match self._send_boxed_payload_checked(payload).await {
                Ok(()) => Ok(output),
                Err(SendCheckedError::Closed(payload)) => {
                    let payload = payload
                        .downcast::<T>()
                        .expect("Failed to convert payload back");

                    Err(SendCheckedError::Closed(T::destroy_payload(payload)))
                }
                Err(SendCheckedError::NotAccepted(payload)) => {
                    Err(SendCheckedError::NotAccepted(T::destroy_payload(
                        payload
                            .downcast::<T>()
                            .expect("Failed to convert payload back"),
                    )))
                }
            }
        }
    }

    fn send_checked_blocking<T: Message>(&self, msg: T) -> Result<Output<T>, SendCheckedError<T>> {
        let (payload, output) = T::build_payload(msg);
        let payload = BoxedPayload::new::<T>(payload);

        match self._send_boxed_payload_checked_blocking(payload) {
            Ok(()) => Ok(output),
            Err(SendCheckedError::Closed(payload)) => {
                let payload = payload
                    .downcast::<T>()
                    .expect("Failed to convert payload back");

                Err(SendCheckedError::Closed(T::destroy_payload(payload)))
            }
            Err(SendCheckedError::NotAccepted(payload)) => {
                Err(SendCheckedError::NotAccepted(T::destroy_payload(
                    payload
                        .downcast::<T>()
                        .expect("Failed to convert payload back"),
                )))
            }
        }
    }
}
impl<T: PolyBox> PolyboxExt for T {}

pub trait DynPolyBox: Send + Sync {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<BoxedPayload>>>;

    fn _send_boxed_payload_checked_blocking(
        &self,
        msg: BoxedPayload,
    ) -> Result<(), SendCheckedError<BoxedPayload>> {
        futures::executor::block_on(self._send_boxed_payload_checked(msg))
    }
}

pub use dynamic_inbox::*;
use futures::future::BoxFuture;
use type_sets::SubsetOf;
