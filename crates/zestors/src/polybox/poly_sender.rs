use crate::*;
use futures::future::BoxFuture;
use std::{
    any::{Any, TypeId},
    future::Future,
};
use type_sets::SubsetOf;

/// A trait that allows for conversions to [`DynInbox`].
pub trait PolySender: TypeSet<Set: DynSenderKind> + DynPolySender + Sized {
    type DynVariant<T: DynSenderKind>;

    /// Converts into a dynamic inbox without checking if the types are compatible.
    ///
    /// Avoid using this method unless you are sure that the types are compatible, as it can lead to runtime errors. Instead, consider using `into_dyn_checked` or `into_dyn_subset` for safer conversions.
    ///
    /// # Safety
    /// This method is not marked as unsafe, because violating the type system can
    /// only lead to runtime errors, not undefined behavior.
    fn into_dyn_unchecked<T: DynSenderKind>(self) -> Self::DynVariant<T>;
}

/// Object-safe sub-trait of [`PolyBox`], allowing for dynamic dispatch.
pub trait DynPolySender: Any + Send + Sync {
    /// Send a boxed payload.
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<BoxedPayload>>>;

    /// Same as [`Self::_send_boxed_payload_checked`], but blocks the current thread until the message is sent.
    fn _send_boxed_payload_checked_blocking(
        &self,
        msg: BoxedPayload,
    ) -> Result<(), SendCheckedError<BoxedPayload>> {
        futures::executor::block_on(self._send_boxed_payload_checked(msg))
    }

    /// Returns the type IDs of the message types that this inbox can accept.
    fn members(&self) -> &'static [TypeId]
    where
        Self: 'static;
}

/// A trait that extends [`PolyBox`] with some helper methods.
pub trait PolySenderExt: PolySender + Sized {
    /// Send any message, checking at runtime if the message is accepted or not.
    fn send_checked<M: Message>(
        &self,
        msg: M,
    ) -> impl Future<Output = Result<M::Output, SendCheckedError<M>>> + Send {
        async {
            let (payload, output) = M::build_payload(msg);
            let payload = BoxedPayload::new::<M>(payload);

            match self._send_boxed_payload_checked(payload).await {
                Ok(()) => Ok(output),
                Err(SendCheckedError::Closed(payload)) => {
                    let payload = payload
                        .downcast::<M>()
                        .expect("Failed to convert payload back");

                    Err(SendCheckedError::Closed(M::destroy_payload(payload)))
                }
                Err(SendCheckedError::NotAccepted(payload)) => {
                    Err(SendCheckedError::NotAccepted(M::destroy_payload(
                        payload
                            .downcast::<M>()
                            .expect("Failed to convert payload back"),
                    )))
                }
            }
        }
    }

    /// Same as [`Self::send_checked`], but blocks the current thread until the message is sent.
    fn send_checked_blocking<M: Message>(&self, msg: M) -> Result<M::Output, SendCheckedError<M>> {
        let (payload, output) = M::build_payload(msg);
        let payload = BoxedPayload::new::<M>(payload);

        match self._send_boxed_payload_checked_blocking(payload) {
            Ok(()) => Ok(output),
            Err(SendCheckedError::Closed(payload)) => {
                let payload = payload
                    .downcast::<M>()
                    .expect("Failed to convert payload back");

                Err(SendCheckedError::Closed(M::destroy_payload(payload)))
            }
            Err(SendCheckedError::NotAccepted(payload)) => {
                Err(SendCheckedError::NotAccepted(M::destroy_payload(
                    payload
                        .downcast::<M>()
                        .expect("Failed to convert payload back"),
                )))
            }
        }
    }

    /// Converts into a dynamic inbox with a subset of the original types.
    ///
    /// This conversion is type-safe, and entirely at compile-time.
    fn into_dyn<T: DynSenderKind>(self) -> Self::DynVariant<T>
    where
        T: SubsetOf<Self::Set>,
    {
        self.into_dyn_unchecked()
    }

    /// Converts into a dynamic inbox with the full set of original types.
    fn into_dyn_full(self) -> Self::DynVariant<Self::Set> {
        self.into_dyn_unchecked()
    }

    /// Converts into a dynamic inbox, checking at runtime if the types are compatible.
    fn into_dyn_checked<T: DynSenderKind>(self) -> Result<Self::DynVariant<T>, Self> {
        if self.accepts_msgs(&self.members()) {
            Ok(self.into_dyn_unchecked())
        } else {
            Err(self)
        }
    }

    /// Checks if the inbox accepts a message of the given type.
    #[must_use]
    fn accepts_msg(&self, id: TypeId) -> bool {
        self.members().contains(&id)
    }

    /// Checks if the inbox accepts messages of the given types.
    #[must_use]
    fn accepts_msgs(&self, ids: &[TypeId]) -> bool {
        ids.iter().all(|id| self.members().contains(id))
    }

    fn accepts_current_set<T: DynSenderKind>(&self) -> bool {
        self.accepts_msgs(<T as TypeSet>::members())
    }
}
impl<T: PolySender> PolySenderExt for T {}
