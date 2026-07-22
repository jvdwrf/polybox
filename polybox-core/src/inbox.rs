use crate::*;
use futures::future::BoxFuture;
use std::{any::TypeId, future::Future, marker::PhantomData, sync::Arc};
use type_sets::SubsetOf;

/// A trait that allows for conversions to [`DynInbox`].
pub trait PolyBox: DynPolyBox + Clone {
    /// The set of message types that this inbox can accept.
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

/// A trait that extends [`PolyBox`] with some helper methods.
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

    /// Checks if the inbox accepts a message of the given type.
    #[must_use]
    fn accepts_msg(&self, id: TypeId) -> bool {
        <Self::Set as Members>::members().contains(&id)
    }

    /// Checks if the inbox accepts messages of the given types.
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

    /// Same as [`Self::send_checked`], but blocks the current thread until the message is sent.
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

/// Object-safe sub-trait of [`PolyBox`], allowing for dynamic dispatch.
pub trait DynPolyBox: Send + Sync {
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
}

/// A dynamic inbox that can accept messages of any type, as long as they are part of the specified set.
///
/// An inbox is typed as: `DynInbox<Set![Msg1, Msg2, ...]>`.
///
/// Conversions between inboxes:
/// - Into more specific subsets -> [`PolyboxExt::into_dyn_subset`].
/// - Into more general supersets -> [`PolyboxExt::into_dyn_checked`] or [`PolyBox::into_dyn_unchecked`].
pub struct DynInbox<T> {
    inbox: Arc<dyn DynPolyBox>,
    _t: PhantomData<fn() -> T>,
}

impl<T> Clone for DynInbox<T> {
    fn clone(&self) -> Self {
        Self {
            inbox: self.inbox.clone(),
            _t: PhantomData,
        }
    }
}

impl<T> DynInbox<T> {
    pub fn new_unchecked(inbox: Arc<dyn DynPolyBox>) -> Self {
        Self {
            inbox,
            _t: PhantomData,
        }
    }

    pub fn new<R>(inbox: R) -> Self
    where
        R: DynPolyBox + PolyBox + 'static,
        T: SubsetOf<R::Set>,
    {
        Self {
            inbox: Arc::new(inbox),
            _t: PhantomData,
        }
    }
}

impl<T: Members> PolyBox for DynInbox<T> {
    type Set = T;

    fn into_dyn_unchecked<R>(self) -> DynInbox<R> {
        DynInbox::new_unchecked(self.inbox)
    }
}

impl<T, R> Sends<T> for DynInbox<R>
where
    T: Message<Kind: MessageSpecifier<T, Output: Send, Payload: Send>>,
    R: Members + Contains<T>,
{
    async fn send(&self, msg: T) -> Result<Output<T>, SendError<T>> {
        self.send_checked(msg).await.map_err(|e| match e {
            SendCheckedError::Closed(msg) => SendError(msg),
            SendCheckedError::NotAccepted(_msg) => {
                panic!(
                    "Payload was not accepted, this should not happen if the type system is used correctly"
                )
            }
        })
    }
}

impl<T> DynPolyBox for DynInbox<T> {
    fn _send_boxed_payload_checked(
        &self,
        msg: BoxedPayload,
    ) -> BoxFuture<'_, Result<(), SendCheckedError<BoxedPayload>>> {
        self.inbox._send_boxed_payload_checked(msg)
    }
}
