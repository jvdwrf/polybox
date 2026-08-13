use crate::*;
use futures::future::BoxFuture;
use std::{
    any::{Any, TypeId},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::Arc,
};
use type_sets::SubsetOf;

/// A trait that allows for conversions to [`DynInbox`].
pub trait PolyBox: DynPolyBox {
    /// The set of message types that this inbox can accept.
    type Set: Members + 'static;
    type Dyn<T: Members + 'static>;

    /// Converts into a dynamic inbox without checking if the types are compatible.
    ///
    /// Avoid using this method unless you are sure that the types are compatible, as it can lead to runtime errors. Instead, consider using `into_dyn_checked` or `into_dyn_subset` for safer conversions.
    ///
    /// # Safety
    /// This method is not marked as unsafe, because violating the type system can
    /// only lead to runtime errors, not undefined behavior.
    fn into_dyn_unchecked<T: Members>(self) -> Self::Dyn<T>;
}

/// A trait that extends [`PolyBox`] with some helper methods.
pub trait PolyboxExt: PolyBox + Sized {
    /// Converts into a dynamic inbox with a subset of the original types.
    ///
    /// This conversion is type-safe, and entirely at compile-time.
    fn into_dyn<T: Members>(self) -> Self::Dyn<T>
    where
        T: SubsetOf<Self::Set>,
    {
        self.into_dyn_unchecked()
    }

    /// Converts into a dynamic inbox with the full set of original types.
    fn into_dyn_full(self) -> Self::Dyn<Self::Set> {
        self.into_dyn_unchecked()
    }

    /// Converts into a dynamic inbox, checking at runtime if the types are compatible.
    fn into_dyn_checked<T: Members>(self) -> Result<Self::Dyn<T>, Self> {
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

pub(crate) trait AnyDynPolyBox: Any + DynPolyBox {}
impl<T: Any + DynPolyBox> AnyDynPolyBox for T {}

/// A dynamic inbox that can accept messages of any type, as long as they are part of the specified set.
///
/// An inbox is typed as: `DynInbox<Set![Msg1, Msg2, ...]>`.
///
/// Conversions between inboxes:
/// - Into more specific subsets -> [`PolyboxExt::into_dyn_subset`].
/// - Into more general supersets -> [`PolyboxExt::into_dyn_checked`] or [`PolyBox::into_dyn_unchecked`].
pub struct DynInbox<T> {
    inbox: Arc<dyn AnyDynPolyBox>,
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

impl<T> Debug for DynInbox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynInbox")
            .field("inbox", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> DynInbox<T> {
    pub(crate) fn new_unchecked(inbox: Arc<dyn AnyDynPolyBox>) -> Self {
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

    pub fn downcast_ref<R: Interface>(&self) -> Option<&Inbox<R>> {
        let inbox = &*self.inbox as &dyn Any;
        inbox.downcast_ref::<Inbox<R>>()
    }
}

impl<T: Members + 'static> PolyBox for DynInbox<T> {
    type Set = T;
    type Dyn<R: Members + 'static> = DynInbox<R>;

    fn into_dyn_unchecked<R>(self) -> DynInbox<R> {
        DynInbox::new_unchecked(self.inbox)
    }
}

impl<M, R> Sends<M> for DynInbox<R>
where
    M: Message<Output: Send, Payload: Send>,
    R: Members + 'static + Contains<M>,
{
    async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
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
