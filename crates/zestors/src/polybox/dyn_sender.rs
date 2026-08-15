// use std::{
//     any::{Any, TypeId},
//     fmt::Debug,
//     marker::PhantomData,
//     sync::Arc,
// };

// use futures::future::BoxFuture;

// use crate::_prelude::*;
// use crate::polybox::*;

// // A dynamic inbox that can accept messages of any type, as long as they are part of the specified set.
// ///
// /// An inbox is typed as: `DynInbox<Set![Msg1, Msg2, ...]>`.
// ///
// /// Conversions between inboxes:
// /// - Into more specific subsets -> [`PolyboxExt::into_dyn_subset`].
// /// - Into more general supersets -> [`PolyboxExt::into_dyn_checked`] or [`PolyBox::into_dyn_unchecked`].
// pub struct DynSender<T> {
//     inbox: Arc<dyn DynPolySender>,
//     _t: PhantomData<fn() -> T>,
// }

// impl<T> Clone for DynSender<T> {
//     fn clone(&self) -> Self {
//         Self {
//             inbox: self.inbox.clone(),
//             _t: PhantomData,
//         }
//     }
// }

// impl<T> Debug for DynSender<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("DynSender")
//             .field("inbox", &std::any::type_name::<T>())
//             .finish()
//     }
// }

// impl<T> DynSender<T> {
//     pub(crate) fn new_unchecked(inbox: Arc<dyn DynPolySender>) -> Self {
//         Self {
//             inbox,
//             _t: PhantomData,
//         }
//     }

//     pub fn new<R>(inbox: R) -> Self
//     where
//         R: DynPolySender + PolySender + 'static,
//         T: SubsetOf<R::Set>,
//     {
//         Self {
//             inbox: Arc::new(inbox),
//             _t: PhantomData,
//         }
//     }

//     pub fn downcast_ref<R: Interface>(&self) -> Option<&Sender<R>> {
//         let inbox = &*self.inbox as &dyn Any;
//         inbox.downcast_ref::<Sender<R>>()
//     }
// }

// impl<T: DynSenderKind> PolySender for DynSender<T> {
//     type DynVariant<R: DynSenderKind> = DynSender<R>;

//     fn into_dyn_unchecked<R: DynSenderKind>(self) -> DynSender<R> {
//         DynSender::new_unchecked(self.inbox)
//     }
// }

// impl<T: TypeSet + 'static> TypeSet for DynSender<T> {
//     type Set = T;

//     fn members() -> &'static [std::any::TypeId]
//     where
//         Self: 'static,
//     {
//         <T as TypeSet>::members()
//     }
// }

// impl<T: TypeSet + 'static> DynPolySender for DynSender<T> {
//     fn _send_boxed_payload_checked(
//         &self,
//         msg: BoxedPayload,
//     ) -> BoxFuture<'_, Result<(), SendCheckedError<BoxedPayload>>> {
//         self.inbox._send_boxed_payload_checked(msg)
//     }

//     fn members(&self) -> &'static [TypeId]
//     where
//         Self: 'static,
//     {
//         <T as TypeSet>::members()
//     }
// }

// impl<M, T> Sends<M> for DynSender<T>
// where
//     M: Message<Output: Send, Payload: Send>,
//     T: DynSenderKind + Contains<M>,
// {
//     async fn send(&self, msg: M) -> Result<M::Output, SendError<M>> {
//         debug_assert!(
//             self.is_valid(),
//             "DynSender has incorrect type.
//     - Expected {:?}
//     - to be a superset of {:?} - ({})",
//             self.members(),
//             <T as TypeSet>::members(),
//             std::any::type_name::<T>(),
//         );

//         self.send_checked(msg).await.map_err(|e| match e {
//             SendCheckedError::Closed(msg) => SendError(msg),
//             SendCheckedError::NotAccepted(_msg) => {
//                 panic!(
//                     "Payload was not accepted, this should not happen if the type system is used correctly"
//                 )
//             }
//         })
//     }
// }
