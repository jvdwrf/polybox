use crate::*;
use std::convert::Infallible;

/// A trait that must be implemented for all types that are sent as messages.
///
/// It defines the kind of the message, which can be either [`Request<T>`] or [`FireAndForget`].
pub trait Message: Send + 'static + Sized {
    type Output: MessageOutput<Self, Payload = Self::Payload, Reply = Self::Reply> + Send;
    type Reply: Send + 'static;
    type Payload: Send + 'static;
}

/// A trait for types that can be used as the output of a [`Message`].
///
/// This trait is sealed and cannot be implemented outside of this crate.
/// It is implemented for [`Rx<T>`] and `()`, which are the output types of
/// request and fire-and-forget messages, respectively.
pub trait MessageOutput<M>: Sized + sealed::Sealed {
    /// The reply type of the message.
    type Reply;
    type Payload;

    /// Receive the reply of the message.
    fn receive(self) -> impl Future<Output = Result<Self::Reply, RxError>> + Send;

    /// Same as [`Self::receive`], but blocks the current thread until the reply is received.
    fn receive_blocking(self) -> Result<Self::Reply, RxError> {
        futures::executor::block_on(self.receive())
    }

    /// Convert a message into its payload and output.
    fn into_payload(msg: M) -> (Self::Payload, Self);

    /// Convert a payload back into the message.
    fn from_payload(payload: Self::Payload) -> M;
}

impl<M> MessageOutput<M> for () {
    type Reply = ();
    type Payload = M;

    async fn receive(self) -> Result<Self::Reply, RxError> {
        Ok(())
    }

    fn into_payload(msg: M) -> (M, Self) {
        (msg, ())
    }

    fn from_payload(payload: M) -> M {
        payload
    }
}

impl<M, R> MessageOutput<M> for Rx<R>
where
    M: Send + 'static,
    R: Send + 'static,
{
    type Reply = R;
    type Payload = (M, Tx<R>);

    async fn receive(self) -> Result<Self::Reply, RxError> {
        self.await
    }

    fn into_payload(msg: M) -> ((M, Tx<R>), Self) {
        let (tx, rx) = new_request();
        ((msg, tx), rx)
    }

    fn from_payload(payload: (M, Tx<R>)) -> M {
        let (msg, _tx) = payload;
        msg
    }
}

/// A helper type for the payload of a [`Message`].
pub type Payload<T> = <T as Message>::Payload;

/// A trait that extends [`Message`] with some helper methods.
pub trait MessageExt: Message {
    fn build_payload(self) -> (Self::Payload, Self::Output)
    where
        Self: Sized,
    {
        <Self::Output as MessageOutput<Self>>::into_payload(self)
    }

    fn destroy_payload(payload: Self::Payload) -> Self
    where
        Self: Sized,
    {
        <Self::Output as MessageOutput<Self>>::from_payload(payload)
    }
}
impl<I> MessageExt for I where I: Message {}

pub(crate) mod sealed {
    pub trait Sealed {}

    impl<T> Sealed for super::Rx<T> where T: Send + 'static {}
    impl Sealed for () {}
}

//------------------------------------------------------------------------------------------------
//  Message: Default implementations
//------------------------------------------------------------------------------------------------

macro_rules! implement_message_for_base_types {
    ($(
        $ty:ty
    ),*) => {
        $(
            impl Message for $ty {
                type Reply = ();
                type Output = ();
                type Payload = Self;
            }
        )*
    };
}
implement_message_for_base_types! {
    u8, u16, u32, u64, u128,
    i8, i16, i32, i64, i128,
    (), Infallible,
    String, &'static str
}

macro_rules! implement_message_for_wrappers {
    ($(
        $wrapper:ty
        $(where $_:ty: $where:ident)*
    ,)*) => {
        $(
            impl<M> Message for $wrapper
                where M: Send + 'static + $($where +)*
            {
                type Reply = ();
                type Output = ();
                type Payload = Self;
            }
        )*
    };
}
implement_message_for_wrappers!(
    Box<M>,
    std::sync::Arc<M> where M: Sync,
    Vec<M>,
    Box<[M]>,
);

macro_rules! implement_message_kind_and_message_for_tuples {
    ($(
        ($($id:ident: $na:ident + $na2:ident),*),
    )*) => {
        $(
            impl<$($id),*> Message for ($($id,)*)
            where
                $($id: Message + Send + 'static,)*
            {
                type Reply = ();
                type Output = ();
                type Payload = Self;
            }
        )*
    };
}
implement_message_kind_and_message_for_tuples!(
    (M1: m1 + m_1),
    (M1: m1 + m_1, M2: m2 + m_2),
    (M1: m1 + m_1, M2: m2 + m_2, M3: m3 + m_3),
    (M1: m1 + m_1, M2: m2 + m_2, M3: m3 + m_3, M4: m4 + m_4),
    (
        M1: m1 + m_1,
        M2: m2 + m_2,
        M3: m3 + m_3,
        M4: m4 + m_4,
        M5: m5 + m_5
    ),
    (
        M1: m1 + m_1,
        M2: m2 + m_2,
        M3: m3 + m_3,
        M4: m4 + m_4,
        M5: m5 + m_5,
        M6: m6 + m_6
    ),
    (
        M1: m1 + m_1,
        M2: m2 + m_2,
        M3: m3 + m_3,
        M4: m4 + m_4,
        M5: m5 + m_5,
        M6: m6 + m_6,
        M7: m7 + m_7
    ),
    (
        M1: m1 + m_1,
        M2: m2 + m_2,
        M3: m3 + m_3,
        M4: m4 + m_4,
        M5: m5 + m_5,
        M6: m6 + m_6,
        M7: m7 + m_7,
        M8: m8 + m_8
    ),
    (
        M1: m1 + m_1,
        M2: m2 + m_2,
        M3: m3 + m_3,
        M4: m4 + m_4,
        M5: m5 + m_5,
        M6: m6 + m_6,
        M7: m7 + m_7,
        M8: m8 + m_8,
        M9: m9 + m_9
    ),
    (
        M1: m1 + m_1,
        M2: m2 + m_2,
        M3: m3 + m_3,
        M4: m4 + m_4,
        M5: m5 + m_5,
        M6: m6 + m_6,
        M7: m7 + m_7,
        M8: m8 + m_8,
        M9: m9 + m_9,
        M10: m10 + m_10
    ),
);
