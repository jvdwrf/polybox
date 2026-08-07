use crate::*;
use std::marker::PhantomData;

/// A marker type for request messages.
pub struct Request<T>(PhantomData<T>);

/// A marker type for fire-and-forget messages.
pub struct FireAndForget(());

/// A trait that must be implemented for all types that are sent as messages.
///
/// It defines the kind of the message, which can be either [`Request<T>`] or [`FireAndForget`].
pub trait Message: Send + 'static + Sized {
    /// The kind of the message, which can be either [`Request<T>`] or [`FireAndForget`].
    type Kind: MessageSpecifier<Self>;
}

/// A trait for types that can be used to specify the kind of a [`Message`].
///
/// This trait is sealed and cannot be implemented outside of this crate.
/// Use [`Request<T>`] or [`FireAndForget`] to specify the kind of a [`Message`].
pub trait MessageSpecifier<T>: sealed::Sealed {
    /// The output type of the message.
    ///
    /// This must implement [`MessageReply`], and is either [`Rx<T>`] for request
    /// messages, or `()` for fire-and-forget messages.
    type Output: MessageReply + Send;

    /// The actual payload of the message.
    ///
    /// This is `T` for fire-and-forget messages, and `(T, Tx<R>)` for requests.
    type Payload: Send + 'static;

    /// Convert a message into its payload and output.
    fn into_payload(msg: T) -> (Self::Payload, Self::Output);

    /// Convert a payload back into the message.
    fn from_payload(payload: Self::Payload) -> T;
}

impl<I: Send + 'static, R: Send + 'static> MessageSpecifier<I> for Request<R> {
    type Output = Rx<R>;
    type Payload = (I, Tx<R>);

    fn into_payload(msg: I) -> (Self::Payload, Self::Output) {
        let (tx, rx) = new_request();
        ((msg, tx), rx)
    }

    fn from_payload(payload: Self::Payload) -> I {
        let (msg, _tx) = payload;
        msg
    }
}

impl<I: Send + 'static> MessageSpecifier<I> for FireAndForget {
    type Output = ();
    type Payload = I;

    fn into_payload(msg: I) -> (Self::Payload, Self::Output) {
        (msg, ())
    }

    fn from_payload(payload: Self::Payload) -> I {
        payload
    }
}

/// A trait for types that can be used as the output of a [`Message`].
///
/// This trait is sealed and cannot be implemented outside of this crate.
/// It is implemented for [`Rx<T>`] and `()`, which are the output types of
/// request and fire-and-forget messages, respectively.
pub trait MessageReply: Sized + sealed::Sealed {
    /// The reply type of the message.
    type Reply;

    /// Receive the reply of the message.
    fn receive(self) -> impl Future<Output = Result<Self::Reply, RxError>> + Send;

    /// Same as [`Self::receive`], but blocks the current thread until the reply is received.
    fn receive_blocking(self) -> Result<Self::Reply, RxError> {
        futures::executor::block_on(self.receive())
    }
}

impl MessageReply for () {
    type Reply = ();

    async fn receive(self) -> Result<Self::Reply, RxError> {
        Ok(())
    }
}

impl<T> MessageReply for Rx<T>
where
    T: Send + 'static,
{
    type Reply = T;

    async fn receive(self) -> Result<Self::Reply, RxError> {
        self.await
    }
}

/// A helper type for the output of a [`Message`].
pub type Output<T> = <<T as Message>::Kind as MessageSpecifier<T>>::Output;

/// A helper type for the reply of a [`Message`].
pub type Reply<T> = <Output<T> as MessageReply>::Reply;

/// A helper type for the payload of a [`Message`].
pub type Payload<T> = <<T as Message>::Kind as MessageSpecifier<T>>::Payload;

/// A trait that extends [`Message`] with some helper methods.
pub trait MessageExt: Message {
    fn build_payload(self) -> (Payload<Self>, Output<Self>)
    where
        Self: Sized,
    {
        <Self::Kind as MessageSpecifier<Self>>::into_payload(self)
    }

    fn destroy_payload(payload: Payload<Self>) -> Self
    where
        Self: Sized,
    {
        <Self::Kind as MessageSpecifier<Self>>::from_payload(payload)
    }
}
impl<I> MessageExt for I where I: Message {}

pub(crate) mod sealed {
    pub trait Sealed {}

    impl<T> Sealed for super::Request<T> {}
    impl Sealed for super::FireAndForget {}

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
                type Kind = FireAndForget;
            }
        )*
    };
}
implement_message_for_base_types! {
    u8, u16, u32, u64, u128,
    i8, i16, i32, i64, i128,
    (),
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
                type Kind = FireAndForget;
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
                type Kind = FireAndForget;
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
