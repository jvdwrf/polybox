use crate::*;
use std::marker::PhantomData;

pub trait MessageSpecifier<I>: sealed::Sealed {
    type Output: MessageReply + Send;
    type Payload: Send + 'static;

    fn into_payload(msg: I) -> (Self::Payload, Self::Output);
    fn from_payload(payload: Self::Payload) -> I;
}

pub trait SimpleSpecifier<I> {
    type Payload;
}

impl<T> SimpleSpecifier<T> for FireAndForget {
    type Payload = T;
}

pub struct Request<T>(PhantomData<T>);
pub struct FireAndForget(());

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

pub trait MessageReply: Sized {
    type Reply;

    fn get(self) -> impl Future<Output = Result<Self::Reply, RxError>> + Send;

    fn get_blocking(self) -> Result<Self::Reply, RxError> {
        futures::executor::block_on(self.get())
    }
}

impl MessageReply for () {
    type Reply = ();

    async fn get(self) -> Result<Self::Reply, RxError> {
        Ok(())
    }
}

impl<T> MessageReply for Rx<T>
where
    T: Send + 'static,
{
    type Reply = T;

    async fn get(self) -> Result<Self::Reply, RxError> {
        self.await
    }
}

pub(crate) mod sealed {
    pub trait Sealed {}
    impl<T> Sealed for super::Request<T> {}
    impl Sealed for super::FireAndForget {}
}

/// A trait for types that can be invoked, either as a request (with a response),
/// or as a fire-and-forget cast.
pub trait Message: Send + 'static + Sized {
    type Kind: MessageSpecifier<Self>;
}

/// The output type of an [`Message`].
pub type Output<I> = <<I as Message>::Kind as MessageSpecifier<I>>::Output;

/// The request output type of an [`Message`].
pub type Reply<I> = <Output<I> as MessageReply>::Reply;

/// The payload type of an [`Message`].
pub type Payload<I> = <<I as Message>::Kind as MessageSpecifier<I>>::Payload;

/// A trait for types that can be invoked as a fire-and-forget cast.
///
/// This trait is implemented for all types that implement [`Message`] with a [`FireAndForget`] kind.
pub trait Cast: Message<Kind = FireAndForget> {}
impl<I> Cast for I where I: Message<Kind = FireAndForget> {}

/// A trait for types that can be invoked as a request (with a response).
///
/// This trait is implemented for all types that implement [`Message`] with a [`Request<T>`] kind.
pub trait Call<T>: Message<Kind = Request<T>> {}
impl<I, T> Call<T> for I where I: Message<Kind = Request<T>> {}

pub trait MessageExt: Message {
    fn into_payload(self) -> (Payload<Self>, Output<Self>)
    where
        Self: Sized,
    {
        <Self::Kind as MessageSpecifier<Self>>::into_payload(self)
    }

    fn from_payload(payload: Payload<Self>) -> Self
    where
        Self: Sized,
    {
        <Self::Kind as MessageSpecifier<Self>>::from_payload(payload)
    }
}

impl<I> MessageExt for I where I: Message {}

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
