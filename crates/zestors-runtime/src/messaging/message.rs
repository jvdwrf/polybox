use crate::_prelude::*;
use std::convert::Infallible;

/// Defines how a message is sent, and what kind of reply it expects.
///
/// There are two kinds of messages:
/// - Fire-and-forget: The sender does not expect a reply, and the message is sent without any acknowledgment. The [`Receipt`] type for this kind of message is `()`.
/// - Request: The sender expects a reply, and the message is sent with a [`Receipt`] that can be used to receive the reply. The [`Receipt`] type for this kind of message is [`Rx<R>`], where `R` is the type of the reply.
pub trait Message: Send + 'static + Sized {
    /// The immediate output of the message after sending. (`()` or `Rx<R>`)
    type Receipt: Receipt<Self>;

    /// The output of the message after resolving the [`Receipt`]. (`()` or `R`)
    type Output: Send + 'static;

    type Completer: Send + 'static;
}

/// A trait for types that can be used as the Receipt of a [`Message`].
///
/// This trait is sealed and cannot be implemented outside of this crate.
/// It is implemented for [`Rx<T>`] and `()`, which are the Receipt types of
/// request and fire-and-forget messages, respectively.
pub trait Receipt<M: Message>: Send + Sized + sealed::Sealed {
    /// Receive the reply of the message.
    fn receive(self) -> impl Future<Output = Result<M::Output, RxError>> + Send;

    /// Same as [`Self::receive`], but blocks the current thread until the reply is received.
    fn receive_blocking(self) -> Result<M::Output, RxError> {
        futures::executor::block_on(self.receive())
    }

    /// Convert a message into its envelope and Receipt.
    fn into_envelope(msg: M) -> (Envelope<M>, Self);

    /// Convert a envelope back into the message.
    fn from_envelope(envelope: Envelope<M>) -> M;
}

impl<M> Receipt<M> for ()
where
    M: Message<Completer: Default, Output: Default>,
{
    async fn receive(self) -> Result<M::Output, RxError> {
        Ok(Default::default())
    }

    fn into_envelope(message: M) -> (Envelope<M>, Self) {
        (
            Envelope {
                msg: message,
                handle: Default::default(),
            },
            (),
        )
    }

    fn from_envelope(envelope: Envelope<M>) -> M {
        envelope.msg
    }
}

impl<M, O> Receipt<M> for Rx<O>
where
    O: Send + 'static,
    M: Message<Completer = Tx<O>, Output = O>,
{
    async fn receive(self) -> Result<O, RxError> {
        self.await
    }

    fn into_envelope(msg: M) -> (Envelope<M>, Self) {
        let (tx, rx) = new_request();
        (Envelope::new(msg, tx), rx)
    }

    fn from_envelope(envelope: Envelope<M>) -> M {
        envelope.msg
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Envelope<M: Message> {
    pub msg: M,
    pub handle: M::Completer,
}

impl<M: Message> Envelope<M> {
    pub fn new(msg: M, handle: M::Completer) -> Self {
        Self { msg, handle }
    }
}

/// A trait that extends [`Message`] with some helper methods.
pub trait MessageExt: Message {
    fn build_envelope(self) -> (Envelope<Self>, Self::Receipt)
    where
        Self: Sized,
    {
        <Self::Receipt as Receipt<Self>>::into_envelope(self)
    }

    fn destroy_envelope(envelope: Envelope<Self>) -> Self
    where
        Self: Sized,
    {
        <Self::Receipt as Receipt<Self>>::from_envelope(envelope)
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
                type Output = ();
                type Receipt = ();
                type Completer = ();
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
                type Output = ();
                type Receipt = ();
                type Completer = ();
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
                type Output = ();
                type Receipt = ();
                type Completer = ();
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
