use crate::_prelude::*;
use std::convert::Infallible;

/// Defines how a message is sent and whether its processing produces an outcome.
///
/// There are two kinds of messages:
///
/// - **Fire-and-forget:** The sender does not expect an outcome. Its [`Receipt`]
///   and [`Outcome`] are both `()`.
/// - **Request:** The sender expects an outcome. Its [`Receipt`] is [`Rx<R>`],
///   which receives an outcome of type `R`.
pub trait Message: Send + 'static + Sized {
    /// A handle held by the sender to observe the message's outcome.
    type Receipt: Receipt<Self>;

    /// A capability held by the receiver to produce the message's outcome.
    type Resolver: Send + 'static;

    /// The value produced by the receiver when the receipt is resolved.
    type Outcome: Send + 'static;
}

/// A handle for observing the outcome of a [`Message`].
///
/// This trait is sealed and cannot be implemented outside of this crate.
/// It is implemented for [`Rx<T>`] and `()`, the receipt types for request
/// and fire-and-forget messages, respectively.
pub trait Receipt<M: Message>: Send + Sized + sealed::Sealed {
    /// Waits for the message's receipt to resolve.
    fn resolve(self) -> impl Future<Output = Result<M::Outcome, RxError>> + Send;

    /// Waits for the message's receipt to resolve, blocking the current thread.
    fn resolve_blocking(self) -> Result<M::Outcome, RxError> {
        futures::executor::block_on(self.resolve())
    }

    /// Converts a message into its envelope and receipt.
    fn into_envelope(msg: M) -> (Envelope<M>, Self);

    /// Extracts the message from its envelope.
    fn from_envelope(envelope: Envelope<M>) -> M;
}

impl<M> Receipt<M> for ()
where
    M: Message<Resolver: Default, Outcome: Default>,
{
    async fn resolve(self) -> Result<M::Outcome, RxError> {
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
    M: Message<Resolver = Tx<O>, Outcome = O>,
{
    async fn resolve(self) -> Result<O, RxError> {
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
    pub handle: M::Resolver,
}

impl<M: Message> Envelope<M> {
    pub fn new(msg: M, handle: M::Resolver) -> Self {
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
                type Outcome = ();
                type Receipt = ();
                type Resolver = ();
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
                type Outcome = ();
                type Receipt = ();
                type Resolver = ();
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
                type Outcome = ();
                type Receipt = ();
                type Resolver = ();
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
