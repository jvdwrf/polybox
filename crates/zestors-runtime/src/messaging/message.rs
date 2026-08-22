use crate::_prelude::*;
use std::{convert::Infallible, fmt::Debug};

/// Defines how a message is sent and whether its processing produces an outcome.
///
/// There are two kinds of messages:
///
/// - **Fire-and-forget:** The sender does not expect an outcome. Its [`Receipt`]
///   and [`Outcome`] are both `()`.
/// - **Request:** The sender expects an outcome. Its [`Receipt`] is [`Rx<R>`],
///   which receives an outcome of type `R`.
pub trait Message: Send + 'static + Sized {
    /// The value produced by the receiver when the receipt is resolved.
    type Outcome: Send + 'static;

    /// A handle held by the sender to observe the message's outcome.
    type Receipt: Receipt<Self::Outcome>;
}

pub type Resolver<M> = <<M as Message>::Receipt as Receipt<<M as Message>::Outcome>>::Resolver;

/// A handle for observing the outcome of a [`Message`].
///
/// This trait is sealed and cannot be implemented outside of this crate.
/// It is implemented for [`Rx<T>`] and `()`, the receipt types for request
/// and fire-and-forget messages, respectively.
pub trait Receipt<O>: Send + Sized + sealed::Sealed {
    type Resolver: Debug + Send + 'static;

    /// Waits for the message's receipt to resolve.
    fn resolved(self) -> impl Future<Output = Result<O, RxError>> + Send;

    /// Waits for the message's receipt to resolve, blocking the current thread.
    fn resolved_blocking(self) -> Result<O, RxError> {
        futures::executor::block_on(self.resolved())
    }

    fn new() -> (Self, Self::Resolver);
}

impl<O: Default> Receipt<O> for () {
    type Resolver = ();

    async fn resolved(self) -> Result<O, RxError> {
        Ok(O::default())
    }

    fn new() -> (Self, Self::Resolver) {
        ((), ())
    }
}

impl<O: Send + 'static> Receipt<O> for Rx<O> {
    type Resolver = Tx<O>;

    async fn resolved(self) -> Result<O, RxError> {
        self.await
    }

    fn new() -> (Self, Self::Resolver) {
        let (tx, rx) = new_request();
        (rx, tx)
    }
}

#[derive(Debug)]
pub struct Envelope<M: Message> {
    pub msg: M,
    pub handle: Resolver<M>,
}

impl<M: Message> Envelope<M> {
    pub fn new(msg: M, handle: Resolver<M>) -> Self {
        Self { msg, handle }
    }
}

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
