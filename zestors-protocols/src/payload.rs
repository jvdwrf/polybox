use crate::{Payload, invocation::Invocation};
use std::any::Any;

/// Holds a [`InvocationSpecifier::Payload`]
#[derive(Debug)]
pub struct AnyPayload(Box<dyn Any + Send>);

impl AnyPayload {
    pub fn new<I>(payload: Payload<I>) -> Self
    where
        I: Invocation,
        Payload<I>: Send + 'static,
    {
        Self(Box::new(payload))
    }

    pub fn downcast<I>(self) -> Result<Payload<I>, Self>
    where
        I: Invocation,
        Payload<I>: 'static,
    {
        match self.0.downcast() {
            Ok(cast) => Ok(*cast),
            Err(boxed) => Err(Self(boxed)),
        }
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn boxed_msg() {
//         struct Msg1;
//         struct Msg2;

//         impl Message for Msg1 {
//             type Payload = Self;
//             type Returned = ();
//             fn into_payload(self) -> (Self::Payload, Self::Returned) {
//                 (self, ())
//             }
//             fn from_payload(sent: Self::Payload, _returned: Self::Returned) -> Self {
//                 sent
//             }
//         }

//         impl Message for Msg2 {
//             type Payload = Self;
//             type Returned = ();
//             fn into_payload(self) -> (Self::Payload, Self::Returned) {
//                 (self, ())
//             }
//             fn from_payload(sent: Self::Payload, _returned: Self::Returned) -> Self {
//                 sent
//             }
//         }

//         let boxed = AnyPayload::new::<Msg1>(Msg1);
//         assert!(boxed.downcast::<Msg1>().is_ok());

//         let boxed = AnyPayload::new::<Msg1>(Msg1);
//         assert!(boxed.downcast::<Msg2>().is_err());
//     }
// }
