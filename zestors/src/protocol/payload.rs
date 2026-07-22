use crate::*;
use std::any::Any;

/// Holds a [`MessageSpecifier::Payload`]
#[derive(Debug)]
pub struct AnyPayload(Box<dyn Any + Send>);

impl AnyPayload {
    pub fn new<T>(payload: Payload<T>) -> Self
    where
        T: Message,
        Payload<T>: Send + 'static,
    {
        Self(Box::new(payload))
    }

    pub fn downcast<T>(self) -> Result<Payload<T>, Self>
    where
        T: Message,
        Payload<T>: 'static,
    {
        match self.0.downcast() {
            Ok(cast) => Ok(*cast),
            Err(boxed) => Err(Self(boxed)),
        }
    }

    pub fn try_into_interface<T>(self) -> Result<T, Self>
    where
        T: Interface,
    {
        T::try_from_any_payload(self)
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
