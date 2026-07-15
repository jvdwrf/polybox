use crate::*;

#[derive(Invocation)]
#[invoke(request = String)]
#[zestors(crate = "crate")]
struct MyMessage;

// #[derive(Interface)]
// #[zestors(crate = "crate")]
enum MyActorProtocol {
    A(Payload<u32>),
    B(Payload<u64>),
    C(Payload<MyMessage>),
    D(Payload<u32>),
}

// Recursive expansion of Interface macro
// =======================================

impl crate::Interface for MyActorProtocol {
    fn try_from_any_payload(payload: crate::AnyPayload) -> Result<Self, crate::AnyPayload> {
        let payload = match payload.downcast::<u32>() {
            Ok(payload) => return Ok(Self::A(payload)),
            Err(payload) => payload,
        };
        let payload = match payload.downcast::<u64>() {
            Ok(payload) => return Ok(Self::B(payload)),
            Err(payload) => payload,
        };
        let payload = match payload.downcast::<MyMessage>() {
            Ok(payload) => return Ok(Self::C(payload)),
            Err(payload) => payload,
        };
        let payload = match payload.downcast::<u32>() {
            Ok(payload) => return Ok(Self::D(payload)),
            Err(payload) => payload,
        };
        Err(payload)
    }
    fn into_any_payload(self) -> crate::AnyPayload {
        match self {
            Self::A(payload) => crate::AnyPayload::new::<u32>(payload),
            Self::B(payload) => crate::AnyPayload::new::<u64>(payload),
            Self::C(payload) => crate::AnyPayload::new::<MyMessage>(payload),
            Self::D(payload) => crate::AnyPayload::new::<u32>(payload),
        }
    }
}
impl crate::AsSet for MyActorProtocol {
    type Set = crate::Set![u32, u64, MyMessage, u32];
}
// impl From<Payload<u32>> for MyActorProtocol {
//     fn from(payload: Payload<u32>) -> Self {
//         Self::A(payload)
//     }
// }
// impl From<Payload<u64>> for MyActorProtocol {
//     fn from(payload: Payload<u64>) -> Self {
//         Self::B(payload)
//     }
// }
// impl From<Payload<MyMessage>> for MyActorProtocol {
//     fn from(payload: Payload<MyMessage>) -> Self {
//         Self::C(payload)
//     }
// }
impl From<<FireAndForget as InvocationSpecifier<u32>>::Payload> for MyActorProtocol {
    fn from(payload: u32) -> Self {
        Self::D(payload)
    }
}

async fn test(address: Address<MyActorProtocol>) {
    let res: () = address.send(2u32).await;
    address.send(2u64).await;
    let res = address.send(MyMessage).await.await;
    // address.send("");
}

trait Trait<T> {
    type Type;
}

struct A;
struct B;

impl<T> Trait<T> for A {
    type Type = T;
}

// impl From<<A as Trait<u32>>::Type> for MyActorProtocol {
//     fn from(payload: <A as Trait<u32>>::Type) -> Self {
//         unimplemented!()
//     }
// }

// impl From<<A as Trait<u64>>::Type> for MyActorProtocol {
//     fn from(payload: <A as Trait<u64>>::Type) -> Self {
//         unimplemented!()
//     }
// }
