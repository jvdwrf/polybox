use crate::*;

#[derive(Message)]
#[msg(request = String)]
#[polybox(crate = "crate")]
struct MyMessage;

#[derive(Interface)]
#[polybox(crate = "crate")]
enum MyActorProtocol {
    A(Payload<u32>),
    B(Payload<u64>),
    C(Payload<MyMessage>),
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
// impl From<<FireAndForget as SimpleSpecifier<u32>>::Payload> for MyActorProtocol {
//     fn from(payload: u32) -> Self {
//         Self::D(payload)
//     }
// }

// async fn test(address: Address<MyActorProtocol>) {
//     let res: () = address.send(2u32).await;
//     address.send(2u64).await;
//     let res = address.send(MyMessage).await.await;
//     // address.send("");
// }
