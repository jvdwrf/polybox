use std::any::TypeId;

use polybox::{
    AsSet, Interface, Message, Payload, PolyboxExt as _, Sends, SendsExt as _, Set, TokioInbox,
};

#[derive(Message, Debug)]
#[msg(request = i32)]
pub struct MyMessage;

#[derive(Interface, Debug)]
pub enum MyInterface {
    A(Payload<u32>),
    B(Payload<u64>),
    C(Payload<MyMessage>),
}

#[tokio::test]
async fn creating_address() {
    let (inbox, mut receiver) = TokioInbox::<MyInterface>::new(1000);

    let handle = tokio::task::spawn(async move {
        while let Some(msg) = receiver.recv().await {
            match msg {
                MyInterface::A(payload) => {
                    println!("Received A: {}", payload);
                }
                MyInterface::B(payload) => {
                    println!("Received B: {}", payload);
                }
                MyInterface::C((payload, tx)) => {
                    println!("Received C: {:?}", payload);
                    tx.send(42).unwrap();
                }
            }
        }
    });

    inbox.send(50u32).await.unwrap();
    inbox.send(50u64).await.unwrap();
    let _: i32 = inbox.send(MyMessage).await.unwrap().await.unwrap();
    let _: i32 = inbox.request(MyMessage).await.unwrap();
    inbox.send(MyInterface::A(10)).await.unwrap();

    inbox.send_checked("hello").await.unwrap_err();
    inbox.send_checked(30u32).await.unwrap();

    let address = inbox.clone().into_dyn();
    let address = address.into_dyn_subset::<<MyInterface as AsSet>::Set>();
    let address = address.into_dyn_subset::<Set![u64, u32]>();
    // let address = address.into_any::<Set![String]>();

    address.send(50u64).await.unwrap();
    address.send(50u32).await.unwrap();

    address.send_checked("hello").await.unwrap_err();
    address.send_checked(30u32).await.unwrap();

    assert!(address.accepts_msg(TypeId::of::<u64>()));
    accepting(address.clone()).await;
    // address.send("hello").await.unwrap();

    drop(address);
    drop(inbox);

    handle.await.unwrap();
}

async fn accepting(a: impl Sends<u32>) {
    a.send(50u32).await.unwrap();
    // a.into_subset::<Set![u32]>();
}
