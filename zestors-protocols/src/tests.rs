use zestors_codegen::Interface;

use super::*;

#[derive(Interface)]
#[zestors(crate = "crate")]
enum MyActorProtocol {
    A(Payload<u32>),
    B(Payload<String>),
}

struct TestMessage;

impl Invocation for TestMessage {
    type Kind = Request<String>;
}

fn test(x: Payload<String>, (msg, tx): Payload<TestMessage>) {
    let x: String = x;
}
