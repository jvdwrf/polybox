use uuid::Uuid;
use zestors::{
    ActorInterface, Interface,
    actor::{Actor, HandleMessage},
    polybox::Payload,
    registry::Pid,
    state::{ActorState, ExitReason},
    supervision::{ActorBlueprintExt, ActorRunnerExt, ChildSpec, Supervisor},
};

#[derive(Interface, ActorInterface)]
enum MyInterface {
    Add(Payload<u32>),
    Print(Payload<String>),
}

#[derive(Debug, Clone)]
struct MyActor {
    id: Uuid,
}

impl MyActor {
    fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

impl Actor for MyActor {
    type Interface = MyInterface;
    type Error = anyhow::Error;
    type Exit = ();

    async fn exit(&mut self, _reason: ExitReason) -> Result<Self::Exit, Self::Error> {
        Ok(())
    }
}

impl HandleMessage<u32> for MyActor {
    async fn handle_message(
        &mut self,
        _state: &mut ActorState<Self>,
        msg: Payload<u32>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl HandleMessage<String> for MyActor {
    async fn handle_message(
        &mut self,
        _state: &mut ActorState<Self>,
        msg: Payload<String>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let supervisor_a = Supervisor::blueprint()
        .with_child(ChildSpec::new("HelloActor", MyActor::new()))
        .with_child(ChildSpec::new("HelloActor2", MyActor::new()));

    let supervisor_b = Supervisor::blueprint()
        .with_child(ChildSpec::new_uuid(MyActor::new()))
        .with_child(ChildSpec::new_uuid(MyActor::new()));

    let supervisor = Supervisor::blueprint()
        .with_child(ChildSpec::new("SupervisorA", supervisor_a))
        .with_child(ChildSpec::new("SupervisorB", supervisor_b));

    supervisor.spawn_ref();
}
