use futures::join;
use uuid::Uuid;
use zestors::{
    ActorInterface, Interface,
    actor::{Actor, HandleMessage},
    polybox::Payload,
    registry::{Pid, Registry},
    state::{ActorState, ExitReason},
    supervision::{ActorBlueprintExt, ChildSpec, Supervisor},
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
async fn main() -> Result<(), anyhow::Error> {
    let registry = Registry::local();

    let supervisor = Supervisor::blueprint()
        .with_child(ChildSpec::new(
            "SupervisorA",
            Supervisor::blueprint()
                .with_child(ChildSpec::new("HelloActor", MyActor::new()))
                .with_child(ChildSpec::new("HelloActor2", MyActor::new())),
        ))
        .with_child(ChildSpec::new(
            "SupervisorB",
            Supervisor::blueprint()
                .with_child(ChildSpec::new(Pid::rand_uuid(), MyActor::new()))
                .with_child(ChildSpec::new(Pid::rand_uuid(), MyActor::new())),
        ));

    supervisor.spawn(Pid::rand_uuid());
    supervisor.spawn(Pid::rand_uuid());

    let actor_a = registry.get_typed::<MyInterface>(&Pid::from("HelloActor"))?;
    let actor_b = registry.get_typed::<MyInterface>(&Pid::from("HelloActor2"))?;

    Ok(())
}

async fn test2() {
    let mut supervisor_a = Supervisor::blueprint();

    let mut actor_a = supervisor_a.add_child(ChildSpec::new("HelloActor", MyActor::new()));
    let mut actor_b = supervisor_a.add_child(ChildSpec::new("HelloActor2", MyActor::new()));

    let mut supervisor_b = Supervisor::blueprint();

    let mut actor_c = supervisor_b.add_child(ChildSpec::new(Pid::rand_uuid(), MyActor::new()));
    let mut actor_d = supervisor_b.add_child(ChildSpec::new(Pid::rand_uuid(), MyActor::new()));

    let root_supervisor = Supervisor::blueprint()
        .with_child(ChildSpec::new("SupervisorA", supervisor_a))
        .with_child(ChildSpec::new("SupervisorB", supervisor_b))
        .spawn(Pid::from("RootSupervisor"));

    join!(
        actor_a.watch_start(),
        actor_b.watch_start(),
        actor_c.watch_start(),
        actor_d.watch_start()
    );
}
