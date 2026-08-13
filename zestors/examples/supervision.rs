use std::time::Duration;

use futures::join;
use zestors::{
    HandlerInterface, Interface,
    handler::{ExitReason, Handle, Handler, HandlerState},
    node::Node,
    polybox::Payload,
    registry::{Pid, Registry},
    signals::Observable,
    supervision::{
        BlueprintExt, ChildSpec, RestartIntensity, RestartMode, SupervisionTree, Supervisor,
    },
};

#[derive(Interface, HandlerInterface)]
enum MyInterface {
    Add(Payload<u32>),
    Print(Payload<String>),
}

#[derive(Debug, Clone)]
struct MyActor {
    id: String,
}

impl MyActor {
    fn new() -> Self {
        Self { id: "Hello".into() }
    }
}

impl Handler for MyActor {
    type Interface = MyInterface;
    type Error = anyhow::Error;
    type Exit = ();

    async fn exit(&mut self, _reason: ExitReason) -> Result<Self::Exit, Self::Error> {
        Ok(())
    }
}

impl Handle<u32> for MyActor {
    async fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        msg: Payload<u32>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl Handle<String> for MyActor {
    async fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        msg: Payload<String>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

// #[tokio::main]
// async fn main() -> Result<(), anyhow::Error> {
//     let registry = Registry::local();

//     let supervisor = Supervisor::blueprint()
//         .with_child(ChildSpec::new(
//             "SupervisorA",
//             Supervisor::blueprint()
//                 .with_child(ChildSpec::new("HelloActor", MyActor::new()))
//                 .with_child(ChildSpec::new("HelloActor2", MyActor::new())),
//         ))
//         .with_child(ChildSpec::new(
//             "SupervisorB",
//             Supervisor::blueprint()
//                 .with_child(ChildSpec::new(Pid::rand(), MyActor::new()))
//                 .with_child(ChildSpec::new(Pid::rand(), MyActor::new())),
//         ));

//     supervisor.spawn(Pid::rand());
//     supervisor.spawn(Pid::rand());

//     let actor_a = registry.get_typed::<MyInterface>(&Pid::from("HelloActor"))?;
//     let actor_b = registry.get_typed::<MyInterface>(&Pid::from("HelloActor2"))?;

//     Ok(())
// }

#[tokio::main]
async fn main() {
    let mut supervisor_a = Supervisor::blueprint();

    let mut actor_a = supervisor_a
        .add_child(ChildSpec::new("HelloActor", MyActor::new()).mode(RestartMode::Never));
    let mut actor_b = supervisor_a.add_child(ChildSpec::new("HelloActor2", MyActor::new()));

    let mut supervisor_b = Supervisor::blueprint();

    let mut actor_c = supervisor_b.add_child(ChildSpec::new(Pid::rand(), MyActor::new()));
    let mut actor_d = supervisor_b.add_child(ChildSpec::new(Pid::rand(), MyActor::new()));

    let root_supervisor = Node::new(
        "RootSupervisor",
        Supervisor::blueprint()
            .with_child(ChildSpec::new("SupervisorA", supervisor_a))
            .with_child(ChildSpec::new("SupervisorB", supervisor_b)),
    )
    .with_shutdown_timeout(Duration::from_secs(60))
    .with_restart_intensity(RestartIntensity::new(3, Duration::from_secs(60)))
    .start()
    .unwrap();

    join!(
        actor_a.watch_start(),
        actor_b.watch_start(),
        actor_c.watch_start(),
        actor_d.watch_start()
    );

    let tree = SupervisionTree::new_populated(root_supervisor.pid().clone())
        .await
        .unwrap();

    println!("Supervision tree: {:#?}", tree);
}
