use futures::join;
use rootcause::Report;
use std::time::Duration;
use zestors::{
    ActorRef as _, HandlerInterface, Interface, Payload, Pid, RestartMode,
    handler::{ExitReason, Handle, Handler, HandlerState},
    node::{ApiConfig, Node},
    supervision::{ChildSpec, RestartIntensity, Supervisor},
};

#[derive(Interface, HandlerInterface)]
enum MyInterface {
    Add(Payload<u32>),
    Print(Payload<String>),
}

#[derive(Debug, Clone)]
struct MyActor {
    name: String,
}

impl MyActor {
    fn new(name: &str) -> Self {
        Self { name: name.into() }
    }
}

impl Handler for MyActor {
    type Interface = MyInterface;
    type Error = Report;
    type Exit = ();

    async fn exit(&mut self, _reason: ExitReason) -> Result<Self::Exit, Self::Error> {
        Ok(())
    }

    async fn on_shutdown(&mut self) -> Result<(), Self::Error> {
        tracing::info!("Actor {} is shutting down", self.name);

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

#[tokio::main]
async fn main() -> Result<(), Report> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let mut supervisor_a = Supervisor::blueprint();

    let actor_a = supervisor_a
        .add_child(ChildSpec::new("HelloActor", MyActor::new("A")).mode(RestartMode::Never));
    let actor_b = supervisor_a.add_child(ChildSpec::new("HelloActor2", MyActor::new("B")));

    let mut supervisor_b = Supervisor::blueprint();

    let actor_c = supervisor_b.add_child(ChildSpec::new(Pid::rand(), MyActor::new("C")));
    let actor_d = supervisor_b.add_child(ChildSpec::new(Pid::rand(), MyActor::new("D")));

    let _root = Node::new(ChildSpec::new(
        "RootSupervisor",
        Supervisor::blueprint()
            .with_child(ChildSpec::new("SupervisorA", supervisor_a))
            .with_child(ChildSpec::new("SupervisorB", supervisor_b)),
    ))
    .with_restart_intensity(RestartIntensity::new(3, Duration::from_secs(60)))
    .with_api(ApiConfig {
        addr: "127.0.0.1:8080".parse()?,
        swagger_ui: true,
        ..Default::default()
    })
    .start()?;

    join!(
        actor_a.watch_start(),
        actor_b.watch_start(),
        actor_c.watch_start(),
        actor_d.watch_start()
    );

    tracing::info!("All actors started, sending messages...");

    futures::future::pending::<()>().await;
    Ok(())
}
