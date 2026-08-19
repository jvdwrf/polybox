use futures::{future::join_all, join};
use rootcause::{
    Report,
    prelude::{IteratorExt as _, ResultExt},
    report,
};
use std::{convert::Infallible, time::Duration};
use zestors::{
    HandlerInterface,
    handler::{Handle, HandledBy, Handler, HandlerState, ShutdownReason},
    node::{ApiServer, ApiServerBlueprint, Node},
    prelude::*,
    signals::RestartMode,
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

    async fn init(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn exit(
        &mut self,
        _address: &Address<Self::Interface>,
        _reason: ShutdownReason,
    ) -> Result<Self::Exit, Self::Error> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn on_shutdown(
        &mut self,
        _address: &Address<Self::Interface>,
    ) -> Result<(), Self::Error> {
        tracing::info!("Actor {} is shutting down", self.name);

        Ok(())
    }

    // async fn schedule_next(&mut self) -> Result<impl HandledBy<Self>, Self::Error> {
    //     tokio::time::sleep(Duration::from_secs(5)).await;
    //     tracing::error!("Actor {} is idle for too long, shutting down", self.name);
    //     Err::<Infallible, _>(report!("Idle timeout reached, shutting down"))
    // }
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
        .add_child(ChildSpec::new("HelloActor", MyActor::new("A")).with_mode(RestartMode::Never));
    let actor_b = supervisor_a
        .add_child(ChildSpec::new("HelloActor2", MyActor::new("B")).with_mode(RestartMode::Always));

    let mut supervisor_b = Supervisor::blueprint();

    let actor_c = supervisor_b.add_child(ChildSpec::new(Pid::rand(), MyActor::new("C")));
    let actor_d = supervisor_b.add_child(ChildSpec::new(Pid::rand(), MyActor::new("D")));

    let _root = Node::new(ChildSpec::new(
        "root-supervisor",
        Supervisor::blueprint()
            .with_child(ChildSpec::new("supervisor-A", supervisor_a))
            .with_child(ChildSpec::new("supervisor-B", supervisor_b))
            .with_child(ChildSpec::new(
                "api-server",
                ApiServer::blueprint("127.0.0.1:8080".parse().unwrap()),
            )),
    ))
    .start()?;

    join_all([
        actor_a.watch_start(),
        actor_b.watch_start(),
        actor_c.watch_start(),
        actor_d.watch_start(),
    ])
    .await;

    tracing::info!("All actors started, sending messages...");

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        actor_a.signal_shutdown();
        actor_b.signal_shutdown();
    }

    futures::future::pending::<()>().await;
    Ok(())
}
