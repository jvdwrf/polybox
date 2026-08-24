use rootcause::Report;
use std::time::Duration;
use zestors::{
    HandlerInterface,
    api_server::ApiServer,
    handler::{Handle, Handler, HandlerShutdownReason, HandlerState},
    node::Node,
    prelude::*,
    signals::RestartMode,
    supervision::{BlueprintExt, Supervisor, new_actor, new_blueprint},
};

#[derive(Interface, HandlerInterface)]
enum MyInterface {
    Add(Envelope<u32>),
    Print(Envelope<String>),
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

    async fn init(&mut self, _address: &Address<Self::Interface>) -> Result<(), Report> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn exit(
        &mut self,
        result: Result<(), Report>,
        _address: &Address<Self::Interface>,
    ) -> Result<(), Report> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    async fn on_shutdown(&mut self, _address: &Address<Self::Interface>) -> Result<(), Report> {
        tracing::info!("Actor {} is shutting down", self.name);

        Ok(())
    }

    // async fn schedule_next(&mut self) -> Result<impl HandledBy<Self>, Report> {
    //     tokio::time::sleep(Duration::from_secs(5)).await;
    //     tracing::error!("Actor {} is idle for too long, shutting down", self.name);
    //     Err::<Infallible, _>(report!("Idle timeout reached, shutting down"))
    // }
}

impl Handle<u32> for MyActor {
    async fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        msg: Envelope<u32>,
    ) -> Result<(), Report> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl Handle<String> for MyActor {
    async fn handle(
        &mut self,
        _state: &mut HandlerState<Self>,
        msg: Envelope<String>,
    ) -> Result<(), Report> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let (spec_a, addr_a) = MyActor::new("A")
        .with_pid("HelloActor")
        .with_mode(RestartMode::Never)
        .split();

    let (spec_b, addr_b) = MyActor::new("B")
        .with_pid("HelloActor2")
        .with_mode(RestartMode::Always)
        .split();

    let (super_spec_a, _) = Supervisor::blueprint()
        .with_children([spec_a, spec_b])
        .with_pid("SupervisorA")
        .split();

    let (spec_c, _) = MyActor::new("C")
        .with_pid("HelloActor3")
        .with_mode(RestartMode::Always)
        .split();

    let (spec_d, _) = MyActor::new("D")
        .with_pid("HelloActor4")
        .with_mode(RestartMode::Always)
        .split();

    let (super_spec_b, _) = Supervisor::blueprint()
        .with_children([spec_c, spec_d])
        .with_pid("SupervisorB")
        .split();

    let (api_server_spec, _) = ApiServer::blueprint("127.0.0.1:8080".parse().unwrap())
        .with_pid("ApiServer")
        .split();

    let (dyn_actor_spec, _) = new_actor(async |_: Inbox<MyInterface>| Ok(()))
        .with_pid("DynActor")
        .split();

    let root_supervisor = Supervisor::blueprint()
        .with_children([
            super_spec_a,
            super_spec_b,
            api_server_spec,
            dyn_actor_spec,
            new_blueprint(|| new_actor(async |_: Inbox<MyInterface>| Ok(())))
                .with_pid("DynBlueprintActor")
                .into(),
            new_blueprint(|| MyActor::new("E"))
                .with_pid("DynBlueprintActor2")
                .into(),
        ])
        .with_pid("RootSupervisor");

    let root_address = Node::new(root_supervisor).start().await?;

    root_address.watch_start().await;

    tracing::info!("All actors started, sending messages...");

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        // addr_a.signal_shutdown();
        // addr_b.signal_shutdown();
    }
}
