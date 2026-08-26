use futures::future::pending;
use rootcause::Report;
use std::time::Duration;
use zestors::{
    HandlerInterface,
    api_server::ApiServer,
    handler::{BasicScheduler, Handle, Handler, HandlerExit, HandlerState},
    node::Node,
    prelude::*,
    signals::RestartMode,
    supervision::{BlueprintExt, Supervisor, actor_fn, blueprint_fn, task_fn},
};

#[derive(Interface, HandlerInterface)]
enum MyInterface {
    Add(Envelope<u32>),
    Print(Envelope<String>),
}

#[derive(Debug)]
struct MyActor {
    name: String,
    scheduler: BasicScheduler<MyActor>,
}

impl MyActor {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            scheduler: BasicScheduler::new(),
        }
    }
}

#[derive(Message)]
struct Tick;

impl Handler for MyActor {
    type Interface = MyInterface;

    async fn init(&mut self, _state: HandlerState<'_, MyActor>) -> Result<(), Report> {
        tokio::time::sleep(Duration::from_secs(3)).await;

        self.scheduler.schedule_msg(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;

            Ok(Tick)
        });

        Ok(())
    }

    async fn exit(
        &mut self,
        _state: HandlerState<'_, Self>,
        reason: HandlerExit,
    ) -> Result<(), Report> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        reason.into()
    }

    async fn on_shutdown(&mut self, _address: &Address<Self::Interface>) -> Result<(), Report> {
        tracing::info!("Actor {} is shutting down", self.name);

        Ok(())
    }
}

impl Handle<Tick> for MyActor {
    async fn handle(
        &mut self,
        _state: HandlerState<'_, Self>,
        _msg: Envelope<Tick>,
    ) -> Result<(), Report> {
        tracing::info!("Actor {} received a tick", self.name);

        self.scheduler.schedule_msg(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(Tick)
        });

        Ok(())
    }
}

impl Handle<u32> for MyActor {
    async fn handle(
        &mut self,
        _state: HandlerState<'_, Self>,
        msg: Envelope<u32>,
    ) -> Result<(), Report> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl Handle<String> for MyActor {
    async fn handle(
        &mut self,
        _state: HandlerState<'_, Self>,
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

    let (spec_a, _addr) = blueprint_fn(|| MyActor::new("A"))
        .with_pid("HelloActor")?
        .with_mode(RestartMode::Never)
        .split();

    let (spec_b, _addr) = blueprint_fn(|| MyActor::new("B"))
        .with_pid("HelloActor2")?
        .with_mode(RestartMode::Always)
        .split();

    let (super_spec_a, _addr) = Supervisor::blueprint()
        .with_children([spec_a, spec_b])
        .with_pid("SupervisorA")?
        .split();

    let (spec_c, _addr) = blueprint_fn(|| MyActor::new("C"))
        .with_pid("HelloActor3")?
        .with_mode(RestartMode::Always)
        .split();

    let (spec_d, _addr) = blueprint_fn(|| MyActor::new("D"))
        .with_pid("HelloActor4")?
        .with_mode(RestartMode::Always)
        .split();

    let (super_spec_b, _addr) = Supervisor::blueprint()
        .with_children([spec_c, spec_d])
        .with_pid("SupervisorB")?
        .split();

    let (api_server_spec, _addr) = ApiServer::blueprint("127.0.0.1:8080".parse().unwrap())
        .with_pid("ApiServer")?
        .split();

    let (dyn_actor_spec, _addr) = actor_fn(async |_: Inbox<MyInterface>| Ok(()))
        .with_pid("DynActor")?
        .split();

    let (task_spec, _addr) = task_fn(|mut task_box| async move {
        task_box
            .run_until_shutdown(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                println!("Task completed successfully")
            })
            .await?;

        Ok(())
    })
    .with_pid("TaskActor")?
    .split();

    let root_supervisor = Supervisor::blueprint()
        .with_children([
            super_spec_a,
            super_spec_b,
            api_server_spec,
            dyn_actor_spec,
            task_spec,
            blueprint_fn(|| actor_fn(async |_: Inbox<MyInterface>| Ok(())))
                .with_pid("DynBlueprintActor")?
                .into(),
            blueprint_fn(|| MyActor::new("E"))
                .with_pid("DynBlueprintActor2")?
                .into(),
        ])
        .with_pid("RootSupervisor")?;

    let root_address = Node::new(root_supervisor).start().await?;

    root_address.watch_start().await;

    tracing::info!("All actors started, sending messages...");

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        // addr_a.signal_shutdown();
        // addr_b.signal_shutdown();
    }
}
