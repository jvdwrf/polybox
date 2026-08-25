use rootcause::Report;
use std::time::Duration;
use zestors::{
    HandlerInterface,
    handler::{Handle, Handler, HandlerState},
    prelude::*,
    spawn,
    supervision::{ActorExt as _, GetChildren, GetHealth, Health},
};

#[tokio::main]
async fn main() {
    let child = spawn(Pid::rand(), async move |mut stream: Inbox<MyInterface>| {
        while let Some(msg) = stream.next().await {
            match msg {
                Event::Signal(signal) => match signal {
                    Signal::Shutdown => {
                        println!("Received shutdown signal");
                        break;
                    }
                    Signal::Resume | Signal::Suspend => {}
                },

                Event::Message(message) => match message {
                    MyInterface::Add(envelope) => {
                        println!("Received message: {:?}", envelope);
                    }
                    MyInterface::Print(envelope) => {
                        println!("Received message: {:?}", envelope);
                    }
                    MyInterface::Health(Envelope { handle, .. }) => {
                        handle.send(Health::healthy()).ok();
                    }
                    MyInterface::Children(Envelope { handle, .. }) => {
                        handle.send(vec![]).ok();
                    }
                },
            }
        }

        Ok(())
    });

    child.address().send(10u32).await.unwrap();
    child.address().signal_shutdown();
    child.watch_exit().await.unwrap();

    // test().await;
}

#[derive(Debug)]
struct MyActor {
    nr: u32,
    interval: tokio::time::Interval,
}

impl MyActor {
    fn new() -> Self {
        Self {
            nr: 0,
            interval: tokio::time::interval(Duration::from_secs(5)),
        }
    }
}

#[derive(Interface, HandlerInterface)]
enum MyInterface {
    Add(Envelope<u32>),
    Print(Envelope<String>),
    Health(Envelope<GetHealth>),
    Children(Envelope<GetChildren>),
}

#[derive(Message)]
struct IntervalTick;

impl Handler for MyActor {
    type Interface = MyInterface;
}

impl Handle<u32> for MyActor {
    async fn handle(
        &mut self,
        state: HandlerState<'_, Self>,
        Envelope { msg, handle: () }: Envelope<u32>,
    ) -> Result<(), Report> {
        println!("Received message: {:?}", msg);

        self.nr += msg;

        if msg == 301 {
            state.signal_shutdown();
        }

        Ok(())
    }
}

impl Handle<String> for MyActor {
    async fn handle(
        &mut self,
        _: HandlerState<'_, Self>,
        msg: Envelope<String>,
    ) -> Result<(), Report> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl Handle<IntervalTick> for MyActor {
    async fn handle(
        &mut self,
        _: HandlerState<'_, Self>,
        _: Envelope<IntervalTick>,
    ) -> Result<(), Report> {
        println!("Interval tick: {}", self.nr);
        Ok(())
    }
}

impl Handle<GetHealth> for MyActor {
    async fn handle(
        &mut self,
        mut state: HandlerState<'_, Self>,
        Envelope { msg: _, handle }: Envelope<GetHealth>,
    ) -> Result<(), Report> {
        handle.send(Health::healthy().with_debug_repr(&self)).ok();

        state.schedule_msg(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok("Hello".to_string())
        });

        state.schedule_fut(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        });

        Ok(())
    }
}

impl Handle<GetChildren> for MyActor {
    async fn handle(
        &mut self,
        _: HandlerState<'_, Self>,
        Envelope { msg: _, handle }: Envelope<GetChildren>,
    ) -> Result<(), Report> {
        handle.send(vec![]).ok();
        Ok(())
    }
}

async fn test() {
    let child = MyActor::new()
        // .map_actor_exit(|x| x.map(|x| x * 2))
        .spawn(Pid::rand());
    let address = child.address().clone();

    address.send(5u32).await.unwrap();
    child.send(15u32).await.unwrap();
    child.send("Hello, world!".to_string()).await.unwrap();

    child.signal_shutdown();
    child.await.unwrap();
}
