use rootcause::Report;
use std::time::Duration;
use zestors::{
    HandlerInterface,
    handler::{Handle, HandledBy, Handler, HandlerState, ShutdownReason},
    prelude::*,
    spawn,
    supervision::{ActorRunnerExt as _, GetChildren, GetHealth, Health},
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

    test().await;
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
    type Error = Report;
    type Exit = u32;

    async fn exit(
        &mut self,
        _address: &Address<Self::Interface>,
        reason: ShutdownReason,
    ) -> Result<Self::Exit, Self::Error> {
        println!("Exiting with reason: {:?}", reason);
        Ok(self.nr)
    }

    async fn schedule_next(&mut self) -> Result<impl HandledBy<Self>, Self::Error> {
        self.interval.tick().await;
        Ok(IntervalTick)
    }
}

impl Handle<u32> for MyActor {
    async fn handle(
        &mut self,
        state: &mut HandlerState<Self>,
        Envelope { msg, handle: () }: Envelope<u32>,
    ) -> Result<(), Self::Error> {
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
        _: &mut HandlerState<Self>,
        msg: Envelope<String>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl Handle<IntervalTick> for MyActor {
    async fn handle(
        &mut self,
        _: &mut HandlerState<Self>,
        _: Envelope<IntervalTick>,
    ) -> Result<(), Self::Error> {
        println!("Interval tick: {}", self.nr);
        Ok(())
    }
}

impl Handle<GetHealth> for MyActor {
    async fn handle(
        &mut self,
        _: &mut HandlerState<Self>,
        Envelope { msg: _, handle }: Envelope<GetHealth>,
    ) -> Result<(), Self::Error> {
        handle.send(Health::healthy().with_debug_repr(self)).ok();
        Ok(())
    }
}

impl Handle<GetChildren> for MyActor {
    async fn handle(
        &mut self,
        _: &mut HandlerState<Self>,
        Envelope { msg: _, handle }: Envelope<GetChildren>,
    ) -> Result<(), Self::Error> {
        handle.send(vec![]).ok();
        Ok(())
    }
}

async fn test() {
    let child = MyActor::new().map(|x| x.map(|x| x * 2)).spawn(Pid::rand());
    let address = child.address().clone();

    address.send(5u32).await.unwrap();
    child.send(15u32).await.unwrap();
    child.send("Hello, world!".to_string()).await.unwrap();
    child.signal_shutdown();
    let exit_value = child.await.unwrap();
    assert_eq!(exit_value, 20 * 2);
}
