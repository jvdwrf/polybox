use rootcause::Report;
use std::time::Duration;
use zestors::{
    HandlerInterface,
    handler::{Handle, HandledBy, Handler, HandlerState, ShutdownReason},
    prelude::*,
    signals::{GetChildren, GetDebug},
    spawn,
    supervision::ActorRunnerExt as _,
};

#[tokio::main]
async fn main() {
    let child = spawn(
        Pid::rand(),
        async move |mut stream: EventStream<MyInterface>| {
            while let Some(msg) = stream.next().await {
                match msg {
                    Event::Signal(signal) => match signal {
                        SignalEvent::Shutdown => {
                            println!("Received shutdown signal");
                            break;
                        }
                        SignalEvent::Resume | SignalEvent::Suspend => {}
                    },

                    Event::Message(message) => match message {
                        MyInterface::Add(payload) => {
                            println!("Received message: {:?}", payload);
                        }
                        MyInterface::Print(payload) => {
                            println!("Received message: {:?}", payload);
                        }
                        MyInterface::Debug((_, tx)) => {
                            tx.send("MyActor is running".into()).ok();
                        }
                    },
                }
            }

            Ok(())
        },
    );

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
    Add(Payload<u32>),
    Print(Payload<String>),
    Debug(Payload<GetDebug>),
    Children(Payload<GetChildren>),
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
        msg: Payload<u32>,
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
        msg: Payload<String>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

impl Handle<IntervalTick> for MyActor {
    async fn handle(
        &mut self,
        _: &mut HandlerState<Self>,
        _: Payload<IntervalTick>,
    ) -> Result<(), Self::Error> {
        println!("Interval tick: {}", self.nr);
        Ok(())
    }
}

impl Handle<GetDebug> for MyActor {
    async fn handle(
        &mut self,
        _: &mut HandlerState<Self>,
        (_, tx): Payload<GetDebug>,
    ) -> Result<(), Self::Error> {
        tx.send("MyActor is running".into()).ok();
        Ok(())
    }
}

impl Handle<GetChildren> for MyActor {
    async fn handle(
        &mut self,
        _: &mut HandlerState<Self>,
        (_, tx): Payload<GetChildren>,
    ) -> Result<(), Self::Error> {
        tx.send(vec![]).ok();
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
