use std::time::Duration;
use zestors::{
    event_stream::EventStream,
    handler::{Handle, HandledBy, Handler},
    polybox::{Payload, Sends as _},
    registry::Pid,
    signals::{Event, SendSignal, Shutdown, Signal},
    state::HandlerState,
    supervision::ActorRunnerExt as _,
    *,
};

#[tokio::main]
async fn main() {
    let child = spawn(
        Pid::rand_uuid(),
        async move |mut stream: EventStream<MyInterface>, _address| {
            while let Some(msg) = stream.recv().await {
                match msg {
                    Event::Signal(signal) => match signal {
                        Signal::Shutdown(_) => break,
                        Signal::Exit(_) => break,
                        Signal::Suspend(_) => todo!(),
                        Signal::Resume(_) => todo!(),
                        Signal::GetStatus((_, tx)) => {
                            let _ = tx.send(signals::Status::Running);
                        }
                        Signal::GetState((_, tx)) => {
                            let _ = tx.send(signals::State {
                                status: signals::Status::Running,
                                uptime: Duration::from_secs(0),
                                description: "Test".to_string(),
                            });
                        }
                        Signal::Ping((_, tx)) => {
                            let _ = tx.send(());
                        }
                    },
                    Event::Message(message) => match message {
                        MyInterface::Add(payload) => {
                            println!("Received message: {:?}", payload);
                        }
                        MyInterface::Print(payload) => {
                            println!("Received message: {:?}", payload);
                        }
                    },
                }
            }

            Ok(())
        },
    );

    child.address().send(10u32).await.unwrap();
    child.address().signal(Shutdown).await.unwrap();
    child.await.unwrap();

    test().await;
}

#[derive(Debug)]
struct MyActor {
    nr: u32,
}

#[derive(Interface, HandlerInterface)]
enum MyInterface {
    Add(Payload<u32>),
    Print(Payload<String>),
}

impl Handler for MyActor {
    type Interface = MyInterface;
    type Error = anyhow::Error;
    type Exit = u32;

    async fn exit(&mut self, reason: state::ExitReason) -> Result<Self::Exit, Self::Error> {
        println!("Exiting with reason: {:?}", reason);
        Ok(self.nr)
    }

    async fn schedule_next(&mut self) -> Result<impl HandledBy<Self>, Self::Error> {
        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(10u32)
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
            state.signal(Shutdown).await.ok();
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

async fn test() {
    let child = MyActor { nr: 0 }
        .map(|x| x.map(|x| x * 2))
        .spawn(Pid::rand_uuid());
    let address = child.address().clone();

    address.send(5u32).await.unwrap();
    child.send(15u32).await.unwrap();
    child.send("Hello, world!".to_string()).await.unwrap();
    child.signal(Shutdown).await.unwrap();
    let exit_value = child.await.unwrap();
    assert_eq!(exit_value, 20 * 2);
}
