use polybox::{Interface, Payload, Sends as _};
use std::time::Duration;
use zestors::{
    actor::{Actor, HandleMessage},
    event_stream::EventStream,
    signals::{Event, SendSignal, Shutdown, Signal},
    state::ActorState,
    supervision::RunnableExt as _,
    *,
};

#[tokio::main]
async fn main() {
    let child = spawn(
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

#[derive(Interface, ActorInterface)]
enum MyInterface {
    Add(Payload<u32>),
    Print(Payload<String>),
}

impl Actor for MyActor {
    type Interface = MyInterface;

    type Error = anyhow::Error;

    type Exit = u32;

    async fn exit(&mut self, reason: state::ExitReason) -> Result<Self::Exit, Self::Error> {
        println!("Exiting with reason: {:?}", reason);
        Ok(self.nr)
    }
}

impl HandleMessage<u32> for MyActor {
    async fn handle_message(
        &mut self,
        state: &mut ActorState<Self>,
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

impl HandleMessage<String> for MyActor {
    async fn handle_message(
        &mut self,
        _: &mut ActorState<Self>,
        msg: Payload<String>,
    ) -> Result<(), Self::Error> {
        println!("Received message: {:?}", msg);
        Ok(())
    }
}

async fn test() {
    let child = MyActor { nr: 0 }.map(|x| x.map(|x| x * 2)).spawn();
    let address = child.address().clone();

    address.send(5u32).await.unwrap();
    child.send(15u32).await.unwrap();
    child.send("Hello, world!".to_string()).await.unwrap();
    child.signal(Shutdown).await.unwrap();
    let exit_value = child.await.unwrap();
    assert_eq!(exit_value, 20 * 2);
}
