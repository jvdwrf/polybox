use crate::{
    address::Address,
    inbox::{Inbox, Receiver},
    signals::{Signal, SignalReceiver, SignalSender},
};

pub fn spawn<T, F, Fut>(f: F) -> (Address<T>, tokio::task::JoinHandle<Fut::Output>)
where
    T: Interface,
    F: FnOnce(Receiver<T>, SignalReceiver, Address<T>) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let (inbox, receiver) = Inbox::new(1_000_000);
    let (signal_inbox, signal_receiver) = SignalSender::new();
    let address = Address::new(inbox, signal_inbox);
    let handle = tokio::spawn(f(receiver, signal_receiver, address.clone()));
    (address, handle)
}

pub mod actor;
pub mod address;
pub mod inbox;
pub mod signals;
pub mod state;
pub use polybox;
pub(crate) use polybox::*;

pub use polybox_codegen::{
    ActorInterface, InterfaceZestors as Interface, MessageZestors as Message,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        actor::{Actor, HandleMessage},
        signals::{SendSignal, Shutdown, SignalOrMessage},
        state::ActorState,
    };
    use polybox::Payload;
    use std::time::Duration;

    #[derive(Interface, ActorInterface)]
    #[interface(crate = "crate")]
    enum TestInterface {
        Add(Payload<u32>),
    }

    #[tokio::test]
    async fn test_spawn() {
        let (address, handle) = spawn(
            async move |mut rx: Receiver<TestInterface>, mut signal_rx, _address| {
                while let Some(msg) = signal_rx.recv_with(&mut rx).await {
                    match msg {
                        SignalOrMessage::Signal(signal) => match signal {
                            Signal::Shutdown(_) => break,
                            Signal::Kill(_) => break,
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
                        SignalOrMessage::Message(message) => match message {
                            TestInterface::Add(payload) => {
                                println!("Received message: {:?}", payload);
                            }
                        },
                    }
                }
            },
        );

        address.send(10u32).await.unwrap();
        address.signal(Shutdown).await.unwrap();
        handle.await.unwrap();
    }

    #[derive(Debug)]
    struct MyActor {
        nr: u32,
    }

    impl Actor for MyActor {
        type Interface = TestInterface;
        type Error = Box<dyn std::error::Error + Send + Sync>;
        type Exit = u32;

        async fn exit(&mut self, reason: state::ExitReason) -> Result<Self::Exit, Self::Error> {
            println!("Exiting with reason: {:?}", reason);
            Ok(self.nr)
        }
    }

    impl HandleMessage<u32> for MyActor {
        async fn handle_message(
            &mut self,
            _state: &mut ActorState<Self>,
            msg: u32,
        ) -> Result<(), Self::Error> {
            println!("Handling message: {}", msg);
            self.nr += msg;
            Ok(())
        }
    }
}
