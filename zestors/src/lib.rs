use crate::{
    address::Address,
    child::Child,
    inbox::{Inbox, Receiver},
    signals::{Signal, SignalReceiver, SignalSender},
};

pub fn spawn<T, R, F>(
    f: impl FnOnce(Receiver<T>, SignalReceiver, Address<T>) -> F,
) -> (Address<T>, Child<R>)
where
    T: Interface,
    R: Send + 'static,
    F: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    F::Output: Send + 'static,
{
    let (inbox, receiver) = Inbox::new(1_000_000);
    let (signal_inbox, signal_receiver) = SignalSender::new();
    let address = Address::new(inbox, signal_inbox);
    let handle = tokio::spawn(f(receiver, signal_receiver, address.clone()));
    let child = Child::new(handle);
    (address, child)
}

pub mod actor;
pub mod address;
pub mod child;
pub mod inbox;
pub mod signals;
pub mod state;
pub use polybox;
pub mod supervisor;
pub(crate) use polybox::*;

pub(crate) mod _prelude {
    #![allow(unused_imports)]
    pub(crate) use crate::{actor::*, address::*, child::*, inbox::*, signals::*, state::*, *};
}

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
                Ok(())
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
            _state: &mut ActorState<Self>,
            msg: u32,
        ) -> Result<(), Self::Error> {
            println!("Handling message: {}", msg);
            self.nr += msg;
            Ok(())
        }
    }
}
