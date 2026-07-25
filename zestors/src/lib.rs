use crate::{address::Address, signals::Signal};
use polybox::{
    DynInbox, Interface, Message, Output, PolyboxExt, Sends, errors::SendError, inboxes::TokioInbox,
};

pub fn spawn<T, F, Fut>(f: F) -> (Address<T>, tokio::task::JoinHandle<Fut::Output>)
where
    T: Interface,
    F: FnOnce(tokio::sync::mpsc::Receiver<T>, tokio::sync::mpsc::Receiver<Signal>) -> Fut,
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let (inbox, receiver) = TokioInbox::new(1_000_000);
    let (signal_inbox, signal_receiver) = TokioInbox::new(1_000);
    let handle = tokio::spawn(f(receiver, signal_receiver));
    (Address::new(inbox, signal_inbox), handle)
}

pub mod address;
pub mod handler;
pub mod signals;
pub use polybox;

#[cfg(test)]
mod tests {
    use crate::signals::{SendSignal, Shutdown};

    use super::*;
    use polybox::{Payload, Sends as _};
    use tokio::sync::mpsc::Receiver;

    #[derive(Interface)]
    enum TestInterface {
        A(Payload<u32>),
    }

    #[tokio::test]
    async fn test_spawn() {
        let (address, handle) = spawn(
            async move |mut rx: Receiver<TestInterface>, mut signal_rx| {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        TestInterface::A(payload) => {
                            println!("Received: {}", payload);
                        }
                    }
                }
            },
        );

        address.send(10u32).await.unwrap();
        address.signal(Shutdown).await.unwrap();
        handle.await.unwrap();
    }
}
