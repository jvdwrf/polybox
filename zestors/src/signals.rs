use crate::*;
use polybox::{Payload, errors::SendError, oneshot::new_request};
use tokio::select;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Shutdown;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Exit;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Suspend;

#[derive(Message, Debug)]
#[msg(path = "crate")]
pub struct Resume;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ActorStatus)]
pub struct GetStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
pub enum ActorStatus {
    Running,
    Suspended,
    Exiting,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = DebugState)]
pub struct GetState;

#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DebugState {
    pub status: ActorStatus,
    #[schema(value_type = String, example = "2024-06-01T12:00:00Z")]
    pub uptime: std::time::Duration,
    pub description: String,
}

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = "Vec<Pid>")]
pub struct GetChildren;

#[derive(Message, Debug)]
#[msg(path = "crate")]
#[msg(reply = ())]
pub struct Ping;

#[derive(Interface, Debug)]
#[interface(crate = "crate")]
pub enum Signal {
    Shutdown(Payload<Shutdown>),
    Exit(Payload<Exit>),
    Suspend(Payload<Suspend>),
    Resume(Payload<Resume>),
    GetStatus(Payload<GetStatus>),
    GetState(Payload<GetState>),
    Ping(Payload<Ping>),
    GetChildren(Payload<GetChildren>),
}

impl Signal {
    pub fn kind(&self) -> SignalKind {
        match self {
            Signal::Shutdown(_) => SignalKind::Shutdown,
            Signal::Exit(_) => SignalKind::Exit,
            Signal::Suspend(_) => SignalKind::Suspend,
            Signal::Resume(_) => SignalKind::Resume,
            Signal::GetStatus(_) => SignalKind::GetStatus,
            Signal::GetState(_) => SignalKind::GetState,
            Signal::Ping(_) => SignalKind::Ping,
            Signal::GetChildren(_) => SignalKind::GetChildren,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalKind {
    Shutdown,
    Exit,
    Suspend,
    Resume,
    GetStatus,
    GetState,
    Ping,
    GetChildren,
}

pub trait Observable {
    fn send_signal(
        &self,
        signal: Signal,
    ) -> impl Future<Output = Result<(), SendError<Signal>>> + Send;

    fn signal_shutdown(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, Signal::Shutdown(Shutdown));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_exit(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, Signal::Exit(Exit));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_suspend(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, Signal::Suspend(Suspend));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn signal_resume(&self) -> impl Future<Output = Result<(), SendError<()>>> + Send {
        let fut = Self::send_signal(&self, Signal::Resume(Resume));
        async { fut.await.map_err(|_| SendError(())) }
    }

    fn get_status(
        &self,
    ) -> impl Future<Output = Result<<GetStatus as Message>::Reply, RequestError<Signal>>> + Send
    {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, Signal::GetStatus((GetStatus, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }

    fn get_debug_state(
        &self,
    ) -> impl Future<Output = Result<<GetState as Message>::Reply, RequestError<Signal>>> + Send
    {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, Signal::GetState((GetState, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }

    fn ping(
        &self,
    ) -> impl Future<Output = Result<<Ping as Message>::Reply, RequestError<Signal>>> + Send {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, Signal::Ping((Ping, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }

    fn get_children(
        &self,
    ) -> impl Future<Output = Result<<GetChildren as Message>::Reply, RequestError<Signal>>> + Send
    {
        let (tx, rx) = new_request();
        let fut = Self::send_signal(&self, Signal::GetChildren((GetChildren, tx)));
        async {
            fut.await?;
            rx.await.map_err(Into::into)
        }
    }
}

#[derive(Clone, Debug)]
pub struct SignalSender {
    sender: async_channel::Sender<Signal>,
}

impl SignalSender {
    pub(crate) fn new() -> (Self, SignalReceiver) {
        let (sender, receiver) = async_channel::bounded(1_000);
        (Self { sender }, SignalReceiver { receiver })
    }
}

impl SignalSender {
    pub async fn send(&self, signal: Signal) -> Result<(), SendError<Signal>> {
        self.sender.send(signal).await.map_err(|e| SendError(e.0))
    }
}

impl Observable for SignalSender {
    async fn send_signal(&self, signal: Signal) -> Result<(), SendError<Signal>> {
        self.sender.send(signal).await.map_err(|e| SendError(e.0))
    }
}

#[derive(Debug, Clone)]
pub struct SignalReceiver {
    receiver: async_channel::Receiver<Signal>,
}

impl SignalReceiver {
    pub async fn recv(&mut self) -> Option<Signal> {
        self.receiver.recv().await.ok()
    }

    pub async fn recv_with<T>(&mut self, other: &mut Receiver<T>) -> Option<Event<T>> {
        select! {
            biased;

            Ok(signal) = self.receiver.recv() => {
                Some(Event::Signal(signal))
            }

            Some(msg) = other.recv() => {
                Some(Event::Message(msg))
            }

            else => None,
        }
    }

    pub async fn recv_with_enabled<T>(
        &mut self,
        other: &mut Receiver<T>,
        enabled: bool,
    ) -> Option<Event<T>> {
        if enabled {
            select! {
                biased;

                Ok(signal) = self.receiver.recv() => {
                    Some(Event::Signal(signal))
                }

                Some(msg) = other.recv() => {
                    Some(Event::Message(msg))
                }

                else => None,
            }
        } else {
            if let Ok(signal) = self.receiver.recv().await {
                Some(Event::Signal(signal))
            } else {
                None
            }
        }
    }
}

#[derive(Debug)]
pub enum Event<T> {
    Signal(Signal),
    Message(T),
}
